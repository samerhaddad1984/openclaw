from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from graph_auth import GraphAuth
from graph_sharepoint import GraphSharePoint
from graph_list import GraphList
from client_registry import ClientRegistry
from rules_engine import RulesEngine
from pdf_extract import extract_pdf_text
from openrouter_client import OpenRouterClient
from client_router import ClientRouter
from ai_client_router import AIClientRouter
from draft_csv_writer import append_draft_row
from fingerprint_utils import sha256_bytes
from fingerprint_registry import has_fingerprint, add_fingerprint
from vendor_intelligence import VendorIntelligenceEngine
from review_policy import decide_review_status
from amount_policy import choose_bookkeeping_amount

CLIENT_ID = "11da5dd7-6b6f-4367-9815-562805ae9b40"

SCOPES = [
    "User.Read",
    "Sites.ReadWrite.All",
]

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

CONFIG_FILE = DATA_DIR / "config.json"
TOKENS_FILE = DATA_DIR / "tokens.json"

RULES_DIR = DATA_DIR / "rules"
STATE_DIR = DATA_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_STATE_FILE = STATE_DIR / "processed_sharepoint_items.json"

QUEUE_LIST_NAME = "LedgerLink Queue"
CLIENTS_LIST_NAME = "LedgerLink Clients"


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _load_processed_ids() -> set[str]:
    if not PROCESSED_STATE_FILE.exists():
        return set()
    try:
        obj = json.loads(PROCESSED_STATE_FILE.read_text(encoding="utf-8"))
        return set(obj.get("processed_ids", []))
    except Exception:
        return set()


def _save_processed_ids(ids: set[str]) -> None:
    PROCESSED_STATE_FILE.write_text(
        json.dumps({"processed_ids": sorted(list(ids))}, indent=2),
        encoding="utf-8",
    )


def process_sharepoint_once(max_files: int = 10) -> dict:
    if not CONFIG_FILE.exists():
        raise RuntimeError(f"config.json not found at: {CONFIG_FILE}")

    cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    ai_enabled = bool(cfg.get("ai_enabled", True))

    site_url = cfg["sharepoint_site_url"]
    inbox_folder = cfg.get("folders_inbox", "/AI/Inbox")
    out_folder = cfg.get("folders_out", "/AI/For_Review")
    done_folder = cfg.get("folders_done", "/AI/Completed")
    err_folder = cfg.get("folders_err", "/AI/Exceptions")

    auth = GraphAuth(client_id=CLIENT_ID, scopes=SCOPES)
    token = auth.acquire_token(TOKENS_FILE)

    sp = GraphSharePoint(token)
    gl = GraphList(token)

    site_id = sp.resolve_site_id(site_url)
    drive_id = sp.get_default_drive_id(site_id)

    sp.ensure_folder_path(drive_id, inbox_folder)
    sp.ensure_folder_path(drive_id, out_folder)
    sp.ensure_folder_path(drive_id, done_folder)
    sp.ensure_folder_path(drive_id, err_folder)

    queue_ref = gl.get_list_by_name(site_id, QUEUE_LIST_NAME)

    client_registry = ClientRegistry(
        graph_list=gl,
        site_id=site_id,
        list_name=CLIENTS_LIST_NAME,
    )
    client_registry.load()

    processed_ids = _load_processed_ids()
    items = sp.list_folder_children(drive_id, inbox_folder, top=200)

    rules = RulesEngine(RULES_DIR)
    router = ClientRouter(RULES_DIR)
    vendor_intel = VendorIntelligenceEngine(RULES_DIR)
    ai_route = AIClientRouter(RULES_DIR) if ai_enabled else None
    ai = OpenRouterClient() if ai_enabled else None

    handled = 0
    skipped_already_done = 0
    skipped_duplicate_fingerprint = 0
    skipped_duplicate_list_row = 0
    moved_to_completed_as_duplicate = 0
    moved_to_exceptions = 0
    pdf_text_extracted = 0
    ai_calls = 0
    ai_route_calls = 0
    list_rows_created = 0
    draft_rows_written = 0
    errors: list[str] = []

    dl_dir = DATA_DIR / "downloads"
    dl_dir.mkdir(parents=True, exist_ok=True)

    for it in items:
        if handled >= max_files:
            break

        if it.get("folder") is not None:
            continue

        item_id = it.get("id")
        name = it.get("name") or ""
        if not item_id:
            continue

        if item_id in processed_ids:
            skipped_already_done += 1
            continue

        if not name.lower().endswith(".pdf"):
            try:
                sp.move_item(drive_id, item_id, err_folder, new_name=name)
                processed_ids.add(item_id)
                moved_to_exceptions += 1
                handled += 1
            except Exception as e:
                errors.append(f"{name}: move non-pdf to Exceptions failed: {e}")
            continue

        try:
            content = sp.download_item_bytes(drive_id, item_id)
            fingerprint = sha256_bytes(content)

            if has_fingerprint(fingerprint):
                try:
                    sp.move_item(drive_id, item_id, done_folder, new_name=name)
                    moved_to_completed_as_duplicate += 1
                except Exception as e:
                    errors.append(f"{name}: duplicate fingerprint found, but move to Completed failed: {e}")

                processed_ids.add(item_id)
                skipped_duplicate_fingerprint += 1
                handled += 1
                continue

            existing_fp_row = gl.find_item_by_field_value(
                site_id=site_id,
                list_id=queue_ref.list_id,
                field_name="Fingerprint",
                field_value=fingerprint,
                top=1000,
            )
            if existing_fp_row:
                try:
                    sp.move_item(drive_id, item_id, done_folder, new_name=name)
                    moved_to_completed_as_duplicate += 1
                except Exception as e:
                    errors.append(f"{name}: list fingerprint duplicate found, but move to Completed failed: {e}")

                add_fingerprint(fingerprint)
                processed_ids.add(item_id)
                skipped_duplicate_fingerprint += 1
                handled += 1
                continue

            existing_id_row = gl.find_item_by_field_value(
                site_id=site_id,
                list_id=queue_ref.list_id,
                field_name="SharePointItemId",
                field_value=item_id,
                top=1000,
            )
            if existing_id_row:
                try:
                    sp.move_item(drive_id, item_id, done_folder, new_name=name)
                    moved_to_completed_as_duplicate += 1
                except Exception as e:
                    errors.append(f"{name}: duplicate item id found, but move to Completed failed: {e}")

                processed_ids.add(item_id)
                skipped_duplicate_list_row += 1
                handled += 1
                continue

            tmp_path = dl_dir / name
            tmp_path.write_bytes(content)

            text = (extract_pdf_text(tmp_path) or "").strip()
            if not text:
                raise RuntimeError("PDF text extraction returned empty (likely scanned PDF and OCR failed)")

            pdf_text_extracted += 1

            rr = rules.run(text)

            result_obj = {
                "run_at": now_iso(),
                "source": "sharepoint",
                "needs_review": False,
                "sharepoint": {
                    "site_url": site_url,
                    "drive_id": drive_id,
                    "inbox_folder": inbox_folder,
                    "item_id": item_id,
                    "file_name": name,
                },
                "fingerprint": fingerprint,
                "client_route": None,
                "vendor_intelligence": None,
                "amount_policy": None,
                "review_decision": None,
                "rules": {
                    "doc_type": rr.doc_type,
                    "confidence": rr.confidence,
                    "vendor_name": rr.vendor_name,
                    "total": rr.total,
                    "document_date": rr.document_date,
                    "currency": rr.currency,
                    "notes": rr.notes,
                },
                "ai": None,
                "final": None,
            }

            if rr.confidence >= 0.85:
                result_obj["final"] = {"method": "rules", **result_obj["rules"]}
            else:
                if not ai_enabled:
                    result_obj["final"] = {"method": "rules_only_ai_disabled", **result_obj["rules"]}
                    result_obj["needs_review"] = True
                else:
                    ai_calls += 1
                    prompt = (
                        "Extract structured invoice/receipt fields from the document text.\n"
                        "Return STRICT JSON only.\n\n"
                        "Required keys: doc_type, vendor_name, document_date, invoice_number, currency, subtotal, tax_total, total, notes.\n\n"
                        f"DOCUMENT TEXT:\n{text[:20000]}"
                    )
                    ai_json = ai.chat_json(
                        system="You are a precise accounting document extractor for Canada. Return strict JSON only. Do not invent amounts.",
                        user=prompt,
                        temperature=0.0,
                    )
                    result_obj["ai"] = ai_json
                    result_obj["final"] = {"method": "rules+ai", **ai_json}

            final = result_obj["final"] or {}

            route = router.route(text)
            ai_route_used = False
            ai_route_reason = ""

            if ai_enabled and ai_route and not route.client_code:
                ai_route_calls += 1
                ar = ai_route.route(text)
                if ar.client_code and ar.confidence >= 0.70:
                    ai_route_used = True
                    ai_route_reason = ar.reason
                    route.client_code = ar.client_code
                    route.assigned_to = ar.assigned_to
                    route.client_name = ar.client_code
                    route.score = max(route.score, 7)
                    route.matched_signals.append(f"ai_route:{ar.reason}")

            # CENTRAL SOURCE OF TRUTH: LedgerLink Clients
            registry_entry = client_registry.get(route.client_code)
            if registry_entry:
                route.client_name = registry_entry.client_name
                route.assigned_to = registry_entry.assigned_to
                resolved_team = registry_entry.team
                resolved_storage_path = registry_entry.storage_path
                client_registry_status = registry_entry.status
            else:
                resolved_team = ""
                resolved_storage_path = ""
                client_registry_status = ""

            result_obj["client_route"] = {
                "client_code": route.client_code,
                "client_name": route.client_name,
                "assigned_to": route.assigned_to,
                "team": resolved_team,
                "storage_path": resolved_storage_path,
                "client_registry_status": client_registry_status,
                "score": route.score,
                "matched_signals": route.matched_signals,
                "min_score_required": route.min_score_required,
                "ai_route_used": ai_route_used,
                "ai_route_reason": ai_route_reason,
            }

            intel = vendor_intel.classify(
                vendor_name=final.get("vendor_name"),
                doc_type=final.get("doc_type"),
            )
            result_obj["vendor_intelligence"] = {
                "category": intel.category,
                "gl_account": intel.gl_account,
                "tax_code": intel.tax_code,
                "source": intel.source,
                "document_family": intel.document_family,
            }

            amount_policy = choose_bookkeeping_amount(
                vendor_name=final.get("vendor_name"),
                doc_type=final.get("doc_type"),
                total=final.get("total"),
                notes=final.get("notes"),
            )
            result_obj["amount_policy"] = {
                "bookkeeping_amount": amount_policy.bookkeeping_amount,
                "amount_source": amount_policy.amount_source,
                "reason": amount_policy.reason,
            }

            review = decide_review_status(
                rules_confidence=float(rr.confidence or 0.0),
                final_method=str(final.get("method") or "").strip(),
                vendor_name=final.get("vendor_name"),
                total=amount_policy.bookkeeping_amount,
                document_date=final.get("document_date"),
                client_code=route.client_code,
            )

            result_obj["review_decision"] = {
                "status": review.status,
                "reason": review.reason,
                "effective_confidence": review.effective_confidence,
            }

            status = review.status
            result_obj["needs_review"] = status != "Ready"

            json_name = f"{name}__{item_id[:8]}.json"
            sp.upload_bytes(
                drive_id=drive_id,
                folder_path=out_folder,
                filename=json_name,
                content=json.dumps(result_obj, indent=2, ensure_ascii=False).encode("utf-8"),
                content_type="application/json",
            )

            notes_parts = []
            notes_parts.append(final.get("notes") or "")
            notes_parts.append(f"route_score:{route.score}")
            if route.matched_signals:
                notes_parts.append("route_matches:" + "|".join(route.matched_signals))
            notes_parts.append(f"vendor_intel_source:{intel.source}")
            notes_parts.append(f"amount_source:{amount_policy.amount_source}")
            if ai_route_used:
                notes_parts.append(f"ai_route_used:{ai_route_reason}")
            notes_parts.append(f"review_reason:{review.reason}")
            notes_parts.append(f"effective_confidence:{review.effective_confidence:.2f}")

            if status == "Ready":
                wrote = append_draft_row(
                    client_code=route.client_code,
                    row={
                        "ClientCode": route.client_code,
                        "AssignedTo": route.assigned_to or "",
                        "Date": final.get("document_date"),
                        "Vendor": final.get("vendor_name"),
                        "VendorCategory": intel.category,
                        "DocumentType": final.get("doc_type"),
                        "GLAccount": intel.gl_account,
                        "TaxCode": intel.tax_code,
                        "BookkeepingAmount": amount_policy.bookkeeping_amount,
                        "AmountSource": amount_policy.amount_source,
                        "Currency": final.get("currency"),
                        "SourceFile": name,
                        "SourceItemId": item_id,
                        "Fingerprint": fingerprint,
                        "Method": final.get("method"),
                        "Confidence": review.effective_confidence,
                        "VendorIntelSource": intel.source,
                        "Notes": "; ".join(x for x in notes_parts if x),
                    }
                )
                if wrote:
                    draft_rows_written += 1

            row_fields = {
                "Title": name,
                "Status": status,
                "FileName": name,
                "SharePointItemId": item_id,
                "Fingerprint": fingerprint,
                "Vendor": final.get("vendor_name") or "",
                "VendorCategory": intel.category,
                "Currency": final.get("currency") or "",
                "Method": final.get("method") or "",
                "Notes": "; ".join(x for x in notes_parts if x),
                "ClientCode": route.client_code or "",
                "ClientName": route.client_name or "",
                "AssignedTo": route.assigned_to or "",
                "Team": resolved_team,
                "Confidence": review.effective_confidence,
                "GLAccount": intel.gl_account,
                "TaxCode": intel.tax_code,
                "VendorIntelSource": intel.source,
                "AmountSource": amount_policy.amount_source,
            }

            if final.get("document_date"):
                row_fields["DocumentDate"] = final["document_date"]

            if amount_policy.bookkeeping_amount is not None:
                row_fields["BookkeepingAmount"] = amount_policy.bookkeeping_amount
                row_fields["Total"] = amount_policy.bookkeeping_amount

            gl.create_item(site_id, queue_ref.list_id, row_fields)
            list_rows_created += 1

            sp.move_item(drive_id, item_id, done_folder, new_name=name)

            add_fingerprint(fingerprint)

            processed_ids.add(item_id)
            handled += 1

        except Exception as e:
            errors.append(f"{name}: processing failed: {e}")
            try:
                sp.move_item(drive_id, item_id, err_folder, new_name=name)
                processed_ids.add(item_id)
                moved_to_exceptions += 1
                handled += 1
            except Exception as e2:
                errors.append(f"{name}: ALSO failed moving to Exceptions: {e2}")

    _save_processed_ids(processed_ids)

    return {
        "run_at": now_iso(),
        "ai_enabled": ai_enabled,
        "site_url": site_url,
        "inbox_folder": inbox_folder,
        "out_folder": out_folder,
        "done_folder": done_folder,
        "err_folder": err_folder,
        "checked_items": len(items),
        "handled": handled,
        "skipped_already_done": skipped_already_done,
        "skipped_duplicate_fingerprint": skipped_duplicate_fingerprint,
        "skipped_duplicate_list_row": skipped_duplicate_list_row,
        "moved_to_completed_as_duplicate": moved_to_completed_as_duplicate,
        "pdf_text_extracted": pdf_text_extracted,
        "ai_calls": ai_calls,
        "ai_route_calls": ai_route_calls,
        "list_rows_created": list_rows_created,
        "draft_rows_written": draft_rows_written,
        "moved_to_exceptions": moved_to_exceptions,
        "errors": errors,
    }


if __name__ == "__main__":
    print(json.dumps(process_sharepoint_once(max_files=10), indent=2, ensure_ascii=False))