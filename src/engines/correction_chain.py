"""
src/engines/correction_chain.py — Correction chain graph & economic event tracking.

Handles Traps 2, 3, 5, and 8:
  Trap 2: Credit memo decomposition with uncertainty.  Decompose only as far
           as evidence allows.  If exact tax split is unsupported, produce
           partial correction + uncertainty flag.
  Trap 3: Subcontractor overlap detection.  Flag when a new vendor invoice
           overlaps with work already billed by the original vendor.  Never
           silently net into original vendor document.
  Trap 5: Persistent n-way duplicate clustering.  Three variants of the same
           credit memo → one cluster, one correction chain, one refund linkage.
  Trap 8: Rollback is explicit, audited, and idempotent.  Re-import after
           rollback either recreates one clean state or stays suppressed.

Public interface
----------------
decompose_credit_memo_safe     — decompose CM with uncertainty flags
detect_overlap_anomaly         — detect cross-vendor work overlap
cluster_documents              — persistent n-way duplicate clustering
get_cluster_for_document       — find cluster a document belongs to
build_correction_chain_link    — add a link to the correction chain graph
get_full_correction_chain      — traverse the chain from root to leaf
rollback_correction            — explicit, audited rollback
check_reimport_after_rollback  — safe re-import gate
apply_single_correction        — ensure one economic event = one correction
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=False)


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    return dict(row) if row else {}


def _normalize_invoice_number(raw: str) -> str:
    """OCR-resilient normalization: O→0, I→1, L→1, strip dashes/spaces."""
    normed = raw.strip().upper()
    normed = normed.replace("O", "0").replace("I", "1").replace("L", "1")
    normed = re.sub(r"[\s\-]", "", normed)
    return normed


# =========================================================================
# TRAP 2 — Credit memo decomposition with uncertainty
# =========================================================================

# Quebec tax rates (2025)
GST_RATE = 0.05
QST_RATE = 0.09975


def decompose_credit_memo_safe(
    conn: sqlite3.Connection,
    *,
    credit_memo_id: str,
    credit_memo_amount_tax_included: float,
    original_invoice_id: str | None = None,
    has_tax_breakdown: bool = False,
    stated_gst: float | None = None,
    stated_qst: float | None = None,
    memo_text: str = "",
) -> dict[str, Any]:
    """Decompose a credit memo with uncertainty tracking.

    TRAP 2: If the credit memo is tax-included with no breakdown, we
    decompose only as far as evidence allows.  If the exact split cannot
    be proven, we produce a partial correction with uncertainty flags
    instead of guessing.

    Returns dict with:
        pretax, gst, qst, confidence, uncertainty_flags[], decomposition_method
    """
    result: dict[str, Any] = {
        "credit_memo_id": credit_memo_id,
        "total_tax_included": credit_memo_amount_tax_included,
        "pretax": None,
        "gst": None,
        "qst": None,
        "confidence": 0.0,
        "decomposition_method": "unknown",
        "uncertainty_flags": [],
        "partial_correction": False,
    }

    # Case 1: Explicit tax breakdown provided
    if has_tax_breakdown and stated_gst is not None and stated_qst is not None:
        result["gst"] = stated_gst
        result["qst"] = stated_qst
        result["pretax"] = credit_memo_amount_tax_included - stated_gst - stated_qst
        result["confidence"] = 0.95
        result["decomposition_method"] = "explicit_breakdown"
        return result

    # Case 2: Linked to original invoice — use proportional decomposition
    if original_invoice_id and _table_exists(conn, "documents"):
        orig = conn.execute(
            """SELECT amount, COALESCE(subtotal, 0) AS subtotal,
                      COALESCE(tax_total, 0) AS tax_total
               FROM documents WHERE document_id = ?""",
            (original_invoice_id,),
        ).fetchone()

        if orig and orig["amount"] and orig["amount"] != 0:
            orig_total = float(orig["amount"])
            ratio = abs(credit_memo_amount_tax_included / orig_total)

            # Try line-level decomposition if invoice_lines exist
            if _table_exists(conn, "invoice_lines"):
                lines = conn.execute(
                    """SELECT SUM(COALESCE(gst_amount, 0)) AS total_gst,
                              SUM(COALESCE(qst_amount, 0)) AS total_qst,
                              SUM(COALESCE(line_total_pretax, 0)) AS total_pretax
                       FROM invoice_lines WHERE document_id = ?""",
                    (original_invoice_id,),
                ).fetchone()

                if lines and lines["total_pretax"] and lines["total_pretax"] > 0:
                    result["gst"] = round(float(lines["total_gst"]) * ratio, 2)
                    result["qst"] = round(float(lines["total_qst"]) * ratio, 2)
                    result["pretax"] = round(
                        credit_memo_amount_tax_included - result["gst"] - result["qst"], 2
                    )
                    result["confidence"] = 0.80
                    result["decomposition_method"] = "proportional_from_invoice_lines"
                    result["uncertainty_flags"].append({
                        "flag": "PROPORTIONAL_TAX_ESTIMATE",
                        "description_en": (
                            "Tax split estimated proportionally from original "
                            "invoice line items. Exact credit memo tax allocation "
                            "not stated on document."
                        ),
                        "description_fr": (
                            "Ventilation de taxes estimée proportionnellement "
                            "des lignes de la facture originale. L'allocation "
                            "exacte n'est pas indiquée sur le document."
                        ),
                    })
                    return result

            # Fall back to document-level proportional
            if orig["subtotal"] and orig["subtotal"] > 0:
                orig_gst = orig_total * GST_RATE / (1 + GST_RATE + QST_RATE)
                orig_qst = orig_total * QST_RATE / (1 + GST_RATE + QST_RATE)
                result["gst"] = round(orig_gst * ratio, 2)
                result["qst"] = round(orig_qst * ratio, 2)
                result["pretax"] = round(
                    credit_memo_amount_tax_included - result["gst"] - result["qst"], 2
                )
                result["confidence"] = 0.65
                result["decomposition_method"] = "proportional_from_document"
                result["partial_correction"] = True
                result["uncertainty_flags"].append({
                    "flag": "DOCUMENT_LEVEL_TAX_ESTIMATE",
                    "description_en": (
                        "Tax split estimated from document totals, not line items. "
                        "Credit memo may apply to subset of original invoice."
                    ),
                    "description_fr": (
                        "Ventilation de taxes estimée des totaux du document. "
                        "La note de crédit peut s'appliquer à un sous-ensemble."
                    ),
                })
                return result

    # Case 3: No linked invoice, no breakdown — reverse-engineer from standard rates
    # This is the weakest decomposition: assume standard Quebec GST+QST
    combined_rate = 1 + GST_RATE + QST_RATE
    estimated_pretax = round(credit_memo_amount_tax_included / combined_rate, 2)
    estimated_gst = round(estimated_pretax * GST_RATE, 2)
    estimated_qst = round(estimated_pretax * QST_RATE, 2)

    result["pretax"] = estimated_pretax
    result["gst"] = estimated_gst
    result["qst"] = estimated_qst
    result["confidence"] = 0.45  # Below posting threshold
    result["decomposition_method"] = "reverse_engineered_standard_rates"
    result["partial_correction"] = True
    result["uncertainty_flags"].append({
        "flag": "TAX_SPLIT_UNPROVEN",
        "description_en": (
            "No tax breakdown on credit memo and no linked original invoice. "
            "Tax split is estimated using standard GST/QST rates. "
            "This BLOCKS automatic posting — manual confirmation required."
        ),
        "description_fr": (
            "Aucune ventilation de taxes sur la note de crédit et aucune "
            "facture originale liée. La ventilation est estimée avec les "
            "taux standards TPS/TVQ. Ceci BLOQUE la comptabilisation "
            "automatique — confirmation manuelle requise."
        ),
    })

    # Check if memo text gives clues about what was credited
    _memo_lower = memo_text.lower()
    components_mentioned: list[str] = []
    if any(w in _memo_lower for w in ("monitoring", "surveillance", "abonnement", "subscription")):
        components_mentioned.append("subscription/monitoring")
    if any(w in _memo_lower for w in ("commissioning", "mise en service", "installation")):
        components_mentioned.append("commissioning/installation")
    if any(w in _memo_lower for w in ("hardware", "matériel", "équipement", "pump", "pompe")):
        components_mentioned.append("hardware/equipment")

    if components_mentioned:
        result["uncertainty_flags"].append({
            "flag": "PARTIAL_COMPONENT_IDENTIFICATION",
            "description_en": (
                f"Credit memo text mentions: {', '.join(components_mentioned)}. "
                f"But exact allocation per component is not stated."
            ),
            "description_fr": (
                f"Le texte de la note de crédit mentionne: {', '.join(components_mentioned)}. "
                f"Mais l'allocation exacte par composante n'est pas indiquée."
            ),
            "components": components_mentioned,
        })

    return result


# =========================================================================
# TRAP 3 — Subcontractor overlap detection
# =========================================================================

# Keywords indicating overlapping work scopes — grouped by concept
# Each group is a set of synonyms (EN + FR) that mean the same work scope.
_OVERLAP_KEYWORD_GROUPS: list[set[str]] = [
    {"commissioning", "mise en service", "commissionnement", "startup", "start-up", "start up", "démarrage"},
    {"installation", "install", "installer"},
    {"calibration", "étalonnage", "calibrage"},
    {"setup", "configuration", "mise en place"},
    {"maintenance", "entretien"},
    {"repair", "réparation"},
]

# Flat set of all keywords for initial text scan
_OVERLAP_KEYWORDS: set[str] = set()
for _group in _OVERLAP_KEYWORD_GROUPS:
    _OVERLAP_KEYWORDS.update(_group)

def _find_overlap_groups(text: str) -> set[int]:
    """Return indices of keyword groups found in text."""
    found: set[int] = set()
    text_lower = text.lower()
    for i, group in enumerate(_OVERLAP_KEYWORD_GROUPS):
        for kw in group:
            if kw in text_lower:
                found.add(i)
                break
    return found


def detect_overlap_anomaly(
    conn: sqlite3.Connection,
    *,
    new_document_id: str,
    client_code: str,
    lookback_days: int = 90,
) -> list[dict[str, Any]]:
    """Detect when a new invoice overlaps with work already billed by another vendor.

    TRAP 3: The new Quebec subcontractor invoice (commissioning) could overlap
    with the original vendor's invoice that included commissioning.  We flag
    the anomaly — never silently net or merge.

    Returns list of overlap anomalies found.
    """
    if not _table_exists(conn, "documents"):
        return []

    new_doc = conn.execute(
        """SELECT document_id, vendor, document_date, amount, memo,
                  COALESCE(raw_ocr_text, '') AS ocr_text
           FROM documents WHERE document_id = ?""",
        (new_document_id,),
    ).fetchone()
    if not new_doc:
        return []

    new_doc = _row_dict(new_doc)
    new_vendor = (new_doc.get("vendor") or "").lower()
    new_date = new_doc.get("document_date") or ""
    new_text = f"{new_doc.get('memo', '')} {new_doc.get('ocr_text', '')}".lower()

    # Find work-scope keyword GROUPS in the new document
    new_groups = _find_overlap_groups(new_text)
    if not new_groups:
        return []

    # Search for other documents from different vendors in the same client/period
    rows = conn.execute(
        """SELECT document_id, vendor, document_date, amount, memo,
                  COALESCE(raw_ocr_text, '') AS ocr_text
           FROM documents
           WHERE client_code = ?
             AND document_id != ?
             AND document_date >= date(?, '-' || ? || ' days')
             AND document_date <= date(?, '+' || ? || ' days')""",
        (client_code, new_document_id,
         new_date, str(lookback_days), new_date, str(lookback_days)),
    ).fetchall()

    anomalies: list[dict[str, Any]] = []
    for row in rows:
        row = _row_dict(row)
        other_vendor = (row.get("vendor") or "").lower()

        # Only flag cross-vendor overlap, not same vendor
        # Use token-based comparison for fuzzy vendor matching
        new_tokens = set(new_vendor.split())
        other_tokens = set(other_vendor.split())
        if new_tokens and other_tokens:
            overlap_ratio = len(new_tokens & other_tokens) / max(len(new_tokens), len(other_tokens))
            if overlap_ratio >= 0.5:
                continue  # Same or very similar vendor — skip
        elif other_vendor == new_vendor:
            continue

        other_text = f"{row.get('memo', '')} {row.get('ocr_text', '')}".lower()
        other_groups = _find_overlap_groups(other_text)
        shared_groups = new_groups & other_groups

        if shared_groups:
            # Map shared groups back to readable keywords
            shared_keywords: list[str] = []
            for gi in shared_groups:
                shared_keywords.extend(sorted(_OVERLAP_KEYWORD_GROUPS[gi])[:2])
            anomaly = {
                "document_a_id": row["document_id"],
                "document_b_id": new_document_id,
                "vendor_a": row.get("vendor", ""),
                "vendor_b": new_doc.get("vendor", ""),
                "overlap_type": "work_scope",
                "shared_keywords": shared_keywords,
                "description_en": (
                    f"Vendor '{row.get('vendor', '')}' and vendor "
                    f"'{new_doc.get('vendor', '')}' both reference: "
                    f"{', '.join(shared_keywords)}. Possible duplicate billing."
                ),
                "description_fr": (
                    f"Fournisseur '{row.get('vendor', '')}' et fournisseur "
                    f"'{new_doc.get('vendor', '')}' mentionnent tous les deux: "
                    f"{', '.join(shared_keywords)}. Facturation potentiellement double."
                ),
            }
            anomalies.append(anomaly)

            # Persist the anomaly
            if _table_exists(conn, "overlap_anomalies"):
                now = _utc_now()
                conn.execute(
                    """INSERT OR IGNORE INTO overlap_anomalies
                           (client_code, document_a_id, document_b_id,
                            vendor_a, vendor_b, overlap_type,
                            overlap_description, status, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?)""",
                    (client_code, row["document_id"], new_document_id,
                     row.get("vendor", ""), new_doc.get("vendor", ""),
                     "work_scope", _json_dumps(anomaly), now),
                )
                conn.commit()

    return anomalies


# =========================================================================
# TRAP 5 — Persistent duplicate clustering
# =========================================================================

def _build_cluster_key(
    vendor: str,
    invoice_number: str,
    amount: float | None,
) -> str:
    """Build a deterministic cluster key from vendor+invoice+amount."""
    parts = [
        _normalize_invoice_number(vendor or ""),
        _normalize_invoice_number(invoice_number or ""),
        str(round(abs(amount or 0), 2)),
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def cluster_documents(
    conn: sqlite3.Connection,
    document_ids: list[str],
    *,
    client_code: str,
    reason: str = "duplicate_detection",
) -> dict[str, Any]:
    """Cluster multiple documents as representing the same economic event.

    TRAP 5: Three variants of the same credit memo → one cluster.
    The cluster head is the first document ingested.  Additional members
    are linked but do NOT create additional economic effects.
    """
    if not _table_exists(conn, "document_clusters"):
        raise RuntimeError("document_clusters table missing — run migrate_db.py")
    if len(document_ids) < 2:
        return {"error": "Need at least 2 documents to form a cluster"}

    # Load documents to build cluster key
    docs: list[dict[str, Any]] = []
    for did in document_ids:
        row = conn.execute(
            "SELECT * FROM documents WHERE document_id = ?", (did,)
        ).fetchone()
        if row:
            docs.append(_row_dict(row))

    if not docs:
        return {"error": "No documents found"}

    # Build cluster key from first document
    first = docs[0]
    cluster_key = _build_cluster_key(
        first.get("vendor", ""),
        first.get("invoice_number", ""),
        first.get("amount"),
    )

    now = _utc_now()

    # Check if cluster already exists
    existing = conn.execute(
        "SELECT cluster_id, cluster_head_id FROM document_clusters WHERE cluster_key = ?",
        (cluster_key,),
    ).fetchone()

    if existing:
        cluster_id = existing["cluster_id"]
        cluster_head_id = existing["cluster_head_id"]
    else:
        # Create new cluster — first document is the head
        cluster_head_id = document_ids[0]
        cursor = conn.execute(
            """INSERT INTO document_clusters
                   (cluster_key, client_code, cluster_head_id,
                    member_count, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, 'active', ?, ?)""",
            (cluster_key, client_code, cluster_head_id, len(document_ids), now, now),
        )
        cluster_id = cursor.lastrowid

    # Add members
    added = 0
    for i, did in enumerate(document_ids):
        is_head = 1 if did == cluster_head_id else 0
        try:
            conn.execute(
                """INSERT INTO document_cluster_members
                       (cluster_id, document_id, is_cluster_head,
                        similarity_score, variant_notes, added_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (cluster_id, did, is_head, 1.0 if is_head else 0.85,
                 reason, now),
            )
            added += 1
        except sqlite3.IntegrityError:
            pass  # Already a member

    # Update member count
    count = conn.execute(
        "SELECT COUNT(*) AS cnt FROM document_cluster_members WHERE cluster_id = ?",
        (cluster_id,),
    ).fetchone()["cnt"]
    conn.execute(
        "UPDATE document_clusters SET member_count = ?, updated_at = ? WHERE cluster_id = ?",
        (count, now, cluster_id),
    )
    conn.commit()

    return {
        "cluster_id": cluster_id,
        "cluster_key": cluster_key,
        "cluster_head_id": cluster_head_id,
        "member_count": count,
        "members_added": added,
        "document_ids": document_ids,
    }


def get_cluster_for_document(
    conn: sqlite3.Connection,
    document_id: str,
) -> dict[str, Any] | None:
    """Find the cluster a document belongs to, if any."""
    if not _table_exists(conn, "document_cluster_members"):
        return None

    row = conn.execute(
        """SELECT dc.*, dcm.is_cluster_head
           FROM document_cluster_members dcm
           JOIN document_clusters dc ON dcm.cluster_id = dc.cluster_id
           WHERE dcm.document_id = ?""",
        (document_id,),
    ).fetchone()

    if not row:
        return None

    result = _row_dict(row)

    # Get all members
    members = conn.execute(
        """SELECT document_id, is_cluster_head, similarity_score
           FROM document_cluster_members
           WHERE cluster_id = ?
           ORDER BY is_cluster_head DESC, added_at""",
        (result["cluster_id"],),
    ).fetchall()
    result["members"] = [_row_dict(m) for m in members]

    return result


def is_duplicate_of_cluster_head(
    conn: sqlite3.Connection,
    document_id: str,
) -> bool:
    """Check if a document is a non-head member of a cluster.

    TRAP 5: If True, this document should NOT create additional economic
    effects — the cluster head already represents this event.
    """
    cluster = get_cluster_for_document(conn, document_id)
    if not cluster:
        return False
    return cluster.get("cluster_head_id") != document_id


# =========================================================================
# TRAP 2+5+8 — Correction chain graph
# =========================================================================

def build_correction_chain_link(
    conn: sqlite3.Connection,
    *,
    chain_root_id: str,
    client_code: str,
    source_document_id: str,
    target_document_id: str,
    link_type: str = "credit_memo",
    economic_effect: str = "reduction",
    amount: float | None = None,
    tax_impact_gst: float | None = None,
    tax_impact_qst: float | None = None,
    uncertainty_flags: list[dict[str, Any]] | None = None,
    created_by: str = "system",
) -> dict[str, Any]:
    """Add a link to the correction chain graph.

    TRAP 5: Before adding, check if the target document is a duplicate
    (non-head cluster member).  If so, skip — the head already handles it.
    """
    if not _table_exists(conn, "correction_chains"):
        raise RuntimeError("correction_chains table missing — run migrate_db.py")

    # TRAP 5 gate: reject if target is a non-head duplicate
    if is_duplicate_of_cluster_head(conn, target_document_id):
        return {
            "status": "skipped",
            "reason": "target_document_is_duplicate_cluster_member",
            "target_document_id": target_document_id,
        }

    # Check for existing active link (idempotency)
    existing = conn.execute(
        """SELECT chain_id FROM correction_chains
           WHERE source_document_id = ? AND target_document_id = ?
             AND status = 'active'""",
        (source_document_id, target_document_id),
    ).fetchone()

    if existing:
        return {
            "status": "already_exists",
            "chain_id": existing["chain_id"],
        }

    now = _utc_now()
    cursor = conn.execute(
        """INSERT INTO correction_chains
               (chain_root_id, client_code, source_document_id,
                target_document_id, link_type, economic_effect,
                amount, tax_impact_gst, tax_impact_qst,
                uncertainty_flags, status, created_by, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)""",
        (chain_root_id, client_code, source_document_id,
         target_document_id, link_type, economic_effect,
         amount, tax_impact_gst, tax_impact_qst,
         _json_dumps(uncertainty_flags or []), created_by, now),
    )
    conn.commit()

    return {
        "status": "created",
        "chain_id": cursor.lastrowid,
        "chain_root_id": chain_root_id,
        "link_type": link_type,
    }


def get_full_correction_chain(
    conn: sqlite3.Connection,
    chain_root_id: str,
) -> dict[str, Any]:
    """Traverse the full correction chain from root to all leaves.

    TRAP 9: Returns the complete graph showing original → credit memo →
    correction entry → refund → rollback.
    """
    if not _table_exists(conn, "correction_chains"):
        return {"chain_root_id": chain_root_id, "links": []}

    links = conn.execute(
        """SELECT * FROM correction_chains
           WHERE chain_root_id = ?
           ORDER BY created_at""",
        (chain_root_id,),
    ).fetchall()

    chain_links = []
    total_economic_impact = 0.0
    total_gst_impact = 0.0
    total_qst_impact = 0.0

    for link in links:
        link_dict = _row_dict(link)
        link_dict["uncertainty_flags"] = json.loads(link_dict.get("uncertainty_flags", "[]"))
        chain_links.append(link_dict)

        if link_dict["status"] == "active":
            amt = link_dict.get("amount") or 0
            if link_dict["economic_effect"] == "reduction":
                total_economic_impact -= amt
            else:
                total_economic_impact += amt
            total_gst_impact += link_dict.get("tax_impact_gst") or 0
            total_qst_impact += link_dict.get("tax_impact_qst") or 0

    return {
        "chain_root_id": chain_root_id,
        "links": chain_links,
        "link_count": len(chain_links),
        "active_links": sum(1 for l in chain_links if l["status"] == "active"),
        "total_economic_impact": round(total_economic_impact, 2),
        "total_gst_impact": round(total_gst_impact, 2),
        "total_qst_impact": round(total_qst_impact, 2),
    }


# =========================================================================
# TRAP 5 — Apply single correction per economic event
# =========================================================================

def apply_single_correction(
    conn: sqlite3.Connection,
    *,
    credit_memo_id: str,
    original_invoice_id: str,
    client_code: str,
    decomposition: dict[str, Any],
    created_by: str = "system",
) -> dict[str, Any]:
    """Ensure one economic credit memo creates exactly one correction chain.

    TRAP 5: Even if three variants arrive, only the cluster head creates
    the correction.  This function is idempotent.
    """
    # Check if this document is a non-head cluster member
    if is_duplicate_of_cluster_head(conn, credit_memo_id):
        cluster = get_cluster_for_document(conn, credit_memo_id)
        return {
            "status": "skipped_duplicate",
            "reason": "document is a non-head cluster member",
            "cluster_head_id": cluster["cluster_head_id"] if cluster else None,
            "credit_memo_id": credit_memo_id,
        }

    # Check if correction chain already exists for this pair
    if _table_exists(conn, "correction_chains"):
        existing = conn.execute(
            """SELECT chain_id FROM correction_chains
               WHERE source_document_id = ? AND target_document_id = ?
                 AND status = 'active'""",
            (original_invoice_id, credit_memo_id),
        ).fetchone()
        if existing:
            return {
                "status": "already_applied",
                "chain_id": existing["chain_id"],
            }

    # Build the correction chain link
    return build_correction_chain_link(
        conn,
        chain_root_id=original_invoice_id,
        client_code=client_code,
        source_document_id=original_invoice_id,
        target_document_id=credit_memo_id,
        link_type="credit_memo",
        economic_effect="reduction",
        amount=decomposition.get("pretax"),
        tax_impact_gst=decomposition.get("gst"),
        tax_impact_qst=decomposition.get("qst"),
        uncertainty_flags=decomposition.get("uncertainty_flags", []),
        created_by=created_by,
    )


# =========================================================================
# TRAP 8 — Rollback (explicit, audited, idempotent)
# =========================================================================

def rollback_correction(
    conn: sqlite3.Connection,
    *,
    chain_id: int,
    client_code: str,
    rolled_back_by: str,
    rollback_reason: str,
    block_reimport: bool = False,
) -> dict[str, Any]:
    """Explicitly roll back a correction chain link.

    TRAP 8: Rollback is:
    - Explicit: requires reason and user identity
    - Audited: full state captured before and after
    - Idempotent: rolling back an already-rolled-back link is a no-op
    """
    if not _table_exists(conn, "correction_chains"):
        raise RuntimeError("correction_chains table missing")

    # Fetch current state
    link = conn.execute(
        "SELECT * FROM correction_chains WHERE chain_id = ?", (chain_id,)
    ).fetchone()
    if not link:
        raise ValueError(f"Correction chain link not found: {chain_id}")

    link_dict = _row_dict(link)

    # Idempotent: already rolled back
    if link_dict["status"] == "rolled_back":
        return {
            "status": "already_rolled_back",
            "chain_id": chain_id,
        }

    state_before = _json_dumps(link_dict)
    now = _utc_now()

    # Mark the chain link as rolled back
    conn.execute(
        """UPDATE correction_chains
           SET status = 'rolled_back', superseded_by = NULL
           WHERE chain_id = ?""",
        (chain_id,),
    )

    # Capture state after rollback
    updated = conn.execute(
        "SELECT * FROM correction_chains WHERE chain_id = ?", (chain_id,)
    ).fetchone()
    state_after = _json_dumps(_row_dict(updated))

    # Log the rollback
    if _table_exists(conn, "rollback_log"):
        conn.execute(
            """INSERT INTO rollback_log
                   (client_code, target_type, target_id, rollback_reason,
                    rolled_back_by, state_before_json, state_after_json,
                    is_reimport_blocked, created_at)
               VALUES (?, 'correction_chain', ?, ?, ?, ?, ?, ?, ?)""",
            (client_code, str(chain_id), rollback_reason,
             rolled_back_by, state_before, state_after,
             1 if block_reimport else 0, now),
        )

    # Audit log
    if _table_exists(conn, "audit_log"):
        conn.execute(
            """INSERT INTO audit_log (event_type, document_id, prompt_snippet, created_at)
               VALUES (?, ?, ?, ?)""",
            (
                "correction_rolled_back",
                link_dict.get("target_document_id", ""),
                _json_dumps({
                    "chain_id": chain_id,
                    "rolled_back_by": rolled_back_by,
                    "reason": rollback_reason,
                    "block_reimport": block_reimport,
                }),
                now,
            ),
        )

    conn.commit()

    return {
        "status": "rolled_back",
        "chain_id": chain_id,
        "rolled_back_by": rolled_back_by,
        "reimport_blocked": block_reimport,
    }


def check_reimport_after_rollback(
    conn: sqlite3.Connection,
    document_id: str,
    client_code: str,
) -> dict[str, Any]:
    """Check whether re-import of a document is safe after a rollback.

    TRAP 8: After rollback, re-import should either:
    - Recreate one clean correction state (if rollback did not block reimport)
    - Stay suppressed (if rollback intentionally archived that evidence)
    - NEVER produce duplicate economic effects
    """
    result: dict[str, Any] = {
        "document_id": document_id,
        "can_reimport": True,
        "reasons": [],
    }

    # Check rollback log for blocked reimports
    if _table_exists(conn, "rollback_log"):
        blocked = conn.execute(
            """SELECT * FROM rollback_log
               WHERE client_code = ?
                 AND is_reimport_blocked = 1
                 AND state_before_json LIKE ?""",
            (client_code, f"%{document_id}%"),
        ).fetchall()

        if blocked:
            result["can_reimport"] = False
            result["reasons"].append({
                "reason": "reimport_blocked_by_rollback",
                "description_en": (
                    "A previous rollback explicitly blocked re-import of "
                    "this document's correction."
                ),
                "description_fr": (
                    "Un annulation précédente a explicitement bloqué la "
                    "réimportation de cette correction."
                ),
                "rollback_ids": [_row_dict(b)["rollback_id"] for b in blocked],
            })
            return result

    # Check if active correction chain already exists
    if _table_exists(conn, "correction_chains"):
        active = conn.execute(
            """SELECT chain_id FROM correction_chains
               WHERE target_document_id = ? AND status = 'active'""",
            (document_id,),
        ).fetchone()

        if active:
            result["can_reimport"] = False
            result["reasons"].append({
                "reason": "active_correction_exists",
                "description_en": (
                    "An active correction chain link already exists for this "
                    "document. Re-import would create duplicate economic effects."
                ),
                "description_fr": (
                    "Un lien de chaîne de correction actif existe déjà pour "
                    "ce document. La réimportation créerait des effets "
                    "économiques en double."
                ),
                "existing_chain_id": active["chain_id"],
            })

    # Check cluster membership
    if is_duplicate_of_cluster_head(conn, document_id):
        result["can_reimport"] = False
        result["reasons"].append({
            "reason": "duplicate_cluster_member",
            "description_en": (
                "Document is a non-head member of a duplicate cluster. "
                "Only the cluster head should create corrections."
            ),
            "description_fr": (
                "Le document est un membre non-chef d'un cluster de doublons. "
                "Seul le chef du cluster devrait créer des corrections."
            ),
        })

    return result
