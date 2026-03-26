from __future__ import annotations

from typing import Any, Optional


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def format_confidence(value: Any) -> str:
    try:
        n = float(value)
        return f"{n:.2f}"
    except (ValueError, TypeError):
        return normalize_text(value) or "unknown"


def build_human_decision_summary(raw_result: dict[str, Any]) -> str:
    if not isinstance(raw_result, dict) or not raw_result:
        return "No decision details are available for this document yet."

    lines: list[str] = []

    # Basic Fields
    file_name = normalize_text(raw_result.get("file_name"))
    doc_type = normalize_text(raw_result.get("doc_type"))
    vendor = normalize_text(raw_result.get("vendor"))
    client_code = normalize_text(raw_result.get("client_code"))
    client_name = normalize_text(raw_result.get("client_name"))
    assigned_to = normalize_text(raw_result.get("assigned_to"))
    amount = normalize_text(raw_result.get("amount"))
    document_date = normalize_text(raw_result.get("document_date"))
    gl_account = normalize_text(raw_result.get("gl_account"))
    tax_code = normalize_text(raw_result.get("tax_code"))
    category = normalize_text(raw_result.get("category"))
    review_status = normalize_text(raw_result.get("review_status"))
    effective_confidence = format_confidence(raw_result.get("effective_confidence"))
    routing_method = normalize_text(raw_result.get("routing_method"))
    routing_score = normalize_text(raw_result.get("routing_score"))
    document_family = normalize_text(raw_result.get("document_family"))
    vendor_source = normalize_text(raw_result.get("vendor_source"))
    text_preview = normalize_text(raw_result.get("text_preview"))

    # Extract currency from rules output early
    raw_rules_output = raw_result.get("raw_rules_output", {})
    currency = ""
    if isinstance(raw_rules_output, dict):
        currency = normalize_text(raw_rules_output.get("currency"))

    # 1. File & Vendor
    if file_name:
        lines.append(f"File reviewed: {file_name}.")

    if vendor:
        v_line = f"Vendor detected as {vendor}"
        if vendor_source:
            v_line += f" using source {vendor_source}"
        lines.append(f"{v_line}.")

    # 2. Client & Assignment
    if client_code:
        c_line = f"Client routed to {client_code}"
        if client_name:
            c_line += f" ({client_name})"
        if assigned_to:
            c_line += f" and assigned to {assigned_to}"
        lines.append(f"{c_line}.")

    # 3. Document Classification
    if doc_type:
        d_line = f"Document classified as {doc_type}"
        if document_family:
            d_line += f" in family {document_family}"
        lines.append(f"{d_line}.")

    # 4. Financials
    if amount:
        amt_line = f"Amount extracted as {amount}"
        if currency:
            amt_line += f" {currency}"
        lines.append(f"{amt_line}.")

    if document_date:
        lines.append(f"Document date extracted as {document_date}.")

    # 5. Mappings
    if gl_account:
        lines.append(f"GL account mapped to {gl_account}.")
    if tax_code:
        lines.append(f"Tax code mapped to {tax_code}.")
    if category:
        lines.append(f"Category assigned as {category}.")

    # 6. Routing & Status
    if routing_method or routing_score:
        r_line = "Routing"
        if routing_method:
            r_line += f" used method {routing_method}"
        if routing_score:
            r_line += f" with score {routing_score}"
        lines.append(f"{r_line}.")

    if review_status:
        lines.append(f"Review status set to {review_status} with confidence {effective_confidence}.")

    # 7. AI & Rules Logic
    raw_ai_client_route = raw_result.get("raw_ai_client_route")
    if isinstance(raw_ai_client_route, dict):
        reason = normalize_text(raw_ai_client_route.get("reason"))
        conf = normalize_text(raw_ai_client_route.get("confidence"))
        if reason:
            prefix = f"AI routing reason ({conf} confidence):" if conf else "AI routing reason:"
            lines.append(f"{prefix} {reason}")

    raw_client_route = raw_result.get("raw_client_route")
    if isinstance(raw_client_route, dict):
        signals = raw_client_route.get("matched_signals")
        if isinstance(signals, list):
            top_signals = ", ".join(normalize_text(s) for s in signals[:8] if normalize_text(s))
            if top_signals:
                lines.append(f"Top routing signals: {top_signals}.")

    if isinstance(raw_rules_output, dict):
        notes = normalize_text(raw_rules_output.get("notes"))
        if notes:
            lines.append(f"Rules engine notes: {notes}.")

    # 8. Errors & General Notes
    for key, label in [("errors", "Errors found"), ("notes", "Additional notes")]:
        items = raw_result.get(key)
        if isinstance(items, list):
            cleaned = [normalize_text(i) for i in items if normalize_text(i)]
            if cleaned:
                lines.append(f"{label}: {' | '.join(cleaned)}.")

    # 9. Preview
    if text_preview:
        preview = " ".join(text_preview.replace("\r", " ").replace("\n", " ").split())
        if len(preview) > 280:
            preview = preview[:280] + "..."
        lines.append(f"Text preview: {preview}")

    return "\n\n".join(lines) if lines else "No readable decision summary could be generated."