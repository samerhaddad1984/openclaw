# OpenClaw → LedgerLink Bridge Setup

This document explains how to configure OpenClaw to forward inbound
WhatsApp and Telegram media messages to the LedgerLink accounting pipeline.

## Overview

```
Client (WhatsApp / Telegram)
        │  sends photo / PDF
        ▼
   OpenClaw gateway
        │  POST /ingest/openclaw  (JSON + base64 file)
        ▼
   LedgerLink  (review_dashboard.py, port 8787)
        │  OCR → documents table → review queue
        ▼
   Accountant reviews in dashboard
```

LedgerLink does **not** send WhatsApp or Telegram replies.  OpenClaw
handles all messaging; LedgerLink handles only the accounting.

---

## Prerequisites

1. LedgerLink dashboard is running (`python scripts/review_dashboard.py`).
2. The database schema is up to date:

   ```
   python scripts/migrate_db.py
   ```

   This adds `whatsapp_number` and `telegram_id` columns to
   `dashboard_users` and creates the `messaging_log` table.

3. Each client's dashboard user account has their WhatsApp number or
   Telegram ID saved in the database (see **Register clients** below).

---

## Register clients

In the LedgerLink admin UI go to **User Management** and set:

| Field             | Value                                  |
|-------------------|----------------------------------------|
| `whatsapp_number` | `+15141234567`  (with country code)    |
| `telegram_id`     | `123456789`     (numeric Telegram ID)  |

Or update directly via SQL:

```sql
UPDATE dashboard_users
   SET whatsapp_number = '+15141234567'
 WHERE username = 'client_abc';

UPDATE dashboard_users
   SET telegram_id = '123456789'
 WHERE username = 'client_abc';
```

---

## Configure OpenClaw

In your OpenClaw instance add a **webhook / forward** rule that fires on
every inbound media message and sends a POST request to LedgerLink.

### Endpoint

```
POST http://127.0.0.1:8787/ingest/openclaw
Content-Type: application/json
```

If OpenClaw and LedgerLink run on different machines, replace
`127.0.0.1:8787` with the LedgerLink host/port (or Cloudflare Tunnel URL).

### Payload schema

```json
{
  "platform":       "whatsapp",
  "sender_id":      "+15141234567",
  "media_url":      "https://cdn.openclaw.io/media/abc123",
  "media_type":     "image/jpeg",
  "client_message": "Here is my receipt",
  "file_bytes":     "<base64-encoded file content>"
}
```

| Field            | Type   | Required | Description                                      |
|------------------|--------|----------|--------------------------------------------------|
| `platform`       | string | yes      | `"whatsapp"` or `"telegram"`                     |
| `sender_id`      | string | yes      | Sender phone number or Telegram chat ID          |
| `media_url`      | string | no       | Original media URL (stored for reference only)   |
| `media_type`     | string | no       | MIME type (`image/jpeg`, `application/pdf`, …)   |
| `client_message` | string | no       | Text body of the message                         |
| `file_bytes`     | string | yes*     | Base64-encoded file content (*required for media)|

### Response schema

```json
{
  "ok":          true,
  "document_id": "doc_a1b2c3d4e5f6",
  "status":      "processed",
  "error":       null
}
```

| `status`         | Meaning                                               |
|------------------|-------------------------------------------------------|
| `processed`      | File ingested and stored; document_id returned        |
| `no_file`        | Text-only message; no document created                |
| `unknown_sender` | sender_id not found in dashboard_users                |
| `error`          | Processing failed; see `error` field for details      |

### HTTP status codes

| Code | Condition                   |
|------|-----------------------------|
| 200  | Success (or text-only msg)  |
| 400  | Bad request / processing error |
| 404  | Unknown sender              |

---

## Supported file types

| MIME type         | Handled as |
|-------------------|------------|
| `image/jpeg`      | Photo → Claude Vision OCR |
| `image/png`       | Photo → Claude Vision OCR |
| `image/webp`      | Photo → Claude Vision OCR |
| `image/tiff`      | Photo → Claude Vision OCR |
| `image/heic`      | Photo → Claude Vision OCR |
| `application/pdf` | PDF → pdfplumber text or Vision fallback |

Any other MIME type will be rejected by the OCR engine as an unsupported
format (the sender receives no document_id).

---

## Monitoring

Open the LedgerLink dashboard and go to **Diagnostics → Troubleshoot**.
The **OpenClaw Bridge** panel shows:

- Last received message timestamp
- Number of messages received today

Raw event history is in the `messaging_log` table:

```sql
SELECT * FROM messaging_log
 ORDER BY sent_at DESC
 LIMIT 20;
```

---

## Security notes

- The `/ingest/openclaw` endpoint accepts requests from any IP without
  session authentication.  Restrict access at the network level (firewall,
  Cloudflare Access, VPN) so only your OpenClaw instance can reach it.
- For an extra layer of protection you may add a shared-secret header in
  OpenClaw and validate it in the endpoint handler.
