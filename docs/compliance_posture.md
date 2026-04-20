# OtoCPA Compliance Posture — 2026-04-20

Status report for Canadian CPA-firm compliance obligations.

Scope: PIPEDA (federal), Quebec Law 25 (provincial), CPA Canada
professional standards, SOC 2 (aspirational). Each item is scored
**Covered** / **Partial** / **Gap** based on product features and
documented processes.

## Legend

- **Covered** — a product feature or process exists and is verified.
- **Partial** — some aspects are in place; others are not.
- **Gap** — nothing in place; identified as follow-up.

## PIPEDA (federal Canadian privacy law)

| Requirement | Status | Notes |
| --- | --- | --- |
| Accountability (designated officer) | Gap | No privacy officer documented in this codebase |
| Identifying purposes | Covered | Signup flow states purpose (accounting / bookkeeping) |
| Consent | Partial | Implicit by signup; no granular consent toggles |
| Limiting collection | Covered | Only fields needed for accounting are collected |
| Limiting use / disclosure / retention | Partial | 7-day cleanup on password_reset_used; documents retained indefinitely |
| Accuracy | Covered | `/document/update` versioned; audit trail on edits |
| Safeguards | Covered | bcrypt passwords, HTTPS (production), session HttpOnly+SameSite, CSRF, API-key on /ingest/openclaw |
| Openness | Gap | No public privacy policy document in repo |
| Individual access | Partial | Dashboard shows user's own data; no self-service export |
| Challenging compliance | Gap | No documented complaint process |

## Quebec Law 25 (provincial, stricter)

| Requirement | Status | Notes |
| --- | --- | --- |
| Privacy officer designation | Gap | Same as PIPEDA; provincially mandated since 2023 |
| Privacy impact assessment (PIA) | Gap | Not documented for new features |
| Granular consent | Partial | Collected implicitly at signup |
| Data-residency (Quebec / Canada) | Partial | SQLite is local; PostgreSQL in production should be documented as Canadian hosting |
| Cross-border transfer flags | Gap | No explicit check before sending data across borders (Anthropic / DocAI are US-based services) |
| Right to be forgotten | Gap | No self-service deletion; manual admin action |
| Breach notification (72h) | Gap | No documented incident response plan |

## CPA professional standards

| Requirement | Status | Notes |
| --- | --- | --- |
| 7-year retention of working papers | Partial | No automatic enforcement; no retention_until column on engagements |
| Client confidentiality | Covered | Per-firm scoping enforced at handler layer (R2 authz matrix) |
| Audit trail for regulatory review | Covered | login_attempts, client_portal_access, gl_transactions (append-only) |
| Separation of duties | Partial | Roles: owner / firm_admin / manager / employee exist; finer-grained permissions per action not matrix-tested |
| Engagement sign-off | Covered | Working papers have sign_off_at trigger preventing post-signoff edits |
| Review / approval workflows | Covered | Engagement status machine: planning → fieldwork → review → finalized |

## SOC 2 (aspirational)

| Requirement | Status | Notes |
| --- | --- | --- |
| Access controls documented | Partial | Code enforces; no written policy |
| Change management | Partial | Git history + PR reviews; no formal change log |
| Incident response plan | Gap | Not documented |
| Monitoring + alerting | Gap | /health endpoint exists; no production alerting |
| Backup + DR | Partial | Daily backup cron (R2 fix); no DR drill documented |
| Encryption in transit | Covered | HTTPS assumed in production; session cookie Secure flag |
| Encryption at rest | Partial | SQLite file is not encrypted; PostgreSQL encryption depends on infra |
| Vendor risk assessment | Gap | Anthropic / DocAI / Stripe usage not formally risk-assessed |

## Summary

- **Covered: 8 items** — the product layer meets the requirement.
- **Partial: 10 items** — some coverage but gaps remain.
- **Gap: 10 items** — no product or process coverage.

The largest cluster of gaps is **documentation + process**: privacy
officer, policies, incident response, PIA. These are
non-technical and should be addressed by the firm owner + legal
counsel before the product goes live with a real paying CPA
handling real client data.

## Recommendations, ranked by effort / impact

1. **Draft a privacy policy** (docs/privacy_policy.md) and surface it
   from the footer. One-page, English + French, compliant with PIPEDA
   + Law 25 baseline. *High impact, low effort.*
2. **Designate a privacy officer** in firm onboarding (a simple
   ``firm.privacy_officer_email`` field + policy text). *Law 25
   requirement.*
3. **Document the incident response procedure** in
   docs/incident_response.md covering backup restore, revocation of
   compromised API keys, Law-25 72-hour breach-notification window.
4. **Add a self-service data-export button** on the user settings
   page — ``GET /me/export`` returning JSON of everything the user
   owns. PIPEDA + Law 25 right-of-access.
5. **Add a ``retention_until`` column** on ``documents`` +
   ``working_papers``; set to ``engagement_date + 7 years`` by
   default. Cron to archive (not delete) past-retention records.

## What is NOT a product gap (deferrals)

- **SOC 2 audit** — organizational, not product. Defer until the
  firm has real paying customers and a compliance budget.
- **ISO 27001** — same.
- **Privacy Impact Assessment template** — deliverable for legal
  counsel, not the product team.
