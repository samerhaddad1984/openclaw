# Hybrid Assignment Model — Report

## What shipped

- **Schema (Phase 1)** — `clients.primary_employee_email`,
  `secondary_employee_email`, `assignment_updated_at`,
  `assignment_updated_by`; new `client_assignment_history` audit
  table; indexes on `(firm_code, primary/secondary)`. All additive
  via `ALTER TABLE ADD COLUMN` in `bootstrap_schema`. Schema drift
  guard passes.
- **Auto-assign (Phase 2)** — `src/integrations/auto_assign.py` routes
  every new document to its client's primary employee, falling back
  to secondary when primary is missing/inactive. Hooked into every
  ingest path:
  - `src/engines/upload_queue.save_and_queue_document` (web upload,
    public portal, multi-user portal)
  - `src/integrations/whatsapp` (Twilio inbound)
  - `src/integrations/openclaw_bridge` (OpenClaw push)
  Each auto-assignment writes a `review_workflow_audit` row with
  `action='auto_assign'` and the routing reason.
- **Employee view filtering (Phase 3)** — `_build_documents_where` in
  `scripts/review_dashboard.py` now applies a three-layer rule for
  employees: (1) explicit `review_workflow.assigned_to_email` override;
  (2) client-level primary/secondary when no override; (3) legacy
  pool only when the client has neither primary nor secondary.
  `render_document` grants access on override even outside the
  portfolio. `build_user_context` unions portfolio clients with
  primary/secondary clients so `/clients` auto-scopes.
- **Client assignment UI (Phase 4)** — `src/integrations/client_assignment.py`
  with `update_client_assignment`, `get_firm_employees`,
  `list_clients_with_assignment`, `get_assignment_history`. New POST
  route `/clients/assignment` is role-gated to owner / firm_admin.
  `/clients/edit` has a "Team Assignment" section with primary/secondary
  dropdowns + the last five history entries. `/clients` gains a Lead
  column + an "assigned to" filter dropdown (All / Mine / Pool / by
  employee) and employees can now open the page (auto-filtered to
  their clients).
- **Document-level override (Phase 5)** —
  `src/integrations/review_workflow.reassign_document`. Admins can
  reassign any document; the currently-assigned employee can hand
  off to a colleague (ask-for-help flow). Empty new assignee returns
  to the pool. POST `/document/reassign`. `render_document` shows the
  current assignee with a source-of-assignment badge
  (auto / reassigned) and a form with employee dropdown + reason code
  + optional free-text note.
- **Migration nudge (Phase 6)** — Home dashboard shows a bilingual
  "N clients have no primary employee" banner to owner / firm_admin
  with a one-click link to `/clients?assignee=__pool__`. Silent when
  the firm is fully set up.
- **Team workload report (Phase 7)** — `src/integrations/team_workload.py`
  aggregates per-employee: primary/secondary client counts, open
  docs, completed-this-week, and avg resolution hours over 90 days.
  New GET route `/reports/team_workload` (owner / firm_admin).
- **E2E verification (Phase 8)** — the 4-user / 3-client / 10-document
  scenario from the brief runs as a single test file and asserts the
  exact expected counts at every layer.

## Architecture

```
ingest (upload / portal / whatsapp / openclaw / multi_user_portal)
        │
        └── save_and_queue_document / process_file
                │
                ├── upsert_document          (documents row)
                │
                └── auto_assign_new_document
                        │
                        ├── clients.primary_employee_email → Sophie? active?
                        │        └─ yes → review_workflow.assigned_to_email = sophie
                        ├── else clients.secondary_employee_email → Jean? active?
                        │        └─ yes → review_workflow.assigned_to_email = jean
                        └── else → leave unassigned (firm pool)

reassign_document (owner/firm_admin/current-assignee)
        └── review_workflow.assigned_to_email := new
                + review_workflow_audit (action='reassign', notes=reason+prev+new)

build_user_context → allowed_clients = portfolio ∪ {primary,secondary}_on

_build_documents_where (for employee):
   explicit-override  OR  (no-override AND primary/secondary)  OR  legacy-pool
```

## E2E scenario verified

4 users, 3 clients, 10 documents, 7 assertion-rich tests in
`tests/workflow/test_hybrid_assignment_e2e.py`. Sophie starts with 5
TREMBLAY docs, Jean with 3 CAFE, Marie with 2 MARCHAND. After Sophie
reassigns 1 doc to Jean, Sophie has 4 and Jean has 4. Firm_admin and
owner see all 10 at every point. Team workload report shows the
distribution cleanly.

## What's still not built (honest)

- **No "vacation mode"** — a flag to auto-route to secondary without
  admin intervention. Currently an admin must either mark the
  employee inactive or manually reassign each document.
- **No auto-reassignment when an employee leaves the firm.** Deactivating
  the user still leaves their open documents pointing at their email.
  Admins must visit the pool-filtered /clients page and reassign
  manually.
- **No capacity-based auto-balancing.** If the senior has 50 docs and
  the junior has 5, the report shows the imbalance but admins still
  need to reassign by hand.
- **No dedicated bulk-assign page.** The design brief proposed a
  `/clients/bulk_assign` table-form. Instead the filter dropdown +
  per-client edit covers the same outcome without a new page; the
  dedicated UI is a future nice-to-have when firms onboard dozens at
  once.
- **Firm-wide retroactive assignment** when a primary is first set —
  intentional: the current pool docs stay in the pool. Phase 6's
  banner makes the backlog visible instead of silently reshuffling.

## For training materials

Update when the hybrid model goes live:

- `docs/training/cpa_owner_guide_{fr,en}.md` — add a Team Assignment
  section: client profile → primary/secondary dropdowns, workload
  report, the bilingual home nudge, and the pool filter.
- `docs/training/cpa_employee_guide_{fr,en}.md` — explain /my_tasks
  scoping (only docs explicitly assigned to the employee by email)
  and the Reassign button for ask-for-help.

## Regression tests added

54 tests under `tests/assignment/`:
- `test_auto_assign.py` — 15 tests (Phase 2)
- `test_employee_visibility.py` — 9 tests (Phase 3)
- `test_client_assignment.py` — 13 tests (Phase 4)
- `test_document_override.py` — 10 tests (Phase 5)
- `test_team_workload.py` — 7 tests (Phase 7)

Plus 7 end-to-end tests in `tests/workflow/test_hybrid_assignment_e2e.py`.

## Targeted regression results

381 tests pass across the relevant surfaces (assignment, workflow,
portal, admin, queue_filter, multi-tenant, client_portal, async
upload, onboarding, reports, close). The 58 failures in the full
`pytest tests/` run are pre-existing environmental / harness
failures (test_generate_test_data, test_stress_test, the
uninitialised `lang` in `do_GET`'s 500-path at line 22019 which was
authored on 2026-04-21, one day before this work began, per
`git blame`).
