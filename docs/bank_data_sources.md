# Bank Data Sources — Reference

OtoCPA can pull bank-transaction data from two sources:

- **QuickBooks Online** — the client's existing QBO bank feeds (read
  via the v3 API: Purchase, Deposit, Transfer, Check entities).
- **Plaid** — a direct bank-side connection OtoCPA brokers.

At the client level, ``clients.bank_source`` drives routing:

| Value   | Meaning |
| ------- | ------- |
| `none`  | Nothing connected. Reconciliation will have no bank rows. |
| `qbo`   | QBO is the only source. Plaid is not connected / not needed. |
| `plaid` | Plaid is the only source. (This is the historical default.) |
| `both`  | QBO **and** Plaid both feed data; dedup runs automatically. |

## Decision tree at setup

Visit ``/clients/{code}/bank/setup``. The dashboard probes QBO and
Plaid and routes to one of five UI states:

```
                                has QBO?
                                /       \
                              yes        no
                               |          |
                     QBO has bank feeds?   has Plaid?
                        /       \              /   \
                      yes        no          yes    no
                       |          |           |      |
                  has Plaid?    plaid_rec   plaid   choice
                   /     \       message   _active  (neither)
                 yes      no
                  |        |
                both     qbo_recommended
               _active   (single source)
```

- ``qbo_recommended``: sync from QBO; don't connect Plaid.
- ``both_active``: both connected; dedup keeps the ledger clean.
- ``plaid_recommended``: QBO connected but no bank feeds — fall back
  to Plaid.
- ``plaid_active``: only Plaid; current behaviour preserved.
- ``choice``: neither — operator picks.

## When to use which

### QBO-only (``bank_source='qbo'``)

- Client already reconciles inside QBO and has bank feeds wired.
- CPA doesn't want to pay for a second data source.
- Trade-off: QBO's `MetaData.LastUpdatedTime` filter is the only
  incremental signal; there's no intraday push like Plaid. Cron
  cadence = 15 min.

### Plaid-only (``bank_source='plaid'``)

- Client doesn't use QBO, OR their QBO doesn't have bank feeds.
- Near-real-time coverage (Plaid pushes transactions as they post).

### Both (``bank_source='both'``)

- Client uses both QBO and a Plaid connection that OtoCPA brokered.
- Useful during migration windows when the CPA is still moving from
  Plaid to QBO-fed data, or when QBO's bank feeds are lagging.
- Dedup engine handles the overlap; see below.

## Migration between modes

| From  | To    | Steps |
| ----- | ----- | ----- |
| plaid | qbo   | Connect QBO at `/qbo/connect`. On `/bank/setup` click **Sync bank transactions from QuickBooks now**. ``clients.bank_source`` flips to `qbo`. Disconnect Plaid at `/bank/disconnect` (optional). |
| qbo   | plaid | Connect bank at `/bank/connect`. ``bank_source`` flips to `both`; dedup auto-runs. Disconnect QBO at `/qbo/disconnect` to finalise as `plaid`. |
| any   | none  | Disconnect both. ``bank_source`` is not rewritten automatically — set it via the setup page if needed. |

## Dedup FAQ

**Q: What does the dedup engine actually match on?**
Same firm + client, different sources, amount within 1¢, date
within 2 days. Confidence score is weighted (amount 0.6, date
proximity 0.2, description fuzz 0.2) and clamped to `[0, 1]`.

**Q: When does dedup auto-hide a row?**
Confidence ≥ 0.75. Below that we record the candidate in
`bank_tx_dedup` but keep both rows visible — the CPA reviews via
`/clients/{code}/bank/dedup`.

**Q: Who wins when both sources have the same transaction?**
QBO wins. QBO is closer to the CPA's system of record; Plaid is the
raw bank feed mirror. The Plaid row is hidden (`hidden_duplicate=1`)
while the QBO row stays the one reconciliation sees.

**Q: What if the engine got it wrong?**
Open `/clients/{code}/bank/dedup`, click **Un-dedup** on the row.
The Plaid row becomes visible again; the audit trail remembers
that the original auto-flag was manually reversed.

**Q: Are transfers double-counted?**
A transfer between two bank accounts in the same realm creates
two `bank_transactions` rows (one `out` on the source, one `in` on
the destination). That is the correct bank-ledger representation
— each side shows up on its own statement. They are **not** marked
as duplicates because they're on different accounts.

**Q: Check numbers across QBO and Plaid don't always match. Is
that a dedup problem?**
Banks format check numbers differently than QBO does. Amount and
date are the dominant match signals; description similarity is
only 0.2 of the score. A real match with very different
descriptions (`CHECK #1041` vs `Utility Co.`) can land below the
auto-hide threshold — those go to manual review by design.

## Scheduled sync

`scripts/qbo_scheduled_sync.py` runs every 15 min. For each client
where `bank_source ∈ ('qbo', 'both')` it:

1. Runs the usual incremental entity pull (COA / customers / JEs /
   bills / invoices).
2. Calls `QBOBankPull.pull_bank_transactions` for every active
   Bank-type account.
3. When `bank_source='both'`, runs the dedup engine in auto-apply
   mode.
4. Drains the webhook queue.

## Reconciliation behaviour

`src.integrations.bank_source_recon.get_unmatched_bank_transactions`
excludes `hidden_duplicate=1` rows by default. Callers that need the
full unfiltered list (audit trail, export) pass
`include_hidden=True`.

Every bank-transaction row carries `source`. The UI renders a
badge via `source_badge(row['source'])` so the CPA can tell at a
glance which feed a line came from.
