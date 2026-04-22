# Client Admin Guide — OtoCPA

For the client-side portal admin — the person at the client company
who coordinates uploads to the CPA firm. If you're a contributor
(non-admin), see
[client_contributor_guide_en.md](client_contributor_guide_en.md).

## 1. Accepting your invitation

1. You get an email from your CPA with subject "invited you to
   submit receipts on OtoCPA".
2. Click **Accept invitation**. The link expires in 14 days.
3. The accept page is bilingual; use the toggle top-right to switch
   FR / EN.
4. After accepting, you're redirected to your **personal portal**.

## 2. Your personal portal

Your personal URL is unique and private; keep it confidential. The
portal has:

- **Upload**: receipts, invoices, statements (photo, PDF, batch).
- **My documents**: history of what you've submitted.
- **Messages**: two-way thread with your CPA.
- **Manage team** (admin only): invite / suspend / remove
  colleagues.

## 3. Inviting colleagues

Tab **Manage team → Invite**:

1. Enter email + full name.
2. Pick the role:
   - **Admin**: can invite others, suspend, remove.
   - **Contributor**: can only upload and send messages.
3. Optional: pick FR or EN for the invitation email language.
4. Click **Send invitation**. The email goes out within 5 minutes
   (auto-retries on failure).

## 4. Managing your team

For each member:

- **Suspend**: blocks access without removing history.
- **Reactivate**: restore a suspended user's access.
- **Remove**: invalidates tokens immediately; history preserved for
  audit.
- **Change role**: promote a contributor to admin, or vice versa.

⚠ **You cannot remove yourself.** Ask your CPA if you need to leave
the organization.

## 5. Uploading receipts

Three options:

- **Photo**: phone camera (iOS / Android).
- **PDF**: drag-drop or file picker.
- **WhatsApp**: send the photo to your CPA's WhatsApp number; it
  shows up in your queue.

Each upload can have a **note** (e.g., "grocery invoice").

## 6. Connecting your bank (if not via QuickBooks)

If your bookkeeping isn't already in QuickBooks with bank feeds:

1. Tab **Bank → Connect**.
2. Pick your institution in Plaid (Plaid secure portal — no password
   stored with your CPA).
3. OtoCPA imports transactions automatically.

**No action needed** if your CPA already uses QBO with your bank
feeds — OtoCPA pulls directly.

## 7. Sending a message to your CPA

Tab **Messages → New**:

- Type your question.
- Click **Send**.
- It appears immediately in your thread; your CPA gets a
  notification.

## 8. Checking submission status

Tab **My documents**:

- **Queued**: received, in auto-processing.
- **In review**: being examined by a CPA employee.
- **Approved / Posted**: validated and recorded.
- **Rejected**: returned with a note; action required.

## 9. Registering WhatsApp numbers for your team

Each person on your team can register one WhatsApp number
(company handset or personal phone) on their portal row. Once
registered, they can send receipt photos directly to your firm's
shared WhatsApp number and every document gets tagged with *their*
name instead of being anonymous.

On the **Manage team** page, each row has a WhatsApp field. Paste
the number (any format — we normalize to `+1 (514) 555-0100`) and
hit *Save*. The live check tells you whether the number is free
or already claimed by another user.

**Security notes:**

- **Only registered numbers work.** Random numbers messaging the
  firm Twilio line are rejected with a bilingual "not registered"
  reply, so nothing unattributed lands in your queue.
- **Removing a user revokes their WhatsApp.** Next message from
  that handset gets "your WhatsApp access has been revoked".
  Suspend is similar but temporary.
- **Numbers are unique across the whole system.** If someone else
  at a different firm already registered the same handset, the
  form tells you so you don't silently collide at Twilio.

## FAQ

**Q: I lost my personal link.**
A: Ask another admin on your team, or contact your CPA who can
regenerate your token.

**Q: A colleague left the company.**
A: Go to **Manage team → [Person] → Remove**. Access revoked on the
spot; their uploads stay in your history.

**Q: Does my CPA see my bank credentials?**
A: No. Plaid handles auth; OtoCPA only receives transactions, never
your credentials.

## Getting help

- **Your CPA** (first line): via the **Messages** tab.
- **OtoCPA support**: support@otocpa.com
