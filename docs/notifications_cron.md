# Notification sender cron

Copy to `/etc/cron.d/otocpa-notifications` (root-owned, 0644):

```cron
# OtoCPA — client notification delivery (every 5 min).
# Drains client_notifications.status='pending' rows and sends email /
# WhatsApp. Summary written per run; individual failures are retried
# up to 3 times before status='failed'.
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

*/5 * * * * root cd /opt/otocpa && /usr/bin/python3 scripts/notification_sender_cron.py >> /var/log/otocpa/notifications.log 2>&1
```

Install:

```bash
sudo install -m 0644 -o root -g root /opt/otocpa/docs/notifications_cron.md.example /etc/cron.d/otocpa-notifications
```

The module itself (`src/integrations/notification_sender.py`) is pure
Python and can be imported directly by tests or the dashboard if a
manual drain is needed: call `send_pending_notifications(DB_PATH)`.
