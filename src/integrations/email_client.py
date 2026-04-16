import html
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logger = logging.getLogger(__name__)


def _smtp_config() -> tuple[str, int, str, str, str]:
    return (
        os.environ.get('SMTP_HOST', ''),
        int(os.environ.get('SMTP_PORT', 587) or 587),
        os.environ.get('SMTP_USER', ''),
        os.environ.get('SMTP_PASSWORD', ''),
        os.environ.get('SMTP_FROM', '') or os.environ.get('SMTP_USER', ''),
    )


def _send(to_email: str, subject: str, html_body: str, text_body: str = '') -> bool:
    host, port, user, password, sender = _smtp_config()
    if not host or not user:
        logger.warning('SMTP not configured - skipping email to %s', to_email)
        return False

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f'OtoCPA <{sender}>'
    msg['To'] = to_email
    if text_body:
        msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
    msg.attach(MIMEText(html_body, 'html', 'utf-8'))

    try:
        with smtplib.SMTP(host, port, timeout=15) as server:
            server.starttls()
            server.login(user, password)
            server.sendmail(sender, [to_email], msg.as_string())
        logger.info('email sent to %s (%s)', to_email, subject)
        return True
    except Exception as e:
        logger.error('email send failed to %s: %s', to_email, e)
        return False


def send_welcome_email(to_email: str, firm_name: str, firm_code: str,
                       username: str, password: str) -> bool:
    # Values are user-controlled (via Stripe customer email). Escape before
    # interpolating into HTML so a crafted email can't inject markup.
    fn = html.escape(firm_name or '')
    fc = html.escape(firm_code or '')
    un = html.escape(username or '')
    pw = html.escape(password or '')

    subject = 'Bienvenue sur OtoCPA / Welcome to OtoCPA'
    html_body = f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:24px;background:#f5f5f5;">
<div style="background:#1a3a5c;padding:32px;border-radius:8px 8px 0 0;text-align:center;">
    <h1 style="color:white;margin:0;font-size:32px;">OtoCPA</h1>
    <p style="color:#aaa;margin:8px 0 0;">Automatisation comptable pour CPAs qu&eacute;b&eacute;cois</p>
</div>
<div style="background:white;padding:32px;border-radius:0 0 8px 8px;">
    <h2 style="color:#1a3a5c;">Bienvenue, {fn}! / Welcome, {fn}!</h2>

    <p>Votre abonnement OtoCPA est activ&eacute;. / Your OtoCPA subscription is now active.</p>

    <div style="background:#f0f7ff;border:1px solid #2E75B6;border-radius:8px;padding:20px;margin:24px 0;">
        <h3 style="color:#1a3a5c;margin-top:0;">Vos identifiants / Your credentials</h3>
        <p><strong>URL:</strong> <a href="https://app.otocpa.com">https://app.otocpa.com</a></p>
        <p><strong>Code cabinet / Firm code:</strong> {fc}</p>
        <p><strong>Nom d'utilisateur / Username:</strong> {un}</p>
        <p><strong>Mot de passe temporaire / Temp password:</strong>
           <code style="background:#eee;padding:4px 8px;border-radius:4px;">{pw}</code></p>
        <p style="color:#cc0000;font-size:13px;">&#9888;&#65039; Changez votre mot de passe &agrave; la premi&egrave;re connexion / Change your password on first login</p>
    </div>

    <h3 style="color:#1a3a5c;">Prochaines &eacute;tapes / Next steps</h3>
    <ol>
        <li>Connectez-vous &agrave; <a href="https://app.otocpa.com/login">app.otocpa.com</a></li>
        <li>Ajoutez vos clients / Add your clients</li>
        <li>Partagez les codes QR / Share QR codes with clients</li>
        <li>Connectez QuickBooks / Connect QuickBooks</li>
        <li>Connectez les comptes bancaires / Connect bank accounts</li>
    </ol>

    <div style="text-align:center;margin-top:32px;">
        <a href="https://app.otocpa.com/login"
           style="background:#2ecc71;color:#000;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:bold;font-size:16px;">
            Connexion / Login &rarr;
        </a>
    </div>

    <hr style="margin:32px 0;border:none;border-top:1px solid #eee;">
    <p style="color:#666;font-size:13px;">
        Questions? Contactez-nous / Contact us: <a href="mailto:support@otocpa.com">support@otocpa.com</a><br>
        &copy; 2026 OtoCPA Inc. | <a href="https://app.otocpa.com/privacy">Politique de confidentialit&eacute;</a>
    </p>
</div>
</body>
</html>"""

    text_body = (
        f"Bienvenue sur OtoCPA / Welcome to OtoCPA\n\n"
        f"Votre abonnement est actif. / Your subscription is active.\n\n"
        f"URL: https://app.otocpa.com\n"
        f"Code cabinet / Firm code: {firm_code}\n"
        f"Nom d'utilisateur / Username: {username}\n"
        f"Mot de passe temporaire / Temp password: {password}\n\n"
        f"Connectez-vous: https://app.otocpa.com/login\n"
        f"Support: support@otocpa.com\n"
    )

    return _send(to_email, subject, html_body, text_body=text_body)


def send_email(to_email: str, subject: str, html_body: str) -> bool:
    return _send(to_email, subject, html_body)
