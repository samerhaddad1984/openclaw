import html
import logging
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import resend

logger = logging.getLogger(__name__)

resend.api_key = os.environ.get('RESEND_API_KEY', '')

FROM_ADDRESS = os.environ.get('RESEND_FROM', 'OtoCPA <noreply@otocpa.com>')


def send_welcome_email(to_email: str, firm_name: str, firm_code: str,
                       username: str, password: str) -> bool:
    if not resend.api_key:
        logger.warning('RESEND_API_KEY not set - skipping welcome email to %s', to_email)
        return False

    # Values are user-controlled (via Stripe customer email). Escape before
    # interpolating into HTML so a crafted value can't inject markup.
    fn = html.escape(firm_name or '')
    fc = html.escape(firm_code or '')
    un = html.escape(username or '')
    pw = html.escape(password or '')

    html_body = f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:24px;background:#f5f5f5;">
<div style="background:#0d1520;padding:32px;border-radius:8px 8px 0 0;text-align:center;">
    <h1 style="color:white;margin:0;font-size:32px;">Oto<span style="color:#00d68f">CPA</span></h1>
    <p style="color:#6b8099;margin:8px 0 0;font-size:14px;">Automatisation comptable pour CPAs qu&eacute;b&eacute;cois</p>
</div>
<div style="background:white;padding:32px;border-radius:0 0 8px 8px;">
    <h2 style="color:#0d1520;">Bienvenue, {fn}!</h2>
    <p style="color:#444;">Votre abonnement OtoCPA est activ&eacute; et votre cabinet est pr&ecirc;t.</p>

    <div style="background:#f0f7ff;border:2px solid #00d68f;border-radius:8px;padding:24px;margin:24px 0;">
        <h3 style="color:#0d1520;margin-top:0;">Vos identifiants de connexion</h3>
        <table style="width:100%;border-collapse:collapse;">
            <tr><td style="padding:8px 0;color:#666;font-size:14px;">URL</td><td style="padding:8px 0;font-weight:600;"><a href="https://app.otocpa.com">app.otocpa.com</a></td></tr>
            <tr><td style="padding:8px 0;color:#666;font-size:14px;">Code cabinet</td><td style="padding:8px 0;font-weight:600;font-family:monospace;">{fc}</td></tr>
            <tr><td style="padding:8px 0;color:#666;font-size:14px;">Nom d'utilisateur</td><td style="padding:8px 0;font-weight:600;font-family:monospace;">{un}</td></tr>
            <tr><td style="padding:8px 0;color:#666;font-size:14px;">Mot de passe temporaire</td><td style="padding:8px 0;font-weight:600;font-family:monospace;background:#eee;padding:4px 8px;border-radius:4px;">{pw}</td></tr>
        </table>
        <p style="color:#cc0000;font-size:13px;margin-top:12px;margin-bottom:0;">&#9888;&#65039; Changez votre mot de passe &agrave; la premi&egrave;re connexion.</p>
    </div>

    <h3 style="color:#0d1520;">Prochaines &eacute;tapes</h3>
    <ol style="color:#444;line-height:2;">
        <li>Connectez-vous &agrave; <a href="https://app.otocpa.com/login">app.otocpa.com</a></li>
        <li>Ajoutez vos clients dans le tableau de bord</li>
        <li>Partagez les codes QR avec vos clients</li>
        <li>Connectez QuickBooks Online</li>
        <li>Connectez les comptes bancaires via Plaid</li>
    </ol>

    <div style="text-align:center;margin-top:32px;">
        <a href="https://app.otocpa.com/login"
           style="background:#00d68f;color:#000;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:bold;font-size:16px;display:inline-block;">
            Connexion &rarr;
        </a>
    </div>

    <hr style="margin:32px 0;border:none;border-top:1px solid #eee;">
    <p style="color:#999;font-size:12px;text-align:center;">
        Questions? <a href="mailto:support@otocpa.com">support@otocpa.com</a><br>
        &copy; 2026 OtoCPA Inc. &middot; <a href="https://app.otocpa.com/privacy">Politique de confidentialit&eacute;</a><br>
        Donn&eacute;es h&eacute;berg&eacute;es &agrave; Toronto, Canada &middot; Conforme Loi 25
    </p>
</div>
</body>
</html>"""

    try:
        resend.Emails.send({
            "from": FROM_ADDRESS,
            "to": [to_email],
            "subject": "Bienvenue sur OtoCPA / Welcome to OtoCPA",
            "html": html_body,
        })
        logger.info('welcome email sent to %s', to_email)
        return True
    except Exception as e:
        logger.error('welcome email send failed to %s: %s', to_email, e)
        return False


def send_email(to_email: str, subject: str, html_body: str,
               from_name: str = 'OtoCPA') -> bool:
    if not resend.api_key:
        logger.warning('RESEND_API_KEY not set - skipping email to %s', to_email)
        return False
    try:
        resend.Emails.send({
            "from": f"{from_name} <noreply@otocpa.com>",
            "to": [to_email],
            "subject": subject,
            "html": html_body,
        })
        logger.info('email sent to %s (%s)', to_email, subject)
        return True
    except Exception as e:
        logger.error('email send failed to %s: %s', to_email, e)
        return False
