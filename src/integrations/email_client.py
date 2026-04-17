import base64
import html
import logging
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logger = logging.getLogger(__name__)

GMAIL_TOKEN_FILE = '/opt/otocpa/gmail_token.json'
GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.send']


def _get_gmail_service():
    if not os.path.exists(GMAIL_TOKEN_FILE):
        logger.error('Gmail token not found at %s', GMAIL_TOKEN_FILE)
        return None
    try:
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN_FILE, GMAIL_SCOPES)
        if not creds.valid and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(GMAIL_TOKEN_FILE, 'w') as f:
                f.write(creds.to_json())
        return build('gmail', 'v1', credentials=creds, cache_discovery=False)
    except Exception as e:
        logger.error('Gmail auth failed: %s', e)
        return None


def send_email(to_email: str, subject: str, html_body: str,
               from_name: str = 'OtoCPA') -> bool:
    service = _get_gmail_service()
    if not service:
        return False
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['To'] = to_email
        sender = os.environ.get('GMAIL_SENDER', 'haddadsamer1984@gmail.com')
        msg['From'] = f'{from_name} <{sender}>'
        msg.attach(MIMEText(html_body, 'html'))
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')
        service.users().messages().send(userId='me', body={'raw': raw}).execute()
        logger.info('Gmail API sent to %s (%s)', to_email, subject)
        return True
    except Exception as e:
        logger.error('Gmail API send failed to %s: %s', to_email, e)
        return False


def send_welcome_email(to_email: str, firm_name: str, username: str,
                       set_password_url: str, plan: str) -> bool:
    fn = html.escape(firm_name or '')
    un = html.escape(username or '')
    url = set_password_url or ''
    pl = html.escape(plan or '')

    html_body = f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;max-width:600px;margin:20px auto;">
<h2 style="color:#16C172;">Bienvenue chez OtoCPA, {fn}!</h2>
<p>Votre abonnement au plan <strong>{pl}</strong> est actif.</p>
<p><strong>Nom d'utilisateur:</strong> {un}</p>
<p>Pour configurer votre compte, cr&eacute;ez votre mot de passe:</p>
<p><a href="{url}" style="background:#16C172;color:#000;padding:14px 28px;text-decoration:none;border-radius:8px;font-weight:700;display:inline-block;">Configurer mon compte &rarr;</a></p>
<p style="color:#666;font-size:13px;">Ce lien expire dans 72 heures.</p>
<p style="color:#666;font-size:13px;">Si vous n'avez pas demand&eacute; ceci, ignorez ce courriel.</p>
<hr style="border:none;border-top:1px solid #ddd;">
<h2 style="color:#16C172;">Welcome to OtoCPA, {fn}!</h2>
<p>Your <strong>{pl}</strong> subscription is active.</p>
<p><strong>Username:</strong> {un}</p>
<p>Set your password to get started:</p>
<p><a href="{url}" style="background:#16C172;color:#000;padding:14px 28px;text-decoration:none;border-radius:8px;font-weight:700;display:inline-block;">Set up my account &rarr;</a></p>
<p style="color:#666;font-size:13px;">This link expires in 72 hours.</p>
<p style="color:#666;font-size:13px;">If you did not request this, ignore this email.</p>
</body></html>"""

    return send_email(
        to_email=to_email,
        subject='OtoCPA - Configurez votre compte / Set up your account',
        html_body=html_body,
        from_name='OtoCPA',
    )


def send_password_reset_email(to_email: str, username: str,
                              set_password_url: str) -> bool:
    un = html.escape(username or '')
    url = set_password_url or ''

    html_body = f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;max-width:600px;margin:20px auto;">
<h2 style="color:#16C172;">R&eacute;initialisation de mot de passe OtoCPA</h2>
<p><strong>Nom d'utilisateur:</strong> {un}</p>
<p>Cliquez pour d&eacute;finir un nouveau mot de passe:</p>
<p><a href="{url}" style="background:#16C172;color:#000;padding:14px 28px;text-decoration:none;border-radius:8px;font-weight:700;display:inline-block;">R&eacute;initialiser mon mot de passe &rarr;</a></p>
<p style="color:#666;font-size:13px;">Ce lien expire dans 72 heures.</p>
<p style="color:#666;font-size:13px;">Si vous n'avez pas demand&eacute; ceci, ignorez ce courriel.</p>
<hr style="border:none;border-top:1px solid #ddd;">
<h2 style="color:#16C172;">OtoCPA password reset</h2>
<p><strong>Username:</strong> {un}</p>
<p>Click to set a new password:</p>
<p><a href="{url}" style="background:#16C172;color:#000;padding:14px 28px;text-decoration:none;border-radius:8px;font-weight:700;display:inline-block;">Reset my password &rarr;</a></p>
<p style="color:#666;font-size:13px;">This link expires in 72 hours.</p>
<p style="color:#666;font-size:13px;">If you did not request this, ignore this email.</p>
</body></html>"""

    return send_email(
        to_email=to_email,
        subject='R\u00e9initialisation de mot de passe OtoCPA / OtoCPA password reset',
        html_body=html_body,
        from_name='OtoCPA',
    )
