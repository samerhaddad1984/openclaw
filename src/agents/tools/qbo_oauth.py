import os, json, time, base64, urllib.request, urllib.parse, secrets, logging
from pathlib import Path
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CONFIG_PATH = Path('data/qbo_config.json')

def load_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}

def save_config(config):
    CONFIG_PATH.parent.mkdir(exist_ok=True)
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f, indent=2)

def get_authorize_url():
    client_id = os.environ.get('QBO_CLIENT_ID', '')
    redirect_uri = os.environ.get('QBO_REDIRECT_URI', 'https://app.otocpa.com/qbo/callback')
    state = secrets.token_urlsafe(16)

    params = {
        'client_id': client_id,
        'response_type': 'code',
        'scope': 'com.intuit.quickbooks.accounting',
        'redirect_uri': redirect_uri,
        'state': state,
    }

    url = 'https://appcenter.intuit.com/connect/oauth2?' + urllib.parse.urlencode(params)
    return url, state

def exchange_code_for_token(code, realm_id):
    client_id = os.environ.get('QBO_CLIENT_ID', '')
    client_secret = os.environ.get('QBO_CLIENT_SECRET', '')
    redirect_uri = os.environ.get('QBO_REDIRECT_URI', 'https://app.otocpa.com/qbo/callback')
    environment = os.environ.get('QBO_ENVIRONMENT', 'sandbox')

    credentials = base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()

    data = urllib.parse.urlencode({
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': redirect_uri,
    }).encode()

    req = urllib.request.Request(
        'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer',
        data=data,
        headers={
            'Authorization': f'Basic {credentials}',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
        }
    )

    with urllib.request.urlopen(req) as resp:
        token_data = json.loads(resp.read())

    config = {
        'access_token': token_data['access_token'],
        'refresh_token': token_data.get('refresh_token', ''),
        'realm_id': realm_id,
        'environment': environment,
        'expires_at': time.time() + token_data.get('expires_in', 3600),
        'minor_version': '75',
    }

    save_config(config)
    logging.info(f'QBO token obtained for realm {realm_id}')
    return config

def refresh_token():
    config = load_config()
    if not config.get('refresh_token'):
        raise ValueError('No refresh token available')

    client_id = os.environ.get('QBO_CLIENT_ID', '')
    client_secret = os.environ.get('QBO_CLIENT_SECRET', '')
    credentials = base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()

    data = urllib.parse.urlencode({
        'grant_type': 'refresh_token',
        'refresh_token': config['refresh_token'],
    }).encode()

    req = urllib.request.Request(
        'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer',
        data=data,
        headers={
            'Authorization': f'Basic {credentials}',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
        }
    )

    with urllib.request.urlopen(req) as resp:
        token_data = json.loads(resp.read())

    config['access_token'] = token_data['access_token']
    config['expires_at'] = time.time() + token_data.get('expires_in', 3600)
    if 'refresh_token' in token_data:
        config['refresh_token'] = token_data['refresh_token']

    save_config(config)
    logging.info('QBO token refreshed')
    return config

def get_valid_token():
    config = load_config()
    if not config.get('access_token'):
        raise ValueError('QBO not connected. Please authorize first.')

    if config.get('expires_at', 0) - time.time() < 300:
        config = refresh_token()

    return config['access_token'], config.get('realm_id'), config.get('environment', 'sandbox')

def is_connected():
    config = load_config()
    return bool(config.get('access_token') and config.get('realm_id'))
