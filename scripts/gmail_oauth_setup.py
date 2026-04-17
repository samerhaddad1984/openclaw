#!/usr/bin/env python3
"""One-time OAuth authorization for Gmail API using modern loopback flow."""

import os, json, sys, urllib.parse
from google_auth_oauthlib.flow import Flow

SCOPES = ['https://www.googleapis.com/auth/gmail.send']
CREDS_FILE = '/opt/otocpa/gmail_oauth_credentials.json'
TOKEN_FILE = '/opt/otocpa/gmail_token.json'

# Use a fake localhost redirect - user will extract code from redirected URL
REDIRECT_URI = 'http://localhost:8080'

def authorize():
    flow = Flow.from_client_secrets_file(CREDS_FILE, SCOPES, redirect_uri=REDIRECT_URI)
    auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline', include_granted_scopes='true')

    print('\n' + '='*70)
    print('STEP 1: Open this URL in your browser:')
    print('='*70)
    print(auth_url)
    print('='*70)
    print('\nSTEP 2: Log in with your Gmail and approve')
    print('STEP 3: You will be redirected to localhost:8080 which will FAIL to load')
    print('        THAT IS EXPECTED. Look at your browser URL bar.')
    print('STEP 4: Copy the ENTIRE URL from your browser (starts with http://localhost:8080/?code=...)')
    print('='*70 + '\n')

    full_url = input('Paste the entire redirected URL here: ').strip()

    # Extract code from URL
    parsed = urllib.parse.urlparse(full_url)
    params = urllib.parse.parse_qs(parsed.query)

    if 'code' not in params:
        print('ERROR: Could not find code parameter in URL')
        print('URL should look like: http://localhost:8080/?code=XXXXX&scope=...')
        sys.exit(1)

    code = params['code'][0]
    flow.fetch_token(code=code)

    creds = flow.credentials
    with open(TOKEN_FILE, 'w') as f:
        f.write(creds.to_json())

    print(f'\n✓ Saved token to {TOKEN_FILE}')
    print(f'✓ Refresh token saved - will auto-renew indefinitely')

if __name__ == '__main__':
    authorize()
