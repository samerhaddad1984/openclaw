import os
import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import plaid
from plaid.api import plaid_api
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.products import Products
from plaid.model.country_code import CountryCode
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.transactions_get_request import TransactionsGetRequest


def get_plaid_client():
    environment = os.environ.get('PLAID_ENVIRONMENT', 'sandbox')

    if environment == 'sandbox':
        host = plaid.Environment.Sandbox
    elif environment == 'development':
        host = getattr(plaid.Environment, 'Development', plaid.Environment.Sandbox)
    else:
        host = plaid.Environment.Production

    configuration = plaid.Configuration(
        host=host,
        api_key={
            'clientId': os.environ.get('PLAID_CLIENT_ID', ''),
            'secret': os.environ.get('PLAID_SECRET', ''),
        },
    )

    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


def create_link_token(client_code: str, user_id: str) -> str:
    client = get_plaid_client()
    request = LinkTokenCreateRequest(
        products=[Products('transactions')],
        client_name='OtoCPA',
        country_codes=[CountryCode('CA')],
        language='fr',
        user=LinkTokenCreateRequestUser(client_user_id=str(user_id)),
    )
    response = client.link_token_create(request)
    return response['link_token']


def exchange_public_token(public_token: str):
    client = get_plaid_client()
    request = ItemPublicTokenExchangeRequest(public_token=public_token)
    response = client.item_public_token_exchange(request)
    return response['access_token'], response['item_id']


def get_transactions(access_token: str, start_date, end_date):
    if isinstance(start_date, str):
        start_date = datetime.date.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = datetime.date.fromisoformat(end_date)
    client = get_plaid_client()
    request = TransactionsGetRequest(
        access_token=access_token,
        start_date=start_date,
        end_date=end_date,
    )
    response = client.transactions_get(request)
    return response['transactions']
