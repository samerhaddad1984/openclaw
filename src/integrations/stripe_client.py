import os
import stripe
import logging

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logger = logging.getLogger(__name__)

stripe.api_key = os.environ.get('STRIPE_SECRET_KEY', '')

PLANS = {
    'starter_monthly': {
        'name': 'Démarrage',
        'price_id': os.environ.get('STRIPE_PRICE_STARTER_MONTHLY', ''),
        'amount': 99,
        'interval': 'month',
        'currency': 'cad',
    },
    'starter_yearly': {
        'name': 'Démarrage — Annuel',
        'price_id': os.environ.get('STRIPE_PRICE_STARTER_YEARLY', ''),
        'amount': 990,
        'interval': 'year',
        'currency': 'cad',
    },
    'pro_monthly': {
        'name': 'Professionnel',
        'price_id': os.environ.get('STRIPE_PRICE_PRO_MONTHLY', ''),
        'amount': 299,
        'interval': 'month',
        'currency': 'cad',
    },
    'pro_yearly': {
        'name': 'Professionnel — Annuel',
        'price_id': os.environ.get('STRIPE_PRICE_PRO_YEARLY', ''),
        'amount': 2990,
        'interval': 'year',
        'currency': 'cad',
    },
    'business_monthly': {
        'name': 'Cabinet',
        'price_id': os.environ.get('STRIPE_PRICE_BUSINESS_MONTHLY', ''),
        'amount': 599,
        'interval': 'month',
        'currency': 'cad',
    },
    'business_yearly': {
        'name': 'Cabinet — Annuel',
        'price_id': os.environ.get('STRIPE_PRICE_BUSINESS_YEARLY', ''),
        'amount': 5990,
        'interval': 'year',
        'currency': 'cad',
    },
}


def create_checkout_session(plan_key, firm_code, firm_email, success_url, cancel_url):
    plan = PLANS.get(plan_key)
    if not plan:
        raise ValueError(f'Unknown plan: {plan_key}')

    session = stripe.checkout.Session.create(
        payment_method_types=['card'],
        line_items=[{
            'price': plan['price_id'],
            'quantity': 1,
        }],
        mode='subscription',
        success_url=success_url,
        cancel_url=cancel_url,
        customer_email=firm_email,
        metadata={
            'firm_code': firm_code or '',
            'plan': plan_key,
        },
    )
    return session


def create_customer_portal_session(stripe_customer_id, return_url):
    session = stripe.billing_portal.Session.create(
        customer=stripe_customer_id,
        return_url=return_url,
    )
    return session


def get_subscription_status(stripe_customer_id):
    subscriptions = stripe.Subscription.list(
        customer=stripe_customer_id,
        status='active',
        limit=1,
    )
    if subscriptions.data:
        sub = subscriptions.data[0]
        return {
            'active': True,
            'plan': sub.metadata.get('plan', 'unknown'),
            'current_period_end': sub.current_period_end,
            'cancel_at_period_end': sub.cancel_at_period_end,
        }
    return {'active': False}


def handle_webhook(payload, sig_header):
    webhook_secret = os.environ.get('STRIPE_WEBHOOK_SECRET', '')
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, webhook_secret)
        return event
    except Exception as e:
        raise ValueError(f'Webhook error: {e}')
