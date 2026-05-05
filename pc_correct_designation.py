#!/usr/bin/env python3
"""
pc_correct_designation.py — Fix Cast/Extruded designation on specific SKUs.

Fetches the current live title from Amazon, swaps the designation,
and pushes the corrected title.

Usage:
    python pc_correct_designation.py --skus AC796_L60,AC796_L72V1 --designation extruded
    python pc_correct_designation.py --skus AC553_L24Q2 --designation cast
"""

import argparse
import json
import os
import re
import sys
import time
from urllib.parse import quote

try:
    import requests
except ImportError:
    print('[ERROR] requests not installed. Run: pip install requests')
    sys.exit(1)

LWA_ENDPOINT = 'https://api.amazon.com/auth/o2/token'
SP_API_BASE  = 'https://sellingpartnerapi-na.amazon.com'
MAX_RETRIES  = 3
REQUEST_GAP  = 0.5


def load_credentials() -> dict:
    c = {
        'lwa_client_id':     os.environ.get('LWA_CLIENT_ID', ''),
        'lwa_client_secret': os.environ.get('LWA_CLIENT_SECRET', ''),
        'lwa_refresh_token': os.environ.get('LWA_REFRESH_TOKEN', ''),
        'seller_id':         os.environ.get('SELLER_ID', ''),
        'marketplace_id':    os.environ.get('MARKETPLACE_ID', 'ATVPDKIKX0DER'),
    }
    missing = [k for k in ['lwa_client_id', 'lwa_client_secret',
                            'lwa_refresh_token', 'seller_id'] if not c[k]]
    if missing:
        print(f'[ERROR] Missing env vars: {", ".join(missing)}')
        sys.exit(1)
    return c


class TokenManager:
    def __init__(self, creds):
        self._c, self._tok, self._exp = creds, '', 0.0

    def get(self) -> str:
        if self._tok and time.time() < self._exp - 60:
            return self._tok
        r = requests.post(LWA_ENDPOINT, data={
            'grant_type':    'refresh_token',
            'refresh_token': self._c['lwa_refresh_token'],
            'client_id':     self._c['lwa_client_id'],
            'client_secret': self._c['lwa_client_secret'],
        }, timeout=30)
        if not r.ok:
            raise RuntimeError(f'LWA refresh failed ({r.status_code}): {r.text[:200]}')
        d = r.json()
        self._tok = d['access_token']
        self._exp = time.time() + d.get('expires_in', 3600)
        return self._tok


def sp_request(method, path, tokens, params=None, body=None):
    url     = f'{SP_API_BASE}{path}'
    payload = json.dumps(body).encode() if body else None
    headers = {'x-amz-access-token': tokens.get(), 'Accept': 'application/json'}
    if body is not None:
        headers['Content-Type'] = 'application/json'
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.request(method, url, headers=headers,
                                 params=params, data=payload, timeout=60)
            if r.status_code == 429:
                wait = 5 * (2 ** attempt)
                print(f'    [THROTTLE] waiting {wait}s')
                time.sleep(wait)
                headers['x-amz-access-token'] = tokens.get()
                continue
            return r
        except requests.RequestException:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** (attempt + 1))
    raise RuntimeError(f'Failed after {MAX_RETRIES} retries')


def fetch_listing(tokens, seller, mkt, sku) -> dict:
    """Return full attributes dict from live Amazon listing."""
    r = sp_request('GET', f'/listings/2021-08-01/items/{seller}/{quote(sku, safe="")}',
                   tokens, params={'marketplaceIds': mkt,
                                   'includedData': 'attributes,summaries'})
    if not r.ok:
        return {}
    return r.json()


def swap_designation(title: str, new_designation: str) -> str:
    """Replace Cast/Extruded in title with new_designation. Returns unchanged title if not found."""
    opposite = 'Extruded' if new_designation.lower() == 'cast' else 'Cast'
    pattern  = re.compile(r'\b' + re.escape(opposite) + r'\b', re.IGNORECASE)
    if pattern.search(title):
        return pattern.sub(new_designation, title)
    # Designation not present at all — inject before material name
    for mat in ('acrylic', 'nylon', 'polycarbonate', 'polyethylene', 'abs'):
        m = re.search(r'\b' + mat + r'\b', title, re.IGNORECASE)
        if m:
            return title[:m.start()] + new_designation + ' ' + title[m.start():]
    return title


def main():
    ap = argparse.ArgumentParser(
        description='Correct Cast/Extruded designation on specific SKUs.')
    ap.add_argument('--skus', required=True,
                    help='Comma-separated SKUs to correct')
    ap.add_argument('--designation', required=True, choices=['cast', 'extruded'],
                    help='Correct designation to apply')
    ap.add_argument('--dry-run', action='store_true',
                    help='Show what would change without pushing')
    args = ap.parse_args()

    skus        = [s.strip() for s in args.skus.split(',') if s.strip()]
    designation = args.designation.capitalize()

    creds  = load_credentials()
    tokens = TokenManager(creds)
    seller = creds['seller_id']
    mkt    = creds['marketplace_id']

    print(f'\n  Correcting {len(skus)} SKU(s) → {designation}\n')

    for sku in skus:
        print(f'  {sku}', end='  ')

        data = fetch_listing(tokens, seller, mkt, sku)
        if not data:
            print('NOT FOUND — skipped')
            continue

        # Get product type
        product_type = None
        for s in (data.get('summaries') or []):
            if s.get('productType'):
                product_type = s['productType']
                break

        if not product_type:
            print('NO PRODUCT TYPE — skipped')
            continue

        # Get current title
        attrs = data.get('attributes', {})
        current_title = ''
        for item in (attrs.get('item_name') or []):
            v = (item.get('value') or '').strip()
            if v:
                current_title = v
                break

        if not current_title:
            print('NO TITLE — skipped')
            continue

        corrected_title = swap_designation(current_title, designation)

        if corrected_title == current_title:
            print(f'NO CHANGE — title already correct or no designation found')
            print(f'    Current: {current_title[:80]}')
            continue

        print(f'{"DRY RUN — " if args.dry_run else ""}"{current_title[:60]}"')
        print(f'    → "{corrected_title[:60]}"')

        if args.dry_run:
            continue

        patch = {
            'productType': product_type,
            'patches': [{
                'op':    'replace',
                'path':  '/attributes/item_name',
                'value': [{'value':        corrected_title,
                           'language_tag': 'en_US',
                           'marketplace_id': mkt}],
            }],
        }
        r = sp_request('PATCH',
                       f'/listings/2021-08-01/items/{seller}/{quote(sku, safe="")}',
                       tokens, params={'marketplaceIds': mkt}, body=patch)
        try:
            payload = r.json()
        except ValueError:
            payload = {'raw': r.text[:200]}

        if r.status_code in (200, 202):
            print(f'    OK')
        else:
            errs = '; '.join(i.get('message', '') for i in (payload.get('issues') or [])
                             if i.get('severity', '').upper() == 'ERROR')
            print(f'    FAIL HTTP {r.status_code} — {errs or str(payload)[:150]}')

        time.sleep(REQUEST_GAP)

    print()


if __name__ == '__main__':
    main()
