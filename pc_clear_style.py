#!/usr/bin/env python3
"""
pc_clear_style.py — Clear invalid 'style' attribute values via SP-API PATCH.

Designed to fix Amazon error 100095 ("invalid style value"), most commonly
caused by an invalid value like "dick" being set on 178 ASINs by a prior
catalog manager.

Amazon's SP-API accepts op:'delete' for optional attributes with zero risk of
new errors — the delete simply removes the invalid value, and style is optional
for all product types we use (RAW_MATERIALS, CUTTING_BOARD, etc.).

Usage:
    # Pull SKUs from pc_pull_issues CSV (filters code 100095 automatically)
    python pc_clear_style.py --from-issues pc_issues_20250101_120000.csv

    # Target specific ASINs by hand
    python pc_clear_style.py --asins B0ABC123,B0DEF456

    # Dry run — show what WOULD be sent without calling the API
    python pc_clear_style.py --from-issues pc_issues_20250101_120000.csv --dry-run

    # Also clear from any feed CSV (processes every row)
    python pc_clear_style.py --from-feed pc_filtered_feed_20250101.csv

Environment variables required:
    LWA_CLIENT_ID, LWA_CLIENT_SECRET, LWA_REFRESH_TOKEN, SELLER_ID
    MARKETPLACE_ID  (optional, default ATVPDKIKX0DER)
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime
from urllib.parse import quote
from pathlib import Path

try:
    import requests
except ImportError:
    print('[ERROR] requests not installed. Run: pip install requests')
    sys.exit(1)

SCRIPT_DIR   = Path(__file__).parent
RUN_ID       = datetime.now().strftime('%Y%m%d_%H%M%S')
RESULTS_FILE = SCRIPT_DIR / f'pc_clear_style_results_{RUN_ID}.csv'

LWA_ENDPOINT = 'https://api.amazon.com/auth/o2/token'
SP_API_BASE  = 'https://sellingpartnerapi-na.amazon.com'
MAX_RETRIES  = 3
REQUEST_GAP  = 0.25   # 4 req/sec, well under the 5/sec limit


# ═══════════════════════════════════════════════════════════════════════════════
# Credentials
# ═══════════════════════════════════════════════════════════════════════════════

def load_credentials() -> dict:
    c = {
        'lwa_client_id':     os.environ.get('LWA_CLIENT_ID', ''),
        'lwa_client_secret': os.environ.get('LWA_CLIENT_SECRET', ''),
        'lwa_refresh_token': os.environ.get('LWA_REFRESH_TOKEN', ''),
        'seller_id':         os.environ.get('SELLER_ID', ''),
        'marketplace_id':    os.environ.get('MARKETPLACE_ID', 'ATVPDKIKX0DER'),
    }
    missing = [k for k in ['lwa_client_id', 'lwa_client_secret',
                            'lwa_refresh_token', 'seller_id']
               if not c[k]]
    if missing:
        print(f'[ERROR] Missing env vars: {", ".join(missing)}')
        sys.exit(1)
    return c


# ═══════════════════════════════════════════════════════════════════════════════
# Token manager
# ═══════════════════════════════════════════════════════════════════════════════

class TokenManager:
    def __init__(self, creds: dict):
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


# ═══════════════════════════════════════════════════════════════════════════════
# SP-API helpers
# ═══════════════════════════════════════════════════════════════════════════════

def sp_request(method: str, path: str, tokens: TokenManager,
               params: dict | None = None,
               body: dict | None = None) -> requests.Response:
    url     = f'{SP_API_BASE}{path}'
    payload = json.dumps(body).encode() if body else None
    headers = {
        'x-amz-access-token': tokens.get(),
        'Accept':             'application/json',
    }
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
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** (attempt + 1))
            print(f'    [RETRY] attempt {attempt + 2}/{MAX_RETRIES} for {path} ({e})')
    raise RuntimeError(f'{path}: failed after {MAX_RETRIES} retries')


def get_product_type(tokens: TokenManager, seller_id: str,
                     marketplace_id: str, sku: str) -> str | None:
    """Fetch the productType for a SKU. Returns None if not found."""
    r = sp_request('GET', f'/listings/2021-08-01/items/{seller_id}/{quote(sku, safe="")}',
                   tokens, params={'marketplaceIds': marketplace_id,
                                   'includedData': 'summaries'})
    if r.status_code == 404:
        return None
    if not r.ok:
        return None
    data = r.json()
    for s in (data.get('summaries') or []):
        if s.get('marketplaceId') == marketplace_id and s.get('productType'):
            return s['productType']
    for s in (data.get('summaries') or []):
        if s.get('productType'):
            return s['productType']
    return None


def clear_style_attribute(tokens: TokenManager, seller_id: str,
                           marketplace_id: str, sku: str,
                           product_type: str,
                           replacement: str = 'Standard') -> tuple[int, dict]:
    """Replace invalid style value with a neutral valid value.
    Amazon rejects both op:delete and op:replace with empty array for style,
    so we replace with a neutral term ('Standard' by default)."""
    body = {
        'productType': product_type,
        'patches': [
            {'op': 'replace', 'path': '/attributes/style',
             'value': [{'value': replacement,
                        'language_tag': 'en_US',
                        'marketplace_id': marketplace_id}]},
        ],
    }
    r = sp_request('PATCH',
                   f'/listings/2021-08-01/items/{seller_id}/{quote(sku, safe="")}',
                   tokens,
                   params={'marketplaceIds': marketplace_id},
                   body=body)
    try:
        payload = r.json()
    except ValueError:
        payload = {'raw': r.text[:300]}
    return r.status_code, payload


# ═══════════════════════════════════════════════════════════════════════════════
# Input loaders
# ═══════════════════════════════════════════════════════════════════════════════

def load_from_issues_csv(path: Path, code: str = '100095') -> list[dict]:
    """Return list of {sku, asin} where error code matches."""
    rows = []
    with open(path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            if row.get('code', '').strip() == code:
                sku  = (row.get('sku') or '').strip()
                asin = (row.get('asin') or '').strip().upper()
                if sku:
                    rows.append({'sku': sku, 'asin': asin})
    # Deduplicate by SKU
    seen = set()
    unique = []
    for r in rows:
        if r['sku'] not in seen:
            seen.add(r['sku'])
            unique.append(r)
    return unique


def load_from_feed_csv(path: Path) -> list[dict]:
    """Return all SKUs from a feed CSV."""
    rows = []
    seen = set()
    with open(path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            sku  = (row.get('sku') or '').strip()
            asin = (row.get('asin') or '').strip().upper()
            if sku and sku not in seen:
                seen.add(sku)
                rows.append({'sku': sku, 'asin': asin})
    return rows


def load_from_asin_list(asin_str: str, feed_path: Path | None = None) -> list[dict]:
    """
    Build SKU list from comma-separated ASINs.
    If feed_path provided, looks up the SKU for each ASIN in the feed.
    Otherwise, uses the ASIN as the SKU (falls back to ASIN-based lookup).
    """
    asins = {a.strip().upper() for a in asin_str.split(',') if a.strip()}

    if feed_path and feed_path.exists():
        asin_to_sku = {}
        with open(feed_path, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                a = (row.get('asin') or '').strip().upper()
                s = (row.get('sku') or '').strip()
                if a and s:
                    asin_to_sku[a] = s
        result = []
        for a in asins:
            sku = asin_to_sku.get(a, '')
            if sku:
                result.append({'sku': sku, 'asin': a})
            else:
                print(f'  [WARN] ASIN {a} not found in feed — will try ASIN as SKU')
                result.append({'sku': a, 'asin': a})
        return result

    # No feed — use ASIN directly (SP-API can look up by ASIN for some sellers)
    return [{'sku': a, 'asin': a} for a in asins]


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description='Clear invalid style attribute from listings (fix error 100095).')
    source = ap.add_mutually_exclusive_group(required=True)
    source.add_argument('--from-issues', metavar='FILE',
                        help='pc_pull_issues CSV — auto-filters rows with code 100095')
    source.add_argument('--from-feed', metavar='FILE',
                        help='Feed CSV — clears style from EVERY row (use with care)')
    source.add_argument('--asins', metavar='ASIN1,ASIN2,...',
                        help='Comma-separated list of ASINs to clear')

    ap.add_argument('--feed-for-skus', metavar='FILE',
                    help='Feed CSV used to resolve ASINs to SKUs (with --asins)')
    ap.add_argument('--dry-run', action='store_true',
                    help='Print what WOULD be sent without calling the API')
    ap.add_argument('--limit', type=int, metavar='N',
                    help='Only process the first N SKUs')
    args = ap.parse_args()

    # ── Load SKU list ──────────────────────────────────────────────────────────
    if args.from_issues:
        path = Path(args.from_issues)
        if not path.is_absolute():
            path = SCRIPT_DIR / path
        if not path.exists():
            print(f'[ERROR] Issues file not found: {path}')
            sys.exit(1)
        items = load_from_issues_csv(path)
        source_label = f'issues CSV ({path.name})'

    elif args.from_feed:
        path = Path(args.from_feed)
        if not path.is_absolute():
            path = SCRIPT_DIR / path
        if not path.exists():
            print(f'[ERROR] Feed file not found: {path}')
            sys.exit(1)
        items = load_from_feed_csv(path)
        source_label = f'feed CSV ({path.name})'

    else:  # --asins
        feed_path = None
        if args.feed_for_skus:
            feed_path = Path(args.feed_for_skus)
            if not feed_path.is_absolute():
                feed_path = SCRIPT_DIR / feed_path
        items = load_from_asin_list(args.asins, feed_path)
        source_label = f'--asins argument ({len(items)} ASINs)'

    if args.limit:
        items = items[:args.limit]

    if not items:
        print('[INFO] No items to process. If using --from-issues, '
              'check that the CSV contains rows with code 100095.')
        sys.exit(0)

    # ── Header ────────────────────────────────────────────────────────────────
    print(f'\n{"═" * 60}')
    print('  PC Clear Style Attribute')
    print(f'  Run ID   : {RUN_ID}')
    print(f'  Source   : {source_label}')
    print(f'  Items    : {len(items)}')
    print(f'  Mode     : {"DRY RUN" if args.dry_run else "LIVE"}')
    print(f'{"═" * 60}\n')

    if args.dry_run:
        print('  [DRY RUN] The following SKUs would have style cleared:\n')
        for item in items:
            print(f'    {item["sku"]}  (ASIN: {item["asin"]})')
        print(f'\n  Total: {len(items)} SKUs\n')
        return

    # ── Live run ───────────────────────────────────────────────────────────────
    creds  = load_credentials()
    tokens = TokenManager(creds)
    seller = creds['seller_id']
    mkt    = creds['marketplace_id']

    ok_count      = 0
    skip_count    = 0
    error_count   = 0
    results       = []

    for n, item in enumerate(items, 1):
        sku  = item['sku']
        asin = item['asin']
        print(f'  [{n}/{len(items)}] {sku}', end='  ')

        # Step 1: get product type
        try:
            product_type = get_product_type(tokens, seller, mkt, sku)
        except Exception as e:
            print(f'GET ERROR: {e}')
            results.append({'sku': sku, 'asin': asin, 'status': 'GET_ERROR',
                             'detail': str(e)})
            error_count += 1
            time.sleep(REQUEST_GAP)
            continue

        if not product_type:
            print(f'NOT FOUND (404) — skipped')
            results.append({'sku': sku, 'asin': asin, 'status': 'NOT_FOUND', 'detail': ''})
            skip_count += 1
            time.sleep(REQUEST_GAP)
            continue

        # Step 2: PATCH replace style with neutral value
        try:
            status, payload = clear_style_attribute(tokens, seller, mkt, sku, product_type)
        except Exception as e:
            print(f'PATCH ERROR: {e}')
            results.append({'sku': sku, 'asin': asin, 'status': 'PATCH_ERROR',
                             'detail': str(e)})
            error_count += 1
            time.sleep(REQUEST_GAP)
            continue

        issues = payload.get('issues') or []
        warn_msgs = '; '.join(
            i.get('message', '') for i in issues
            if i.get('severity', '').upper() == 'WARNING'
        )
        err_msgs = '; '.join(
            i.get('message', '') for i in issues
            if i.get('severity', '').upper() == 'ERROR'
        )

        if status in (200, 202):
            print(f'OK  ({product_type})'
                  + (f'  warnings: {warn_msgs}' if warn_msgs else ''))
            results.append({'sku': sku, 'asin': asin, 'status': 'OK',
                             'product_type': product_type, 'detail': warn_msgs})
            ok_count += 1
        else:
            detail = err_msgs or payload.get('raw', str(payload)[:200])
            print(f'FAIL  HTTP {status}  {detail[:100]}')
            results.append({'sku': sku, 'asin': asin, 'status': f'HTTP_{status}',
                             'product_type': product_type, 'detail': detail[:300]})
            error_count += 1

        time.sleep(REQUEST_GAP)

    # ── Write results CSV ──────────────────────────────────────────────────────
    with open(RESULTS_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['sku', 'asin', 'status',
                                               'product_type', 'detail'],
                                extrasaction='ignore')
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f'\n{"═" * 60}')
    print(f'  Cleared OK   : {ok_count}')
    print(f'  Not found    : {skip_count}')
    print(f'  Errors       : {error_count}')
    print(f'  Results file : {RESULTS_FILE.name}')
    print(f'{"═" * 60}')

    if ok_count:
        print(f'\n  {ok_count} listings had style attribute cleared.')
        print('  Amazon will re-validate these listings. Error 100095 should')
        print('  disappear from the next issues scan (may take up to 24 hrs).\n')


if __name__ == '__main__':
    main()
