#!/usr/bin/env python3
"""
pc_sp_api_push_v2.py — Push description + secondary attributes to Amazon via SP-API.

Run AFTER pc_sp_api_push.py (which handles title, bullets, search terms, weight/dims).
This script pushes the remaining approved fields per SKU using
PATCH /listings/2021-08-01/items/{sellerId}/{sku}:

    description          → product_description
                           (includes embedded Q&A block written by the optimizer)
    subject_matter       → subject_keyword
    intended_use         → intended_use
    target_audience      → target_audience_keyword
    material_type        → material_type
    color                → color
    finish_type          → finish_type
    style                → style
    size_description     → size
    item_form            → item_form
    material_composition → material_composition
    recommended_uses     → recommended_uses_for_product
    manufacturer         → manufacturer
    country_of_origin    → country_of_origin
    brand                → brand
    part_number          → part_number
    model_number         → model_number

Nothing else is touched — no price, quantity, title, bullets, shipping, or variations.

Flow per SKU:
    1. GET listing to discover its productType (e.g. CUTTING_BOARD, RAW_MATERIALS, etc.)
    2. PATCH the listing with the approved fields
    3. Log success/error immediately — no batching, no polling

Usage
-----
    python pc_sp_api_push_v2.py                         # push every eligible row
    python pc_sp_api_push_v2.py --dry-run               # build + validate, do not submit
    python pc_sp_api_push_v2.py --limit 10              # cap to first 10 listings
    python pc_sp_api_push_v2.py --input my_file.csv     # use a specific CSV file

Environment variables
---------------------
    LWA_CLIENT_ID
    LWA_CLIENT_SECRET
    LWA_REFRESH_TOKEN
    SELLER_ID
    MARKETPLACE_ID          (default: ATVPDKIKX0DER)
"""

import argparse
import csv
import glob
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print('[ERROR] requests not installed.  Run: pip install requests')
    sys.exit(1)

# ─── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
RUN_ID       = datetime.now().strftime('%Y%m%d_%H%M%S')
RESULTS_FILE = SCRIPT_DIR / f'pc_push_v2_results_{RUN_ID}.csv'

# ─── SP-API constants ─────────────────────────────────────────────────────────
LWA_ENDPOINT = 'https://api.amazon.com/auth/o2/token'
SP_API_BASE  = 'https://sellingpartnerapi-na.amazon.com'
MAX_RETRIES  = 3
REQUEST_GAP  = 0.25   # seconds between API calls (4 req/sec, under 5/sec limit)


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
    required = ['lwa_client_id', 'lwa_client_secret', 'lwa_refresh_token', 'seller_id']
    missing = [k for k in required if not c[k]]
    if missing:
        print(f'\n[ERROR] Missing env vars: {", ".join(missing)}')
        sys.exit(1)
    return c


# ═══════════════════════════════════════════════════════════════════════════════
# LWA access token (auto-refresh)
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
# SP-API calls
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
            print(f'    [RETRY] {path} ({e})')
    raise RuntimeError(f'{path}: failed after retries')


def get_listing_product_type(tokens: TokenManager, seller_id: str,
                              marketplace_id: str, sku: str) -> str | None:
    """Return productType for an existing SKU, or None if not found."""
    r = sp_request('GET', f'/listings/2021-08-01/items/{seller_id}/{sku}',
                   tokens, params={'marketplaceIds': marketplace_id})
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json()
    summaries = data.get('summaries', []) or []
    for s in summaries:
        if s.get('marketplaceId') == marketplace_id and s.get('productType'):
            return s['productType']
    for s in summaries:
        if s.get('productType'):
            return s['productType']
    return None


def patch_listing(tokens: TokenManager, seller_id: str, marketplace_id: str,
                   sku: str, product_type: str, patches: list) -> tuple[int, dict]:
    """PATCH a listing.  Returns (status_code, response_json)."""
    body = {'productType': product_type, 'patches': patches}
    r = sp_request('PATCH', f'/listings/2021-08-01/items/{seller_id}/{sku}',
                   tokens, params={'marketplaceIds': marketplace_id}, body=body)
    try:
        payload = r.json()
    except ValueError:
        payload = {'raw': r.text[:500]}
    return r.status_code, payload


# ═══════════════════════════════════════════════════════════════════════════════
# Build patches
# ═══════════════════════════════════════════════════════════════════════════════

def _txt(v: str, mkt: str) -> list:
    """Text attribute with language tag (title, description, color, etc.)."""
    return [{'value': v, 'language_tag': 'en_US', 'marketplace_id': mkt}]


def _plain(v: str, mkt: str) -> list:
    """Plain value attribute (no language tag)."""
    return [{'value': v, 'marketplace_id': mkt}]


def build_patches(row: dict, mkt: str) -> list:
    p = []
    g = lambda k: (row.get(k, '') or '').strip()

    # ── Product description (includes embedded Q&A block) ─────────────────────
    if g('description'):
        p.append({'op': 'replace', 'path': '/attributes/product_description',
                  'value': _txt(g('description'), mkt)})

    # ── Secondary characteristics ─────────────────────────────────────────────
    if g('subject_matter'):
        p.append({'op': 'replace', 'path': '/attributes/subject_keyword',
                  'value': _txt(g('subject_matter'), mkt)})

    if g('intended_use'):
        p.append({'op': 'replace', 'path': '/attributes/intended_use',
                  'value': _txt(g('intended_use'), mkt)})

    if g('target_audience'):
        p.append({'op': 'replace', 'path': '/attributes/target_audience_keyword',
                  'value': _txt(g('target_audience'), mkt)})

    if g('material_type'):
        p.append({'op': 'replace', 'path': '/attributes/material_type',
                  'value': _txt(g('material_type'), mkt)})

    if g('color'):
        p.append({'op': 'replace', 'path': '/attributes/color',
                  'value': _txt(g('color'), mkt)})

    if g('finish_type'):
        p.append({'op': 'replace', 'path': '/attributes/finish_type',
                  'value': _txt(g('finish_type'), mkt)})

    if g('style'):
        p.append({'op': 'replace', 'path': '/attributes/style',
                  'value': _txt(g('style'), mkt)})

    if g('size_description'):
        p.append({'op': 'replace', 'path': '/attributes/size',
                  'value': _txt(g('size_description'), mkt)})

    if g('item_form'):
        p.append({'op': 'replace', 'path': '/attributes/item_form',
                  'value': _txt(g('item_form'), mkt)})

    if g('material_composition'):
        p.append({'op': 'replace', 'path': '/attributes/material_composition',
                  'value': _txt(g('material_composition'), mkt)})

    if g('recommended_uses'):
        p.append({'op': 'replace', 'path': '/attributes/recommended_uses_for_product',
                  'value': _txt(g('recommended_uses'), mkt)})

    if g('manufacturer'):
        p.append({'op': 'replace', 'path': '/attributes/manufacturer',
                  'value': _plain(g('manufacturer'), mkt)})

    if g('country_of_origin'):
        p.append({'op': 'replace', 'path': '/attributes/country_of_origin',
                  'value': _plain(g('country_of_origin'), mkt)})

    if g('brand'):
        p.append({'op': 'replace', 'path': '/attributes/brand',
                  'value': _plain(g('brand'), mkt)})

    if g('part_number'):
        p.append({'op': 'replace', 'path': '/attributes/part_number',
                  'value': _plain(g('part_number'), mkt)})

    if g('model_number'):
        p.append({'op': 'replace', 'path': '/attributes/model_number',
                  'value': _plain(g('model_number'), mkt)})

    return p


# ═══════════════════════════════════════════════════════════════════════════════
# Input + CLI
# ═══════════════════════════════════════════════════════════════════════════════

def find_csv() -> Path:
    matches = glob.glob(str(SCRIPT_DIR / 'pc_amazon_feed_v4_*.csv'))
    if not matches:
        print(f'[ERROR] No pc_amazon_feed_v4_*.csv found in {SCRIPT_DIR}')
        sys.exit(1)
    return Path(max(matches, key=os.path.getmtime))


def load_rows(path: Path, limit: int = 0) -> list[dict]:
    out = []
    with open(path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            if not (row.get('sku', '') or '').strip():
                continue
            if not (row.get('description', '') or '').strip():
                continue
            out.append(row)
            if limit and len(out) >= limit:
                break
    return out


def parse_args():
    ap = argparse.ArgumentParser(
        description='Push description + secondary attributes via SP-API Listings Items API.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--dry-run', action='store_true',
                    help='Build + validate patches without submitting to Amazon')
    ap.add_argument('--limit', type=int, default=0, metavar='N',
                    help='Cap to first N listings')
    ap.add_argument('--input', metavar='FILE',
                    help='Exact CSV file to use (default: most-recently-modified pc_amazon_feed_v4_*.csv)')
    return ap.parse_args()


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    args  = parse_args()
    creds = load_credentials()

    if args.input:
        csv_path = Path(args.input)
        if not csv_path.is_absolute():
            csv_path = SCRIPT_DIR / csv_path
        if not csv_path.exists():
            print(f'[ERROR] File not found: {csv_path}')
            sys.exit(1)
    else:
        csv_path = find_csv()

    print('\n' + '═' * 60)
    print(f'  PC SP-API Push v2  ({"DRY RUN" if args.dry_run else "LIVE"})')
    print(f'  Run ID : {RUN_ID}')
    print(f'  Input  : {csv_path.name}')
    print(f'  Seller : {creds["seller_id"]}')
    print(f'  Market : {creds["marketplace_id"]}')
    print(f'  Fields : description (incl. Q&A), subject, intended_use,')
    print(f'           target_audience, material_type, color, finish_type,')
    print(f'           style, size, item_form, material_composition,')
    print(f'           recommended_uses, manufacturer, country_of_origin,')
    print(f'           brand, part_number, model_number')
    print(f'  API    : Listings Items 2021-08-01 (per-SKU PATCH)')
    print('═' * 60)

    rows = load_rows(csv_path, args.limit)
    if not rows:
        print('\n  No eligible rows (need non-empty sku + description).')
        return
    print(f'\n  Loaded {len(rows)} listing(s)\n')

    # Results CSV
    fh = open(RESULTS_FILE, 'w', newline='', encoding='utf-8')
    writer = csv.DictWriter(fh, fieldnames=[
        'sku', 'asin', 'product_type', 'patches_sent', 'status',
        'error_code', 'error_message', 'pushed_at',
    ])
    writer.writeheader()
    fh.flush()

    def record(sku, asin, product_type, patches_sent, status, code='', err=''):
        writer.writerow({
            'sku':          sku,
            'asin':         asin,
            'product_type': product_type,
            'patches_sent': patches_sent,
            'status':       status,
            'error_code':   code,
            'error_message': err,
            'pushed_at':    datetime.now().isoformat(timespec='seconds'),
        })
        fh.flush()

    tokens = TokenManager(creds)
    mkt    = creds['marketplace_id']
    seller = creds['seller_id']
    stats  = {'success': 0, 'error': 0, 'skipped': 0, 'not_found': 0}

    try:
        for n, row in enumerate(rows, 1):
            sku  = (row.get('sku',  '') or '').strip()
            asin = (row.get('asin', '') or '').strip()

            print(f'  [{n}/{len(rows)}] {sku}')

            # ── Build patches ─────────────────────────────────────────────────
            patches = build_patches(row, mkt)
            if not patches:
                print(f'    [SKIP] No fields to update')
                record(sku, asin, '', 0, 'skipped', 'NoFields',
                       'No non-empty secondary fields')
                stats['skipped'] += 1
                continue

            patch_names = ', '.join(p['path'].split('/')[-1] for p in patches)

            # ── DRY RUN ───────────────────────────────────────────────────────
            if args.dry_run:
                print(f'    [DRY RUN] Would PATCH {len(patches)} field(s): {patch_names}')
                record(sku, asin, '', len(patches), 'dry_run')
                stats['success'] += 1
                continue

            # ── Step 1: discover productType ──────────────────────────────────
            try:
                product_type = get_listing_product_type(tokens, seller, mkt, sku)
            except Exception as exc:
                print(f'    [ERROR] GET failed: {exc}')
                record(sku, asin, '', 0, 'error', 'GetFailed', str(exc))
                stats['error'] += 1
                time.sleep(REQUEST_GAP)
                continue

            if product_type is None:
                print(f'    [NOT FOUND] SKU does not exist on {mkt}')
                record(sku, asin, '', 0, 'not_found', 'NotFound',
                       'SKU not found on marketplace')
                stats['not_found'] += 1
                time.sleep(REQUEST_GAP)
                continue

            print(f'    productType: {product_type}  →  PATCH {len(patches)} field(s)')
            time.sleep(REQUEST_GAP)

            # ── Step 2: PATCH ─────────────────────────────────────────────────
            try:
                code, resp = patch_listing(tokens, seller, mkt, sku,
                                           product_type, patches)
            except Exception as exc:
                print(f'    [ERROR] PATCH failed: {exc}')
                record(sku, asin, product_type, len(patches), 'error',
                       'PatchFailed', str(exc))
                stats['error'] += 1
                time.sleep(REQUEST_GAP)
                continue

            if 200 <= code < 300:
                submission_id = resp.get('submissionId', '')
                status_resp   = resp.get('status', 'ACCEPTED')
                issues        = resp.get('issues', []) or []
                err_issues    = [i for i in issues if i.get('severity') == 'ERROR']
                warn_issues   = [i for i in issues if i.get('severity') == 'WARNING']

                if err_issues:
                    msg = '; '.join(
                        f"{i.get('code','')}:{i.get('message','')}"
                        for i in err_issues)
                    print(f'    [ERROR] {msg}')
                    record(sku, asin, product_type, len(patches), 'error',
                           'Rejected', msg)
                    stats['error'] += 1
                else:
                    warn_note = ''
                    if warn_issues:
                        warn_note = '  WARNINGS: ' + '; '.join(
                            f"{i.get('code','')}:{i.get('message','')}"
                            for i in warn_issues)
                    print(f'    [OK] status={status_resp} '
                          f'submissionId={submission_id}{warn_note}')
                    record(sku, asin, product_type, len(patches), 'success',
                           '', warn_note.strip())
                    stats['success'] += 1
            else:
                err_obj = resp.get('errors', [{}])
                err_msg = '; '.join(
                    f"{e.get('code','')}:{e.get('message','')}"
                    for e in (err_obj if isinstance(err_obj, list) else [err_obj]))
                print(f'    [ERROR] HTTP {code}: {err_msg}')
                record(sku, asin, product_type, len(patches), 'error',
                       f'HTTP{code}', err_msg)
                stats['error'] += 1

            time.sleep(REQUEST_GAP)

    finally:
        fh.close()

    print('\n' + '═' * 60)
    print('  DONE')
    print(f'  Success   : {stats["success"]}')
    print(f'  Errors    : {stats["error"]}')
    print(f'  Not found : {stats["not_found"]}')
    print(f'  Skipped   : {stats["skipped"]}')
    print(f'\n  Results → {RESULTS_FILE.name}')
    print('═' * 60 + '\n')


if __name__ == '__main__':
    main()
