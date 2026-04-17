#!/usr/bin/env python3
"""
pc_sp_api_push.py — Push approved listing fields to Amazon via SP-API Listings Items API.

Reads the most recent pc_amazon_feed_v4_*.csv in the script folder and submits
ONLY these attributes per SKU using PATCH /listings/2021-08-01/items/{sellerId}/{sku}:

    new_title              → item_name
    bullet1..bullet5       → bullet_point (list, empties skipped)
    backend_search_terms   → generic_keyword
    item_weight_lbs        → item_weight
    chargeable_weight_lbs  → item_package_weight
    box_length_in/_width_/_height_ → item_package_dimensions
    item_type_keyword      → item_type_keyword

Nothing else is touched — no description, price, quantity, shipping
template, or variations.  Shipping templates must be managed through
Seller Central or an inventory loader feed.

Flow per SKU:
    1. GET listing to discover its productType (e.g. CUTTING_BOARD, PRODUCT, etc.)
    2. PATCH the listing with the approved fields
    3. Log success/error immediately — no batching, no polling

Usage
-----
    python pc_sp_api_push.py                # push every eligible row
    python pc_sp_api_push.py --dry-run      # build + validate, do not submit
    python pc_sp_api_push.py --limit 10     # cap to first 10 listings

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
RESULTS_FILE = SCRIPT_DIR / f'pc_push_results_{RUN_ID}.csv'

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
    # Fall back: any productType
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
# Build patches (only the requested fields)
# ═══════════════════════════════════════════════════════════════════════════════

def _txt(v: str, mkt: str) -> list:
    return [{'value': v, 'language_tag': 'en_US', 'marketplace_id': mkt}]


def _plain(v, mkt: str) -> list:
    return [{'value': v, 'marketplace_id': mkt}]


def _wt(v: str, mkt: str):
    try:
        n = float(v)
    except (ValueError, TypeError):
        return None
    if n <= 0:
        return None
    return [{'value': n, 'unit': 'pounds', 'marketplace_id': mkt}]


def _dims(l: str, w: str, h: str, mkt: str):
    try:
        lv, wv, hv = float(l), float(w), float(h)
    except (ValueError, TypeError):
        return None
    if lv <= 0 or wv <= 0 or hv <= 0:
        return None
    return [{'length': {'value': lv, 'unit': 'inches'},
             'width':  {'value': wv, 'unit': 'inches'},
             'height': {'value': hv, 'unit': 'inches'},
             'marketplace_id': mkt}]


def build_patches(row: dict, mkt: str) -> list:
    p = []
    g = lambda k: (row.get(k, '') or '').strip()

    if g('new_title'):
        p.append({'op': 'replace', 'path': '/attributes/item_name',
                  'value': _txt(g('new_title'), mkt)})

    bullets = []
    for i in range(1, 6):
        b = g(f'bullet{i}')
        if b:
            bullets.append({'value': b, 'language_tag': 'en_US', 'marketplace_id': mkt})
    if bullets:
        p.append({'op': 'replace', 'path': '/attributes/bullet_point', 'value': bullets})

    if g('backend_search_terms'):
        p.append({'op': 'replace', 'path': '/attributes/generic_keyword',
                  'value': _txt(g('backend_search_terms'), mkt)})

    iw = _wt(g('item_weight_lbs'), mkt)
    if iw:
        p.append({'op': 'replace', 'path': '/attributes/item_weight', 'value': iw})

    sw = _wt(g('chargeable_weight_lbs'), mkt)
    if sw:
        p.append({'op': 'replace', 'path': '/attributes/item_package_weight', 'value': sw})

    dm = _dims(g('box_length_in'), g('box_width_in'), g('box_height_in'), mkt)
    if dm:
        p.append({'op': 'replace', 'path': '/attributes/item_package_dimensions', 'value': dm})

    if g('item_type_keyword'):
        p.append({'op': 'replace', 'path': '/attributes/item_type_keyword',
                  'value': _txt(g('item_type_keyword'), mkt)})

    return p


# ═══════════════════════════════════════════════════════════════════════════════
# Input + CLI
# ═══════════════════════════════════════════════════════════════════════════════

def find_csv() -> Path:
    matches = sorted(glob.glob(str(SCRIPT_DIR / 'pc_amazon_feed_v4_*.csv')))
    if not matches:
        print(f'[ERROR] No pc_amazon_feed_v4_*.csv found in {SCRIPT_DIR}')
        sys.exit(1)
    return Path(matches[-1])


def load_rows(path: Path, limit: int = 0) -> list[dict]:
    out = []
    with open(path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            if not (row.get('sku', '') or '').strip():
                continue
            if not (row.get('new_title', '') or '').strip():
                continue
            out.append(row)
            if limit and len(out) >= limit:
                break
    return out


def parse_args():
    ap = argparse.ArgumentParser(
        description='Push approved listing fields via SP-API Listings Items API.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--dry-run', action='store_true',
                    help='Build + validate patches without submitting to Amazon')
    ap.add_argument('--limit', type=int, default=0, metavar='N',
                    help='Cap to first N listings')
    return ap.parse_args()


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    args     = parse_args()
    creds    = load_credentials()
    csv_path = find_csv()

    print('\n' + '═' * 60)
    print(f'  PC SP-API Push  ({"DRY RUN" if args.dry_run else "LIVE"})')
    print(f'  Run ID : {RUN_ID}')
    print(f'  Input  : {csv_path.name}')
    print(f'  Seller : {creds["seller_id"]}')
    print(f'  Market : {creds["marketplace_id"]}')
    print(f'  API    : Listings Items 2021-08-01 (per-SKU PATCH)')
    print('═' * 60)

    rows = load_rows(csv_path, args.limit)
    if not rows:
        print('\n  No eligible rows (need non-empty sku + new_title).')
        return
    print(f'\n  Loaded {len(rows)} listing(s)\n')

    # Results CSV
    fh = open(RESULTS_FILE, 'w', newline='', encoding='utf-8')
    writer = csv.DictWriter(fh, fieldnames=[
        'sku', 'asin', 'title', 'product_type', 'status',
        'error_code', 'error_message', 'pushed_at',
    ])
    writer.writeheader()
    fh.flush()

    def record(sku, asin, title, product_type, status, code='', err=''):
        writer.writerow({
            'sku':           sku,
            'asin':          asin,
            'title':         title[:120],
            'product_type':  product_type,
            'status':        status,
            'error_code':    code,
            'error_message': err,
            'pushed_at':     datetime.now().isoformat(timespec='seconds'),
        })
        fh.flush()

    tokens = TokenManager(creds)
    mkt    = creds['marketplace_id']
    seller = creds['seller_id']
    stats  = {'success': 0, 'error': 0, 'skipped': 0, 'not_found': 0}

    try:
        for n, row in enumerate(rows, 1):
            sku   = (row.get('sku', '') or '').strip()
            asin  = (row.get('asin', '') or '').strip()
            title = (row.get('new_title', '') or '').strip()

            print(f'  [{n}/{len(rows)}] {sku}')

            # ── Build patches ─────────────────────────────────────────────────
            patches = build_patches(row, mkt)
            if not patches:
                print(f'    [SKIP] No fields to update')
                record(sku, asin, title, '', 'skipped', 'NoFields',
                       'No non-empty approved fields')
                stats['skipped'] += 1
                continue

            # ── DRY RUN ───────────────────────────────────────────────────────
            if args.dry_run:
                print(f'    [DRY RUN] Would PATCH {len(patches)} field(s): '
                      f'{", ".join(p["path"].split("/")[-1] for p in patches)}')
                record(sku, asin, title, '', 'dry_run')
                stats['success'] += 1
                continue

            # ── Step 1: discover productType ──────────────────────────────────
            try:
                product_type = get_listing_product_type(tokens, seller, mkt, sku)
            except Exception as exc:
                print(f'    [ERROR] GET failed: {exc}')
                record(sku, asin, title, '', 'error', 'GetFailed', str(exc))
                stats['error'] += 1
                time.sleep(REQUEST_GAP)
                continue

            if product_type is None:
                print(f'    [NOT FOUND] SKU does not exist on {mkt}')
                record(sku, asin, title, '', 'not_found', 'NotFound',
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
                record(sku, asin, title, product_type, 'error',
                       'PatchFailed', str(exc))
                stats['error'] += 1
                time.sleep(REQUEST_GAP)
                continue

            if 200 <= code < 300:
                submission_id = resp.get('submissionId', '')
                status_resp   = resp.get('status', 'ACCEPTED')
                issues        = resp.get('issues', []) or []
                err_issues    = [i for i in issues
                                  if i.get('severity') in ('ERROR',)]
                warn_issues   = [i for i in issues
                                  if i.get('severity') == 'WARNING']

                if err_issues:
                    msg = '; '.join(
                        f"{i.get('code','')}:{i.get('message','')}"
                        for i in err_issues)
                    print(f'    [ERROR] {msg}')
                    record(sku, asin, title, product_type, 'error',
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
                    record(sku, asin, title, product_type, 'success',
                           '', warn_note.strip())
                    stats['success'] += 1
            else:
                # HTTP error
                err_obj = resp.get('errors', [{}])
                err_msg = '; '.join(
                    f"{e.get('code','')}:{e.get('message','')}"
                    for e in (err_obj if isinstance(err_obj, list) else [err_obj]))
                print(f'    [ERROR] HTTP {code}: {err_msg}')
                record(sku, asin, title, product_type, 'error',
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
