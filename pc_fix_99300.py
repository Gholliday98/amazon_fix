#!/usr/bin/env python3
"""
pc_fix_99300.py — Fix Amazon error 99300 (prohibited content) on affected listings only.

Reads the issues CSV from pc_pull_issues.py, extracts every ASIN with code 99300,
finds their content in the feed CSVs, runs the policy validator to strip all
prohibited language, and pushes cleaned content via SP-API.

ONLY touches listings that have a 99300 error. Does not affect any other listings.

Usage:
    # Dry run — shows what would be cleaned, no API calls
    python pc_fix_99300.py --issues pc_issues_20260504_112649.csv --dry-run

    # Live run
    python pc_fix_99300.py --issues pc_issues_20260504_112649.csv

    # Test with first 10 listings only
    python pc_fix_99300.py --issues pc_issues_20260504_112649.csv --limit 10

Environment variables required:
    LWA_CLIENT_ID, LWA_CLIENT_SECRET, LWA_REFRESH_TOKEN, SELLER_ID
    MARKETPLACE_ID  (optional, default ATVPDKIKX0DER)
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

try:
    import requests
except ImportError:
    print('[ERROR] requests not installed. Run: pip install requests')
    sys.exit(1)

try:
    from pc_policy_validator import validate_and_fix, check_backend_terms
    _VALIDATOR = True
except ImportError:
    print('[ERROR] pc_policy_validator.py not found.')
    sys.exit(1)

try:
    from pc_preflight import preflight_check
    _PREFLIGHT = True
except ImportError:
    _PREFLIGHT = False

SCRIPT_DIR   = Path(__file__).parent
RUN_ID       = datetime.now().strftime('%Y%m%d_%H%M%S')
RESULTS_FILE = SCRIPT_DIR / f'pc_fix_99300_results_{RUN_ID}.csv'

LWA_ENDPOINT = 'https://api.amazon.com/auth/o2/token'
SP_API_BASE  = 'https://sellingpartnerapi-na.amazon.com'
MAX_RETRIES  = 3
REQUEST_GAP  = 0.25

BULLET_FIELDS = [f'bullet{i}' for i in range(1, 6)]


# ═══════════════════════════════════════════════════════════════════════════════
# Credentials + token
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


# ═══════════════════════════════════════════════════════════════════════════════
# SP-API
# ═══════════════════════════════════════════════════════════════════════════════

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
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** (attempt + 1))
            print(f'    [RETRY] {attempt + 2}/{MAX_RETRIES}')
    raise RuntimeError(f'Failed after {MAX_RETRIES} retries')


def get_product_type(tokens, seller_id, marketplace_id, sku) -> str | None:
    r = sp_request('GET', f'/listings/2021-08-01/items/{seller_id}/{quote(sku, safe="")}',
                   tokens, params={'marketplaceIds': marketplace_id,
                                   'includedData': 'summaries'})
    if not r.ok:
        return None
    for s in (r.json().get('summaries') or []):
        if s.get('productType'):
            return s['productType']
    return None


def patch_listing(tokens, seller_id, marketplace_id, sku, product_type, patches):
    body = {'productType': product_type, 'patches': patches}
    r = sp_request('PATCH', f'/listings/2021-08-01/items/{seller_id}/{quote(sku, safe="")}',
                   tokens, params={'marketplaceIds': marketplace_id}, body=body)
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, {'raw': r.text[:300]}


def fetch_current_title(tokens, seller_id, marketplace_id, sku) -> str | None:
    """Fetch the live item_name from Amazon for a given SKU."""
    r = sp_request('GET', f'/listings/2021-08-01/items/{seller_id}/{quote(sku, safe="")}',
                   tokens, params={'marketplaceIds': marketplace_id,
                                   'includedData': 'attributes'})
    if not r.ok:
        return None
    for item in (r.json().get('attributes', {}).get('item_name') or []):
        v = (item.get('value') or '').strip()
        if v:
            return v
    return None


CAST_EXTRUDED_MATS = ('acrylic', 'nylon')

def extract_cast_extruded(title: str) -> str | None:
    """Return 'Cast' or 'Extruded' if present in title, else None."""
    tl = title.lower()
    if 'cast' in tl:
        return 'Cast'
    if 'extruded' in tl:
        return 'Extruded'
    return None


def inject_cast_extruded(title: str, designation: str) -> str:
    """
    Insert designation before the first occurrence of 'acrylic' or 'nylon'.
    e.g. 'Clear Acrylic Sheet' + 'Cast' → 'Clear Cast Acrylic Sheet'
    If already present, returns title unchanged.
    """
    tl = title.lower()
    if 'cast' in tl or 'extruded' in tl:
        return title
    for mat in CAST_EXTRUDED_MATS:
        m = re.search(r'\b' + mat + r'\b', title, re.IGNORECASE)
        if m:
            return title[:m.start()] + designation + ' ' + title[m.start():]
    return title


# ═══════════════════════════════════════════════════════════════════════════════
# Patch builders
# ═══════════════════════════════════════════════════════════════════════════════

def _txt(v, mkt):
    return [{'value': v, 'language_tag': 'en_US', 'marketplace_id': mkt}]


def clean_row(row: dict) -> dict:
    """Apply policy validator to all text fields. Returns a cleaned copy of the row."""
    cleaned = dict(row)
    for field in ['new_title'] + BULLET_FIELDS + ['description']:
        val = (cleaned.get(field) or '').strip()
        if val:
            cleaned[field], _ = validate_and_fix(val, field)
    bst = (cleaned.get('backend_search_terms') or '').strip()
    if bst:
        cleaned['backend_search_terms'], _ = check_backend_terms(bst)
    return cleaned


def build_content_patches(row: dict, mkt: str) -> list:
    """Build patches for all text content fields from an already-cleaned row."""
    p   = []
    g   = lambda k: (row.get(k) or '').strip()
    def clean(v, field):
        return validate_and_fix(v, field)[0] if v else v

    # Title
    title = clean(g('new_title'), 'new_title')
    if title:
        p.append({'op': 'replace', 'path': '/attributes/item_name',
                  'value': _txt(title, mkt)})

    # Bullets
    bullets = []
    for bf in BULLET_FIELDS:
        b = clean(g(bf), bf)
        if b:
            bullets.append({'value': b, 'language_tag': 'en_US', 'marketplace_id': mkt})
    if bullets:
        p.append({'op': 'replace', 'path': '/attributes/bullet_point',
                  'value': bullets})

    # Description
    desc = clean(g('description'), 'description')
    if desc:
        p.append({'op': 'replace', 'path': '/attributes/product_description',
                  'value': _txt(desc, mkt)})

    # Backend search terms
    bst = g('backend_search_terms')
    if bst:
        bst_clean, _ = check_backend_terms(bst)
        if bst_clean:
            p.append({'op': 'replace', 'path': '/attributes/generic_keyword',
                      'value': _txt(bst_clean, mkt)})

    return p


# ═══════════════════════════════════════════════════════════════════════════════
# Data loading
# ═══════════════════════════════════════════════════════════════════════════════

def load_99300_asins(issues_path: Path) -> set[str]:
    """Return set of ASINs with code 99300 from issues CSV."""
    asins = set()
    with open(issues_path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            if row.get('code', '').strip() == '99300':
                asin = (row.get('asin') or '').strip().upper()
                if asin:
                    asins.add(asin)
    return asins


def load_feed_rows(target_asins: set[str]) -> list[dict]:
    """
    Search all feed CSVs (newest first) for target ASINs.
    Returns one row per ASIN (most recent version wins).
    """
    feeds = sorted(
        glob.glob(str(SCRIPT_DIR / 'pc_amazon_feed_v4_*.csv')) +
        glob.glob(str(SCRIPT_DIR / 'pc_filtered_feed_*.csv')) +
        glob.glob(str(SCRIPT_DIR / 'pc_stripped_*.csv')),
        key=os.path.getmtime, reverse=True
    )

    if not feeds:
        print('[ERROR] No feed CSVs found. Need pc_amazon_feed_v4_*.csv in the folder.')
        sys.exit(1)

    print(f'  Searching {len(feeds)} feed file(s) for {len(target_asins)} ASINs...')

    found:    dict[str, dict] = {}
    not_found = set(target_asins)

    for feed in feeds:
        if not not_found:
            break
        with open(feed, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                asin = (row.get('asin') or '').strip().upper()
                if asin in not_found:
                    found[asin] = row
                    not_found.discard(asin)

    if not_found:
        print(f'  [WARN] {len(not_found)} ASINs not found in any feed CSV — skipped')

    print(f'  Found content for {len(found)} of {len(target_asins)} ASINs')
    return list(found.values())


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(
        description='Fix error 99300 (prohibited content) on affected listings only.')
    ap.add_argument('--issues', metavar='FILE',
                    help='Issues CSV from pc_pull_issues.py')
    ap.add_argument('--rerun-blocked', metavar='RESULTS_FILE',
                    help='Re-run only BLOCKED rows from a previous results CSV, '
                         'pulling cast/extruded from live Amazon titles')
    ap.add_argument('--dry-run', action='store_true',
                    help='Validate and show changes without pushing to Amazon')
    ap.add_argument('--limit', type=int, metavar='N',
                    help='Only process first N listings (for testing)')
    args = ap.parse_args()

    if not args.issues and not args.rerun_blocked:
        ap.error('Provide either --issues or --rerun-blocked')

    # ── Rerun-blocked mode ────────────────────────────────────────────────────
    if args.rerun_blocked:
        results_path = Path(args.rerun_blocked)
        if not results_path.is_absolute():
            results_path = SCRIPT_DIR / results_path
        if not results_path.exists():
            print(f'[ERROR] Results file not found: {results_path}')
            sys.exit(1)

        blocked_rows = []
        with open(results_path, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                if row.get('status', '').upper() == 'BLOCKED':
                    blocked_rows.append(row)

        if not blocked_rows:
            print('[INFO] No BLOCKED rows found in results file.')
            sys.exit(0)

        print(f'\n  Found {len(blocked_rows)} BLOCKED SKUs — loading feed data...')
        blocked_asins = {r['asin'].strip().upper() for r in blocked_rows}
        rows = load_feed_rows(blocked_asins)

        if args.limit:
            rows = rows[:args.limit]

        creds  = load_credentials()
        tokens = TokenManager(creds)
        seller = creds['seller_id']
        mkt    = creds['marketplace_id']

        ok_count    = 0
        skip_count  = 0
        error_count = 0
        needs_manual = 0
        results     = []

        print(f'\n{"═" * 60}')
        print('  PC Fix 99300 — Rerun Blocked (Cast/Extruded)')
        print(f'  Run ID    : {RUN_ID}')
        print(f'  Source    : {results_path.name}')
        print(f'  Blocked   : {len(blocked_rows)} SKUs')
        print(f'  In feed   : {len(rows)} rows found')
        print(f'  Mode      : {"DRY RUN" if args.dry_run else "LIVE"}')
        print(f'{"═" * 60}\n')

        for n, row in enumerate(rows, 1):
            sku  = (row.get('sku') or '').strip()
            asin = (row.get('asin') or '').strip().upper()

            print(f'  [{n}/{len(rows)}] {sku}', end='  ')

            # Fetch current live title from Amazon to extract cast/extruded
            try:
                live_title = fetch_current_title(tokens, seller, mkt, sku)
            except Exception as e:
                print(f'FETCH ERROR: {e}')
                results.append({'sku': sku, 'asin': asin, 'status': 'FETCH_ERROR', 'detail': str(e)})
                error_count += 1
                time.sleep(REQUEST_GAP)
                continue

            designation = extract_cast_extruded(live_title or '')

            if not designation:
                print('NEEDS MANUAL — cast/extruded not found in live title')
                results.append({'sku': sku, 'asin': asin, 'status': 'NEEDS_MANUAL',
                                 'detail': f'live title: {(live_title or "")[:100]}'})
                needs_manual += 1
                time.sleep(REQUEST_GAP)
                continue

            # Inject designation into feed title
            feed_title = (row.get('new_title') or '').strip()
            if feed_title:
                row['new_title'] = inject_cast_extruded(feed_title, designation)

            if args.dry_run:
                print(f'WOULD ADD "{designation}" — "{row["new_title"][:80]}"')
                time.sleep(REQUEST_GAP)
                continue

            # Clean and preflight
            row = clean_row(row)

            if _PREFLIGHT:
                pf = preflight_check(row, fix_truncate=True)
                if pf.blocked:
                    print(f'BLOCKED — {"; ".join(pf.errors[:2])}')
                    results.append({'sku': sku, 'asin': asin, 'status': 'BLOCKED',
                                     'detail': '; '.join(pf.errors)})
                    skip_count += 1
                    time.sleep(REQUEST_GAP)
                    continue

            # Get product type and push
            try:
                product_type = get_product_type(tokens, seller, mkt, sku)
            except Exception as e:
                print(f'GET ERROR: {e}')
                results.append({'sku': sku, 'asin': asin, 'status': 'GET_ERROR', 'detail': str(e)})
                error_count += 1
                time.sleep(REQUEST_GAP)
                continue

            if not product_type:
                print('NOT FOUND — skipped')
                results.append({'sku': sku, 'asin': asin, 'status': 'NOT_FOUND', 'detail': ''})
                skip_count += 1
                time.sleep(REQUEST_GAP)
                continue

            patches = build_content_patches(row, mkt)
            if not patches:
                print('NO CONTENT — skipped')
                results.append({'sku': sku, 'asin': asin, 'status': 'NO_CONTENT', 'detail': ''})
                skip_count += 1
                time.sleep(REQUEST_GAP)
                continue

            try:
                status, payload = patch_listing(tokens, seller, mkt, sku, product_type, patches)
            except Exception as e:
                print(f'PATCH ERROR: {e}')
                results.append({'sku': sku, 'asin': asin, 'status': 'PATCH_ERROR', 'detail': str(e)})
                error_count += 1
                time.sleep(REQUEST_GAP)
                continue

            issues   = payload.get('issues') or []
            err_msgs = '; '.join(i.get('message', '') for i in issues if i.get('severity', '').upper() == 'ERROR')
            warn_msgs = '; '.join(i.get('message', '') for i in issues if i.get('severity', '').upper() == 'WARNING')

            if status in (200, 202):
                print(f'OK  ({designation} — {product_type})'
                      + (f'  warn: {warn_msgs[:80]}' if warn_msgs else ''))
                results.append({'sku': sku, 'asin': asin, 'status': 'OK',
                                 'product_type': product_type, 'detail': designation})
                ok_count += 1
            else:
                detail = err_msgs or str(payload)[:200]
                print(f'FAIL  HTTP {status}  {detail[:100]}')
                results.append({'sku': sku, 'asin': asin, 'status': f'HTTP_{status}',
                                 'product_type': product_type, 'detail': detail[:300]})
                error_count += 1

            time.sleep(REQUEST_GAP)

        rerun_results_file = SCRIPT_DIR / f'pc_fix_99300_rerun_{RUN_ID}.csv'
        with open(rerun_results_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['sku', 'asin', 'status', 'product_type', 'detail'],
                                    extrasaction='ignore')
            writer.writeheader()
            for r in results:
                writer.writerow(r)

        print(f'\n{"═" * 60}')
        print(f'  Pushed OK    : {ok_count}')
        print(f'  Needs manual : {needs_manual}')
        print(f'  Skipped      : {skip_count}')
        print(f'  Errors       : {error_count}')
        print(f'  Results file : {rerun_results_file.name}')
        print(f'{"═" * 60}')
        return

    # ── Normal --issues mode ──────────────────────────────────────────────────
    issues_path = Path(args.issues)
    if not issues_path.is_absolute():
        issues_path = SCRIPT_DIR / issues_path
    if not issues_path.exists():
        print(f'[ERROR] Issues file not found: {issues_path}')
        sys.exit(1)

    # Load target ASINs
    target_asins = load_99300_asins(issues_path)
    if not target_asins:
        print('[INFO] No 99300 errors found in issues CSV.')
        sys.exit(0)

    # Find their content in feed CSVs
    rows = load_feed_rows(target_asins)
    if not rows:
        print('[ERROR] No matching rows found in feed CSVs.')
        sys.exit(1)

    if args.limit:
        rows = rows[:args.limit]

    print(f'\n{"═" * 60}')
    print('  PC Fix 99300 — Prohibited Content')
    print(f'  Run ID    : {RUN_ID}')
    print(f'  Issues    : {issues_path.name}')
    print(f'  Target    : {len(target_asins)} ASINs with code 99300')
    print(f'  In feed   : {len(rows)} rows found')
    print(f'  Mode      : {"DRY RUN" if args.dry_run else "LIVE"}')
    print(f'{"═" * 60}\n')

    if args.dry_run:
        print('  Showing violations that will be cleaned:\n')
        for row in rows:
            sku  = (row.get('sku') or '').strip()
            asin = (row.get('asin') or '').strip().upper()
            all_viols = []
            for field in ['new_title'] + BULLET_FIELDS + ['description', 'backend_search_terms']:
                val = (row.get(field) or '').strip()
                if val:
                    if field == 'backend_search_terms':
                        _, viols = check_backend_terms(val)
                    else:
                        _, viols = validate_and_fix(val, field)
                    all_viols.extend(viols)
            if all_viols:
                print(f'  {sku} ({asin}) — {len(all_viols)} violation(s)')
                for v in all_viols:
                    print(f'    {v}')
            else:
                print(f'  {sku} ({asin}) — clean (no violations found)')
        print(f'\n  Would push {len(rows)} listings.')
        return

    # Live run
    creds  = load_credentials()
    tokens = TokenManager(creds)
    seller = creds['seller_id']
    mkt    = creds['marketplace_id']

    ok_count    = 0
    skip_count  = 0
    error_count = 0
    results     = []

    for n, row in enumerate(rows, 1):
        sku  = (row.get('sku') or '').strip()
        asin = (row.get('asin') or '').strip().upper()

        print(f'  [{n}/{len(rows)}] {sku}', end='  ')

        # Clean content first, then preflight the cleaned version
        row = clean_row(row)

        if _PREFLIGHT:
            result = preflight_check(row, fix_truncate=True)
            if result.blocked:
                print(f'BLOCKED — {"; ".join(result.errors[:2])}')
                results.append({'sku': sku, 'asin': asin, 'status': 'BLOCKED',
                                 'detail': '; '.join(result.errors)})
                skip_count += 1
                continue

        # Get product type
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
            print('NOT FOUND — skipped')
            results.append({'sku': sku, 'asin': asin, 'status': 'NOT_FOUND', 'detail': ''})
            skip_count += 1
            time.sleep(REQUEST_GAP)
            continue

        # Build and push patches
        patches = build_content_patches(row, mkt)
        if not patches:
            print('NO CONTENT — skipped')
            results.append({'sku': sku, 'asin': asin, 'status': 'NO_CONTENT', 'detail': ''})
            skip_count += 1
            time.sleep(REQUEST_GAP)
            continue

        try:
            status, payload = patch_listing(tokens, seller, mkt, sku, product_type, patches)
        except Exception as e:
            print(f'PATCH ERROR: {e}')
            results.append({'sku': sku, 'asin': asin, 'status': 'PATCH_ERROR',
                             'detail': str(e)})
            error_count += 1
            time.sleep(REQUEST_GAP)
            continue

        issues   = payload.get('issues') or []
        err_msgs = '; '.join(i.get('message', '') for i in issues
                             if i.get('severity', '').upper() == 'ERROR')
        warn_msgs = '; '.join(i.get('message', '') for i in issues
                              if i.get('severity', '').upper() == 'WARNING')

        if status in (200, 202):
            print(f'OK  ({product_type})'
                  + (f'  warn: {warn_msgs[:80]}' if warn_msgs else ''))
            results.append({'sku': sku, 'asin': asin, 'status': 'OK',
                             'product_type': product_type, 'detail': warn_msgs})
            ok_count += 1
        else:
            detail = err_msgs or str(payload)[:200]
            print(f'FAIL  HTTP {status}  {detail[:100]}')
            results.append({'sku': sku, 'asin': asin, 'status': f'HTTP_{status}',
                             'product_type': product_type, 'detail': detail[:300]})
            error_count += 1

        time.sleep(REQUEST_GAP)

    # Write results
    with open(RESULTS_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f, fieldnames=['sku', 'asin', 'status', 'product_type', 'detail'],
            extrasaction='ignore')
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    print(f'\n{"═" * 60}')
    print(f'  Pushed OK    : {ok_count}')
    print(f'  Skipped      : {skip_count}')
    print(f'  Errors       : {error_count}')
    print(f'  Results file : {RESULTS_FILE.name}')
    print(f'{"═" * 60}')

    if ok_count:
        print(f'\n  {ok_count} listings had prohibited content cleaned and pushed.')
        print('  Error 99300 should clear within 24 hrs on successful listings.\n')


if __name__ == '__main__':
    main()
