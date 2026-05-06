#!/usr/bin/env python3
"""
pc_fix_all_violations.py — Single-pass fix for ALL product policy violations.

For every SKU that has any error in the issues CSV this script will:
  1. Clean all content fields (title, bullets, description, backend keywords)
     using the full policy validator — covers error 99300 and all
     content-related restricted-product violations.
  2. If the SKU also has error 100095 (invalid Style value "dick"), replace
     the Style attribute with "Standard" in the same PATCH.
  3. Push one combined PATCH per listing — no double calls.

Errors that cannot be fixed here (image URLs, brand mismatch, GTIN issues,
price problems) are skipped automatically — only fixable violations are touched.

Usage:
    # Dry run — see what would change, no API calls
    python pc_fix_all_violations.py --issues pc_issues_20260506_124012.csv --dry-run

    # Dry run with full before/after detail
    python pc_fix_all_violations.py --issues pc_issues_20260506_124012.csv --dry-run --verbose

    # Live run — fix everything
    python pc_fix_all_violations.py --issues pc_issues_20260506_124012.csv

    # Test with first 20 listings
    python pc_fix_all_violations.py --issues pc_issues_20260506_124012.csv --limit 20

    # Only fix specific error codes (e.g. just 99300 and 100095)
    python pc_fix_all_violations.py --issues pc_issues_20260506_124012.csv --codes 99300,100095

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
except ImportError:
    print('[ERROR] pc_policy_validator.py not found.')
    sys.exit(1)

SCRIPT_DIR   = Path(__file__).parent
RUN_ID       = datetime.now().strftime('%Y%m%d_%H%M%S')
RESULTS_FILE = SCRIPT_DIR / f'pc_fix_all_violations_results_{RUN_ID}.csv'

LWA_ENDPOINT       = 'https://api.amazon.com/auth/o2/token'
SP_API_BASE        = 'https://sellingpartnerapi-na.amazon.com'
MAX_RETRIES        = 3
REQUEST_GAP        = 0.25   # 4 req/sec, well under 5/sec limit
STYLE_INVALID_CODE = '100095'
STYLE_REPLACEMENT  = 'Standard'
BULLET_FIELDS      = [f'bullet{i}' for i in range(1, 6)]


# ===============================================================================
# Credentials + token
# ===============================================================================

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


# ===============================================================================
# SP-API helpers
# ===============================================================================

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
            print(f'    [RETRY] attempt {attempt + 2}/{MAX_RETRIES} ({e})')
    raise RuntimeError(f'Failed after {MAX_RETRIES} retries: {path}')


def get_product_type(tokens, seller_id, marketplace_id, sku) -> str | None:
    r = sp_request('GET',
                   f'/listings/2021-08-01/items/{seller_id}/{quote(sku, safe="")}',
                   tokens,
                   params={'marketplaceIds': marketplace_id, 'includedData': 'summaries'})
    if not r.ok:
        return None
    for s in (r.json().get('summaries') or []):
        if s.get('productType'):
            return s['productType']
    return None


def patch_listing(tokens, seller_id, marketplace_id, sku, product_type, patches):
    body = {'productType': product_type, 'patches': patches}
    r = sp_request('PATCH',
                   f'/listings/2021-08-01/items/{seller_id}/{quote(sku, safe="")}',
                   tokens,
                   params={'marketplaceIds': marketplace_id},
                   body=body)
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, {'raw': r.text[:300]}


# ===============================================================================
# Content cleaning
# ===============================================================================

def clean_title(text: str) -> tuple[str, list[str]]:
    return validate_and_fix(text, 'new_title') if text else (text, [])


def clean_bullet(text: str) -> tuple[str, list[str]]:
    """Drop sentences with HARD violations; inline-fix SOFT violations."""
    if not text.strip():
        return '', []
    all_viols   = []
    parts       = re.split(r'(?<=[.!?])\s+(?=[A-Z""])', text.strip())
    clean_parts = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        _, viols = validate_and_fix(part, 'bullet')
        hard = [v for v in viols if v.startswith('[HARD]')]
        if hard:
            all_viols.extend(hard)
            continue
        clean_part, soft = validate_and_fix(part, 'bullet')
        all_viols.extend(soft)
        if clean_part.strip():
            clean_parts.append(clean_part.strip())
    return ' '.join(clean_parts), all_viols


def clean_description(text: str) -> tuple[str, list[str]]:
    """Remove sentences with HARD violations; inline-fix SOFT violations."""
    if not text.strip():
        return text, []
    all_viols   = []
    clean_paras = []
    for para in re.split(r'\n{2,}', text):
        para = para.strip()
        if not para:
            continue
        clean_parts = []
        for part in re.split(r'(?<=[.!?])\s+(?=[A-Z""])', para):
            part = part.strip()
            if not part:
                continue
            _, viols = validate_and_fix(part, 'description')
            hard = [v for v in viols if v.startswith('[HARD]')]
            if hard:
                all_viols.extend(hard)
                continue
            clean_part, soft = validate_and_fix(part, 'description')
            all_viols.extend(soft)
            if clean_part.strip():
                clean_parts.append(clean_part.strip())
        if clean_parts:
            clean_paras.append(' '.join(clean_parts))
    return '\n\n'.join(clean_paras).strip(), all_viols


def clean_row(row: dict) -> tuple[dict, list[str]]:
    cleaned   = dict(row)
    all_viols = []

    title = (cleaned.get('new_title') or '').strip()
    if title:
        cleaned['new_title'], v = clean_title(title)
        all_viols.extend(v)

    surviving = []
    for bf in BULLET_FIELDS:
        b = (cleaned.get(bf) or '').strip()
        if b:
            cb, v = clean_bullet(b)
            all_viols.extend(v)
            if cb.strip():
                surviving.append(cb.strip())
    for i, bf in enumerate(BULLET_FIELDS):
        cleaned[bf] = surviving[i] if i < len(surviving) else ''

    desc = (cleaned.get('description') or '').strip()
    if desc:
        cleaned['description'], v = clean_description(desc)
        all_viols.extend(v)

    bst = (cleaned.get('backend_search_terms') or '').strip()
    if bst:
        cleaned['backend_search_terms'], v = check_backend_terms(bst)
        all_viols.extend(v)

    return cleaned, all_viols


# ===============================================================================
# Patch builder
# ===============================================================================

def _txt(v, mkt):
    return [{'value': v, 'language_tag': 'en_US', 'marketplace_id': mkt}]


def build_content_patches(row: dict, mkt: str) -> list:
    p = []
    g = lambda k: (row.get(k) or '').strip()

    title = g('new_title')
    if title:
        p.append({'op': 'replace', 'path': '/attributes/item_name',
                  'value': _txt(title, mkt)})

    bullets = [{'value': g(bf), 'language_tag': 'en_US', 'marketplace_id': mkt}
               for bf in BULLET_FIELDS if g(bf)]
    if bullets:
        p.append({'op': 'replace', 'path': '/attributes/bullet_point',
                  'value': bullets})

    desc = g('description')
    if desc:
        p.append({'op': 'replace', 'path': '/attributes/product_description',
                  'value': _txt(desc, mkt)})

    bst = g('backend_search_terms')
    if bst:
        p.append({'op': 'replace', 'path': '/attributes/generic_keyword',
                  'value': _txt(bst, mkt)})

    return p


def build_style_patch(mkt: str) -> dict:
    return {
        'op': 'replace',
        'path': '/attributes/style',
        'value': [{'value': STYLE_REPLACEMENT,
                   'language_tag': 'en_US',
                   'marketplace_id': mkt}],
    }


# ===============================================================================
# Data loading
# ===============================================================================

def load_issues(issues_path: Path, filter_codes: set[str]) -> dict[str, set[str]]:
    """
    Returns {sku: {code, code, ...}} for every SKU that has at least one
    matching error code (or all codes if filter_codes is empty).
    """
    sku_codes: dict[str, set[str]] = {}
    with open(issues_path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            code = row.get('code', '').strip()
            if filter_codes and code not in filter_codes:
                continue
            sku = (row.get('sku') or '').strip()
            if sku:
                sku_codes.setdefault(sku, set()).add(code)
    return sku_codes


def load_feed_rows(target_skus: set[str]) -> dict[str, dict]:
    """Return {sku: row} for all matching SKUs across feed CSVs."""
    feeds = sorted(
        glob.glob(str(SCRIPT_DIR / 'pc_amazon_feed_v4_*.csv')) +
        glob.glob(str(SCRIPT_DIR / 'pc_filtered_feed_*.csv')) +
        glob.glob(str(SCRIPT_DIR / 'pc_stripped_*.csv')),
        key=os.path.getmtime, reverse=True
    )
    if not feeds:
        print('[ERROR] No feed CSVs found (pc_amazon_feed_v4_*.csv / pc_filtered_feed_*.csv).')
        sys.exit(1)

    print(f'  Searching {len(feeds)} feed file(s) for {len(target_skus)} SKUs...')
    found     = {}
    not_found = set(target_skus)

    for feed in feeds:
        if not not_found:
            break
        with open(feed, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                sku = (row.get('sku') or '').strip()
                if sku in not_found:
                    found[sku] = row
                    not_found.discard(sku)

    if not_found:
        print(f'  [WARN] {len(not_found)} SKUs not found in any feed CSV — will be skipped')
    print(f'  Found content for {len(found)} of {len(target_skus)} SKUs')
    return found


# ===============================================================================
# Main
# ===============================================================================

def main():
    ap = argparse.ArgumentParser(
        description='Fix ALL product policy violations in one pass (content + style).')
    ap.add_argument('--issues', required=True, metavar='FILE',
                    help='Issues CSV from pc_pull_issues.py')
    ap.add_argument('--codes', metavar='CODE[,CODE,...]', default='',
                    help='Comma-separated error codes to target. '
                         'Default: ALL codes in the issues file.')
    ap.add_argument('--dry-run', action='store_true',
                    help='Show what would change — no API calls')
    ap.add_argument('--verbose', action='store_true',
                    help='With --dry-run: show full before/after text')
    ap.add_argument('--limit', type=int, metavar='N',
                    help='Only process the first N SKUs')
    args = ap.parse_args()

    issues_path = Path(args.issues)
    if not issues_path.is_absolute():
        issues_path = SCRIPT_DIR / issues_path
    if not issues_path.exists():
        print(f'[ERROR] Issues file not found: {issues_path}')
        sys.exit(1)

    filter_codes: set[str] = (
        {c.strip() for c in args.codes.split(',') if c.strip()}
        if args.codes else set()
    )
    code_label = ', '.join(sorted(filter_codes)) if filter_codes else 'ALL CODES'

    # Load which SKUs have which error codes
    sku_codes = load_issues(issues_path, filter_codes)
    if not sku_codes:
        print('[INFO] No matching SKUs found in issues file.')
        sys.exit(0)

    # How many need the style fix?
    style_skus = {sku for sku, codes in sku_codes.items() if STYLE_INVALID_CODE in codes}

    # Load feed content
    feed_rows = load_feed_rows(set(sku_codes.keys()))

    # Build the work list: only SKUs we have content for
    work = [(sku, sku_codes[sku], feed_rows[sku])
            for sku in sku_codes if sku in feed_rows]

    if args.limit:
        work = work[:args.limit]

    print(f'\n{"=" * 62}')
    print('  PC Fix All Violations')
    print(f'  Run ID       : {RUN_ID}')
    print(f'  Issues file  : {issues_path.name}')
    print(f'  Error codes  : {code_label}')
    print(f'  SKUs targeted: {len(sku_codes)}')
    print(f'  In feed      : {len(feed_rows)}')
    print(f'  Style fixes  : {len(style_skus)} SKU(s) with error 100095')
    print(f'  To process   : {len(work)}')
    print(f'  Mode         : {"DRY RUN" if args.dry_run else "LIVE"}')
    print(f'{"=" * 62}\n')

    if args.dry_run:
        total_content_changes = 0
        total_style_fixes     = 0

        for sku, codes, row in work:
            cleaned, viols = clean_row(row)
            fix_style      = STYLE_INVALID_CODE in codes
            content_changed = (
                any((row.get(f) or '') != (cleaned.get(f) or '')
                    for f in ['new_title', 'description', 'backend_search_terms'] + BULLET_FIELDS)
            )

            if not viols and not fix_style:
                if args.verbose:
                    print(f'  {sku} — clean (no changes)')
                continue

            total_content_changes += 1 if content_changed else 0
            total_style_fixes     += 1 if fix_style else 0

            asin = (row.get('asin') or '').strip().upper()
            print(f'  {sku}  ({asin})')

            if fix_style:
                print(f'    [STYLE ] Will replace invalid style with "{STYLE_REPLACEMENT}"')

            if args.verbose:
                title_before = (row.get('new_title') or '').strip()
                title_after  = (cleaned.get('new_title') or '').strip()
                if title_before != title_after:
                    print(f'    [TITLE ] before: {title_before[:120]}')
                    print(f'    [TITLE ] after : {title_after[:120]}')

                for bf in BULLET_FIELDS:
                    before = (row.get(bf) or '').strip()
                    after  = (cleaned.get(bf) or '').strip()
                    if before and not after:
                        print(f'    [DROP  ] {bf}: {before[:120]}')
                    elif before and before != after:
                        print(f'    [EDIT  ] {bf} before: {before[:120]}')
                        print(f'    [EDIT  ] {bf} after : {after[:120]}')

                desc_before = (row.get('description') or '').strip()
                desc_after  = (cleaned.get('description') or '').strip()
                if desc_before != desc_after:
                    print(f'    [DESC  ] before: {desc_before[:160]}')
                    print(f'    [DESC  ] after : {desc_after[:160]}')
            else:
                hard = [v for v in viols if '[HARD]' in v]
                soft = [v for v in viols if '[SOFT]' in v]
                dropped = [bf for bf in BULLET_FIELDS
                           if (row.get(bf) or '').strip() and not (cleaned.get(bf) or '').strip()]
                if dropped:
                    print(f'    Dropped bullets : {", ".join(dropped)}')
                if hard:
                    print(f'    Hard violations : {len(hard)}')
                    for v in hard[:5]:
                        print(f'      {v}')
                if soft:
                    print(f'    Soft fixes      : {len(soft)}')

        print(f'\n{"=" * 62}')
        print(f'  Listings with content changes : {total_content_changes}')
        print(f'  Listings with style fix (100095): {total_style_fixes}')
        print(f'  Total that would be pushed    : {len(work)}')
        print(f'{"=" * 62}\n')
        return

    # ── Live run ──────────────────────────────────────────────────────────────
    creds  = load_credentials()
    tokens = TokenManager(creds)
    seller = creds['seller_id']
    mkt    = creds['marketplace_id']

    ok_count    = 0
    skip_count  = 0
    error_count = 0

    result_fields = ['sku', 'asin', 'status', 'product_type',
                     'content_changed', 'style_fixed', 'detail']
    results_fh = open(RESULTS_FILE, 'w', newline='', encoding='utf-8')
    results_writer = csv.DictWriter(results_fh, fieldnames=result_fields,
                                    extrasaction='ignore')
    results_writer.writeheader()
    results_fh.flush()

    def write_result(r):
        results_writer.writerow(r)
        results_fh.flush()

    for n, (sku, codes, row) in enumerate(work, 1):
        asin       = (row.get('asin') or '').strip().upper()
        fix_style  = STYLE_INVALID_CODE in codes

        print(f'  [{n}/{len(work)}] {sku}', end='  ')

        # Clean content
        cleaned, viols = clean_row(row)
        content_changed = any(
            (row.get(f) or '') != (cleaned.get(f) or '')
            for f in ['new_title', 'description', 'backend_search_terms'] + BULLET_FIELDS
        )

        # Get product type
        try:
            product_type = get_product_type(tokens, seller, mkt, sku)
        except Exception as e:
            print(f'GET ERROR: {e}')
            write_result({'sku': sku, 'asin': asin, 'status': 'GET_ERROR',
                          'content_changed': content_changed, 'style_fixed': fix_style,
                          'detail': str(e)})
            error_count += 1
            time.sleep(REQUEST_GAP)
            continue

        if not product_type:
            print('NOT FOUND — skipped')
            write_result({'sku': sku, 'asin': asin, 'status': 'NOT_FOUND',
                          'content_changed': content_changed, 'style_fixed': fix_style,
                          'detail': ''})
            skip_count += 1
            time.sleep(REQUEST_GAP)
            continue

        # Build patches
        patches = build_content_patches(cleaned, mkt)
        if fix_style:
            patches.append(build_style_patch(mkt))

        if not patches:
            print('NOTHING TO PATCH — skipped')
            write_result({'sku': sku, 'asin': asin, 'status': 'NO_CHANGES',
                          'content_changed': False, 'style_fixed': False, 'detail': ''})
            skip_count += 1
            time.sleep(REQUEST_GAP)
            continue

        # Push
        try:
            status, payload = patch_listing(tokens, seller, mkt, sku, product_type, patches)
        except Exception as e:
            print(f'PATCH ERROR: {e}')
            write_result({'sku': sku, 'asin': asin, 'status': 'PATCH_ERROR',
                          'product_type': product_type,
                          'content_changed': content_changed, 'style_fixed': fix_style,
                          'detail': str(e)})
            error_count += 1
            time.sleep(REQUEST_GAP)
            continue

        issues    = payload.get('issues') or []
        err_msgs  = '; '.join(i.get('message', '') for i in issues
                              if i.get('severity', '').upper() == 'ERROR')
        warn_msgs = '; '.join(i.get('message', '') for i in issues
                              if i.get('severity', '').upper() == 'WARNING')

        style_note   = ' +style' if fix_style else ''
        content_note = ' +content' if content_changed else ''

        if status in (200, 202):
            print(f'OK  ({product_type}){content_note}{style_note}'
                  + (f'  warn: {warn_msgs[:60]}' if warn_msgs else ''))
            write_result({'sku': sku, 'asin': asin, 'status': 'OK',
                          'product_type': product_type,
                          'content_changed': content_changed,
                          'style_fixed': fix_style,
                          'detail': warn_msgs[:200]})
            ok_count += 1
        else:
            detail = err_msgs or str(payload)[:200]
            print(f'FAIL  HTTP {status}  {detail[:80]}')
            write_result({'sku': sku, 'asin': asin, 'status': f'HTTP_{status}',
                          'product_type': product_type,
                          'content_changed': content_changed,
                          'style_fixed': fix_style,
                          'detail': detail[:300]})
            error_count += 1

        time.sleep(REQUEST_GAP)

    results_fh.close()

    print(f'\n{"=" * 62}')
    print(f'  Pushed OK    : {ok_count}')
    print(f'  Skipped      : {skip_count}')
    print(f'  Errors       : {error_count}')
    print(f'  Results file : {RESULTS_FILE.name}')
    print(f'{"=" * 62}')
    if ok_count:
        print(f'\n  {ok_count} listings fixed.')
        print('  Content violations (99300) and style errors (100095) should')
        print('  clear within 24 hrs on the next Amazon validation pass.\n')


if __name__ == '__main__':
    main()
