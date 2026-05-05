#!/usr/bin/env python3
"""
pc_fix_content.py — General-purpose Amazon content fixer with aggressive cleaning.

Targets listings with specific error codes from the issues CSV.

Cleaning strategy:
  Title       — word/phrase level (inline fixes only — can't drop a title)
  Bullets     — entire bullet dropped if it has ANY hard policy violation
  Description — entire sentence dropped if it has ANY hard policy violation
  Backend     — token-level filtering (existing logic)

Remaining bullets are shifted up to fill any gaps left by dropped ones.

Usage:
    # Dry run — show what would be dropped
    python pc_fix_content.py --issues pc_issues_*.csv --codes 99300 --dry-run

    # Live run for specific codes
    python pc_fix_content.py --issues pc_issues_*.csv --codes 99300

    # Multiple codes
    python pc_fix_content.py --issues pc_issues_*.csv --codes 99300,100470

    # All codes in the issues file
    python pc_fix_content.py --issues pc_issues_*.csv --all-codes

    # Test with first 10
    python pc_fix_content.py --issues pc_issues_*.csv --codes 99300 --limit 10
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
RESULTS_FILE = SCRIPT_DIR / f'pc_fix_content_results_{RUN_ID}.csv'

LWA_ENDPOINT = 'https://api.amazon.com/auth/o2/token'
SP_API_BASE  = 'https://sellingpartnerapi-na.amazon.com'
MAX_RETRIES  = 3
REQUEST_GAP  = 0.25

BULLET_FIELDS = [f'bullet{i}' for i in range(1, 6)]


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
# SP-API
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


# ===============================================================================
# Aggressive cleaning
# ===============================================================================

def clean_title(text: str) -> tuple[str, list[str]]:
    """Word/phrase level — inline fixes only. Cannot drop a title."""
    return validate_and_fix(text, 'new_title') if text else (text, [])


def clean_bullet_aggressive(text: str) -> tuple[str, list[str]]:
    """
    Drop the entire bullet if it contains any HARD violation.
    Soft violations are fixed inline.
    Returns (cleaned_or_empty, violations).
    """
    if not text.strip():
        return '', []
    _, viols = validate_and_fix(text, 'bullet')
    if any(v.startswith('[HARD]') for v in viols):
        return '', viols  # drop whole bullet
    clean, soft = validate_and_fix(text, 'bullet')
    return clean, soft


def clean_description_aggressive(text: str) -> tuple[str, list[str]]:
    """
    Remove entire sentences that contain HARD violations.
    Soft violations within kept sentences are fixed inline.
    Paragraph structure is preserved.
    """
    if not text.strip():
        return text, []

    all_viols      = []
    paragraphs     = re.split(r'\n{2,}', text)
    clean_paras    = []

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # Split on sentence-ending punctuation followed by whitespace + capital letter
        parts      = re.split(r'(?<=[.!?])\s+(?=[A-Z"“])', para)
        clean_parts = []

        for part in parts:
            part = part.strip()
            if not part:
                continue
            _, viols = validate_and_fix(part, 'description')
            hard = [v for v in viols if v.startswith('[HARD]')]
            if hard:
                all_viols.extend(hard)
                continue  # drop this sentence
            clean_part, soft = validate_and_fix(part, 'description')
            all_viols.extend(soft)
            if clean_part.strip():
                clean_parts.append(clean_part.strip())

        if clean_parts:
            clean_paras.append(' '.join(clean_parts))

    return '\n\n'.join(clean_paras).strip(), all_viols


def clean_row_aggressive(row: dict) -> tuple[dict, list[str]]:
    """
    Apply aggressive cleaning to all content fields.
    Bullets with hard violations are dropped and remaining ones shifted up.
    Returns (cleaned_row, all_violations).
    """
    cleaned   = dict(row)
    all_viols = []

    # Title — word-level
    title = (cleaned.get('new_title') or '').strip()
    if title:
        cleaned['new_title'], viols = clean_title(title)
        all_viols.extend(viols)

    # Bullets — drop on hard violation, shift remaining up
    surviving = []
    for bf in BULLET_FIELDS:
        b = (cleaned.get(bf) or '').strip()
        if b:
            cb, viols = clean_bullet_aggressive(b)
            all_viols.extend(viols)
            if cb.strip():
                surviving.append(cb.strip())

    for i, bf in enumerate(BULLET_FIELDS):
        cleaned[bf] = surviving[i] if i < len(surviving) else ''

    # Description — sentence-level
    desc = (cleaned.get('description') or '').strip()
    if desc:
        cleaned['description'], viols = clean_description_aggressive(desc)
        all_viols.extend(viols)

    # Backend search terms — token-level
    bst = (cleaned.get('backend_search_terms') or '').strip()
    if bst:
        cleaned['backend_search_terms'], viols = check_backend_terms(bst)
        all_viols.extend(viols)

    return cleaned, all_viols


# ===============================================================================
# Patch builder
# ===============================================================================

def _txt(v, mkt):
    return [{'value': v, 'language_tag': 'en_US', 'marketplace_id': mkt}]


def build_patches(row: dict, mkt: str) -> list:
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


# ===============================================================================
# Data loading
# ===============================================================================

def load_target_asins(issues_path: Path, codes: set[str]) -> set[str]:
    """Return ASINs that have any of the specified error codes (empty set = all codes)."""
    asins = set()
    with open(issues_path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            code = row.get('code', '').strip()
            if not codes or code in codes:
                asin = (row.get('asin') or '').strip().upper()
                if asin:
                    asins.add(asin)
    return asins


def load_feed_rows(target_asins: set[str]) -> list[dict]:
    feeds = sorted(
        glob.glob(str(SCRIPT_DIR / 'pc_amazon_feed_v4_*.csv')) +
        glob.glob(str(SCRIPT_DIR / 'pc_filtered_feed_*.csv')) +
        glob.glob(str(SCRIPT_DIR / 'pc_stripped_*.csv')),
        key=os.path.getmtime, reverse=True
    )
    if not feeds:
        print('[ERROR] No feed CSVs found.')
        sys.exit(1)

    print(f'  Searching {len(feeds)} feed file(s) for {len(target_asins)} ASINs...')
    found     = {}
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
        print(f'  [WARN] {len(not_found)} ASINs not in any feed CSV — skipped')
    print(f'  Found content for {len(found)} of {len(target_asins)} ASINs')
    return list(found.values())


# ===============================================================================
# Main
# ===============================================================================

def main():
    ap = argparse.ArgumentParser(
        description='Fix content violations — drops whole bullets/sentences on hard violations.')
    ap.add_argument('--issues', required=True, metavar='FILE',
                    help='Issues CSV from pc_pull_issues.py')
    ap.add_argument('--codes', metavar='CODE[,CODE...]', default='99300',
                    help='Comma-separated error codes to target (default: 99300)')
    ap.add_argument('--all-codes', action='store_true',
                    help='Target all ASINs with any error code in the issues file')
    ap.add_argument('--dry-run', action='store_true',
                    help='Show what would be dropped without pushing')
    ap.add_argument('--limit', type=int, metavar='N',
                    help='Only process first N listings (for testing)')
    ap.add_argument('--verbose', action='store_true',
                    help='With --dry-run, show full before/after content for each field')
    args = ap.parse_args()

    issues_path = Path(args.issues)
    if not issues_path.is_absolute():
        issues_path = SCRIPT_DIR / issues_path
    if not issues_path.exists():
        print(f'[ERROR] Issues file not found: {issues_path}')
        sys.exit(1)

    codes: set[str] = set() if args.all_codes else {c.strip() for c in args.codes.split(',')}

    target_asins = load_target_asins(issues_path, codes)
    if not target_asins:
        print('[INFO] No matching ASINs found in issues file.')
        sys.exit(0)

    rows = load_feed_rows(target_asins)
    if not rows:
        print('[ERROR] No matching rows found in feed CSVs.')
        sys.exit(1)

    if args.limit:
        rows = rows[:args.limit]

    code_label = 'ALL CODES' if args.all_codes else args.codes

    print(f'\n{"=" * 60}')
    print('  PC Fix Content — Aggressive Clean')
    print(f'  Run ID    : {RUN_ID}')
    print(f'  Issues    : {issues_path.name}')
    print(f'  Codes     : {code_label}')
    print(f'  Target    : {len(target_asins)} ASINs')
    print(f'  In feed   : {len(rows)} rows')
    print(f'  Mode      : {"DRY RUN" if args.dry_run else "LIVE"}')
    print(f'{"=" * 60}\n')

    if args.dry_run:
        print('  Showing aggressive clean results:\n')
        total_dropped_bullets = 0

        for row in rows:
            sku    = (row.get('sku') or '').strip()
            asin   = (row.get('asin') or '').strip().upper()
            cleaned, viols = clean_row_aggressive(row)

            dropped_bullets = [
                bf for bf in BULLET_FIELDS
                if (row.get(bf) or '').strip() and not (cleaned.get(bf) or '').strip()
            ]
            changed_bullets = [
                bf for bf in BULLET_FIELDS
                if (row.get(bf) or '').strip() and (cleaned.get(bf) or '').strip()
                and (row.get(bf) or '').strip() != (cleaned.get(bf) or '').strip()
            ]
            hard_viols = [v for v in viols if '[HARD]' in v]
            soft_viols = [v for v in viols if '[SOFT]' in v]
            total_dropped_bullets += len(dropped_bullets)

            title_changed = (row.get('new_title') or '').strip() != (cleaned.get('new_title') or '').strip()
            desc_changed  = (row.get('description') or '').strip() != (cleaned.get('description') or '').strip()

            if viols or dropped_bullets:
                print(f'  {sku} ({asin})')

                if args.verbose:
                    if title_changed:
                        print(f'    TITLE before : {(row.get("new_title") or "")[:120]}')
                        print(f'    TITLE after  : {(cleaned.get("new_title") or "")[:120]}')
                    for bf in dropped_bullets:
                        print(f'    DROP {bf}  : {(row.get(bf) or "")[:120]}')
                    for bf in changed_bullets:
                        print(f'    EDIT {bf} before: {(row.get(bf) or "")[:120]}')
                        print(f'    EDIT {bf} after : {(cleaned.get(bf) or "")[:120]}')
                    if desc_changed:
                        orig_desc  = (row.get('description') or '').strip()
                        clean_desc = (cleaned.get('description') or '').strip()
                        orig_lines  = orig_desc.splitlines()
                        clean_lines = clean_desc.splitlines()
                        if len(orig_lines) != len(clean_lines) or orig_desc != clean_desc:
                            print(f'    DESC before : {orig_desc[:200]}')
                            print(f'    DESC after  : {clean_desc[:200]}')
                else:
                    if dropped_bullets:
                        print(f'    Dropped bullets : {", ".join(dropped_bullets)}')
                    if hard_viols:
                        print(f'    Hard violations : {len(hard_viols)}')
                        for v in hard_viols[:3]:
                            print(f'      {v}')
                    if soft_viols:
                        print(f'    Soft fixes      : {len(soft_viols)}')
            else:
                print(f'  {sku} ({asin}) — clean')

        print(f'\n  Summary: {len(rows)} listings, {total_dropped_bullets} bullet(s) would be dropped.')
        print(f'  Would push {len(rows)} listings.')
        return

    # Live run
    creds  = load_credentials()
    tokens = TokenManager(creds)
    seller = creds['seller_id']
    mkt    = creds['marketplace_id']

    ok_count    = 0
    skip_count  = 0
    error_count = 0

    result_fields = ['sku', 'asin', 'status', 'product_type', 'detail']
    results_fh = open(RESULTS_FILE, 'w', newline='', encoding='utf-8')
    results_writer = csv.DictWriter(results_fh, fieldnames=result_fields, extrasaction='ignore')
    results_writer.writeheader()
    results_fh.flush()

    def write_result(r):
        results_writer.writerow(r)
        results_fh.flush()

    for n, row in enumerate(rows, 1):
        sku  = (row.get('sku') or '').strip()
        asin = (row.get('asin') or '').strip().upper()

        print(f'  [{n}/{len(rows)}] {sku}', end='  ')

        orig_bullets = {bf: (row.get(bf) or '').strip() for bf in BULLET_FIELDS}
        row, viols   = clean_row_aggressive(row)
        dropped      = [bf for bf in BULLET_FIELDS
                        if orig_bullets[bf] and not (row.get(bf) or '').strip()]

        try:
            product_type = get_product_type(tokens, seller, mkt, sku)
        except Exception as e:
            print(f'GET ERROR: {e}')
            write_result({'sku': sku, 'asin': asin, 'status': 'GET_ERROR', 'detail': str(e)})
            error_count += 1
            time.sleep(REQUEST_GAP)
            continue

        if not product_type:
            print('NOT FOUND — skipped')
            write_result({'sku': sku, 'asin': asin, 'status': 'NOT_FOUND', 'detail': ''})
            skip_count += 1
            time.sleep(REQUEST_GAP)
            continue

        patches = build_patches(row, mkt)
        if not patches:
            print('NO CONTENT — skipped')
            write_result({'sku': sku, 'asin': asin, 'status': 'NO_CONTENT', 'detail': ''})
            skip_count += 1
            time.sleep(REQUEST_GAP)
            continue

        try:
            status, payload = patch_listing(tokens, seller, mkt, sku, product_type, patches)
        except Exception as e:
            print(f'PATCH ERROR: {e}')
            write_result({'sku': sku, 'asin': asin, 'status': 'PATCH_ERROR', 'detail': str(e)})
            error_count += 1
            time.sleep(REQUEST_GAP)
            continue

        issues    = payload.get('issues') or []
        err_msgs  = '; '.join(i.get('message', '') for i in issues
                              if i.get('severity', '').upper() == 'ERROR')
        warn_msgs = '; '.join(i.get('message', '') for i in issues
                              if i.get('severity', '').upper() == 'WARNING')

        if status in (200, 202):
            drop_note = f'  dropped: {", ".join(dropped)}' if dropped else ''
            print(f'OK  ({product_type}){drop_note}'
                  + (f'  warn: {warn_msgs[:60]}' if warn_msgs else ''))
            write_result({'sku': sku, 'asin': asin, 'status': 'OK',
                          'product_type': product_type,
                          'detail': (', '.join(dropped) if dropped else '') or warn_msgs})
            ok_count += 1
        else:
            detail = err_msgs or str(payload)[:200]
            print(f'FAIL  HTTP {status}  {detail[:100]}')
            write_result({'sku': sku, 'asin': asin, 'status': f'HTTP_{status}',
                          'product_type': product_type, 'detail': detail[:300]})
            error_count += 1

        time.sleep(REQUEST_GAP)

    results_fh.close()

    print(f'\n{"=" * 60}')
    print(f'  Pushed OK  : {ok_count}')
    print(f'  Skipped    : {skip_count}')
    print(f'  Errors     : {error_count}')
    print(f'  Results    : {RESULTS_FILE.name}')
    print(f'{"=" * 60}')

    if ok_count:
        print(f'\n  {ok_count} listings cleaned and pushed.')
        print('  Violations should clear within 24 hrs.\n')


if __name__ == '__main__':
    main()
