#!/usr/bin/env python3
"""
pc_fix_listing_language.py — Strip pesticidal/antimicrobial language from
Amazon listings in Groups A, B, and C before submitting policy-violation appeals.

WORKFLOW
--------
1. Reads policy_violations.csv (must be in the same folder) to find Group A/B/C ASINs.
2. Pulls a full listings report to build an ASIN → SKU map.
3. For each ASIN, GETs the live listing attributes via Listings Items API.
4. Removes bullet points / sentences that contain flagged pesticidal phrases.
5. In dry-run mode (default): prints a diff — nothing is written.
6. In --apply mode: PATCHes each changed listing and saves a backup JSON.

USAGE
-----
    # See what would change (safe — no writes):
    python pc_fix_listing_language.py

    # Test on a single ASIN first:
    python pc_fix_listing_language.py --asin B07JWMH4X7

    # Apply all changes for one group:
    python pc_fix_listing_language.py --apply --group A

    # Apply all Groups A/B/C:
    python pc_fix_listing_language.py --apply

ENVIRONMENT VARIABLES
---------------------
    LWA_CLIENT_ID, LWA_CLIENT_SECRET, LWA_REFRESH_TOKEN
    SELLER_ID
    MARKETPLACE_ID   (default: ATVPDKIKX0DER = US)
"""

import argparse
import csv
import gzip
import io
import json
import os
import re
import sys
import time
from copy import deepcopy
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
VIOLATIONS   = SCRIPT_DIR / 'policy_violations.csv'
BACKUP_FILE  = SCRIPT_DIR / f'listing_backups_{RUN_ID}.json'
CHANGES_FILE = SCRIPT_DIR / f'language_fixes_{RUN_ID}.csv'

# ─── SP-API constants ─────────────────────────────────────────────────────────
LWA_ENDPOINT = 'https://api.amazon.com/auth/o2/token'
SP_API_BASE  = 'https://sellingpartnerapi-na.amazon.com'
MAX_RETRIES  = 4
REQUEST_GAP  = 0.5

# ─── Groups to process ────────────────────────────────────────────────────────
TARGET_GROUPS = {'A_hdpe_cutboard', 'B_woodgrain', 'C_oem_cutboard'}

# ─── Phrases that trigger the EPA Pesticide policy ────────────────────────────
# Each pattern is applied to bullet points and description sentences.
# A bullet point containing ANY of these is removed entirely.
# Patterns are case-insensitive.
STRIP_PATTERNS = [
    # absorb claims
    r"will\s+not\s+absorb\s+(odors?|stains?|bacteria|microorganisms?)",
    r"does\s+not\s+absorb\s+(odors?|stains?|bacteria|microorganisms?)",
    r"won'?t\s+absorb\s+(odors?|stains?|bacteria|microorganisms?)",
    # bacteria resistance
    r"resists?\s+bacteria",
    r"bacteria[\s\-]resistant",
    r"bacterial[\s\-]resistant",
    r"\bantibacterial\b",
    r"\bantimicrobial\b",
    # odor resistance
    r"resists?\s+odors?",
    r"odor[\s\-]resistant",
    r"won'?t\s+harbor\s+(bacteria|odors?|germs?)",
    r"does\s+not\s+harbor\s+(bacteria|odors?|germs?)",
    # mold / mildew
    r"(inhibits?|prevents?|resists?)\s+(mold|mildew|microbial\s+growth|fungal?)",
    r"(mold|mildew)[\s\-]resistant",
    r"(mold|mildew)\s+proof",
    # kill / repel / eliminate
    r"(kills?|repels?|eliminates?|destroys?|mitigates?)\s+\w*\s*(germ|pest|bacteria|virus|microorganism|pathogen|mold|mildew|odor)",
    # stain resistance (borderline but often bundled with bacteria claim)
    r"stain[\s\-]resistant",
    r"resists?\s+stains?",
]
_COMPILED = [re.compile(p, re.IGNORECASE) for p in STRIP_PATTERNS]


def is_flagged(text: str) -> bool:
    return any(p.search(text) for p in _COMPILED)


def strip_flagged_sentences(text: str) -> str:
    """Remove individual sentences that contain flagged phrases."""
    # Split on sentence boundaries (period, exclamation, or newline)
    parts = re.split(r'(?<=[.!])\s+|\n', text)
    clean = [s for s in parts if s.strip() and not is_flagged(s)]
    return ' '.join(clean).strip()


# ═══════════════════════════════════════════════════════════════════════════════
# Credentials & token
# ═══════════════════════════════════════════════════════════════════════════════

def load_credentials() -> dict:
    c = {
        'lwa_client_id':     os.environ.get('LWA_CLIENT_ID', ''),
        'lwa_client_secret': os.environ.get('LWA_CLIENT_SECRET', ''),
        'lwa_refresh_token': os.environ.get('LWA_REFRESH_TOKEN', ''),
        'seller_id':         os.environ.get('SELLER_ID', ''),
        'marketplace_id':    os.environ.get('MARKETPLACE_ID', 'ATVPDKIKX0DER'),
    }
    missing = [k for k in ('lwa_client_id', 'lwa_client_secret',
                            'lwa_refresh_token', 'seller_id') if not c[k]]
    if missing:
        print(f'\n[ERROR] Missing env vars: {", ".join(k.upper() for k in missing)}')
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
        self._tok, self._exp = d['access_token'], time.time() + d.get('expires_in', 3600)
        print('[AUTH] Token refreshed.')
        return self._tok


# ═══════════════════════════════════════════════════════════════════════════════
# SP-API helper
# ═══════════════════════════════════════════════════════════════════════════════

def sp_get(path: str, tokens: TokenManager, params: dict | None = None) -> dict:
    url = f'{SP_API_BASE}{path}'
    for attempt in range(MAX_RETRIES):
        headers = {'x-amz-access-token': tokens.get(), 'Accept': 'application/json'}
        try:
            r = requests.get(url, headers=headers, params=params, timeout=60)
            if r.status_code == 429:
                wait = 10 * (2 ** attempt)
                print(f'  [THROTTLE] Waiting {wait}s...')
                time.sleep(wait)
                continue
            return r
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** (attempt + 1))
    raise RuntimeError(f'GET {path} failed after {MAX_RETRIES} retries')


def sp_patch(path: str, tokens: TokenManager,
             params: dict, body: dict) -> requests.Response:
    url = f'{SP_API_BASE}{path}'
    for attempt in range(MAX_RETRIES):
        headers = {
            'x-amz-access-token': tokens.get(),
            'Accept':             'application/json',
            'Content-Type':       'application/json',
        }
        try:
            r = requests.patch(url, headers=headers, params=params,
                               data=json.dumps(body).encode(), timeout=60)
            if r.status_code == 429:
                wait = 10 * (2 ** attempt)
                print(f'  [THROTTLE] Waiting {wait}s...')
                time.sleep(wait)
                continue
            return r
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** (attempt + 1))
    raise RuntimeError(f'PATCH {path} failed after {MAX_RETRIES} retries')


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1 — Load target ASINs from policy_violations.csv
# ═══════════════════════════════════════════════════════════════════════════════

def load_target_asins(group_filter: set[str] | None) -> list[dict]:
    if not VIOLATIONS.exists():
        print(f'[ERROR] {VIOLATIONS} not found. Run the script from the amazon_fix folder.')
        sys.exit(1)
    rows = []
    with open(VIOLATIONS, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            g = row['group'].strip()
            if g in TARGET_GROUPS:
                if group_filter is None or any(g.startswith(f'{"ABCDE"[i]}_') for i, f_ in
                                               enumerate(group_filter) if g.startswith(f_)):
                    rows.append({'group': g, 'asin': row['asin'].strip(),
                                 'product_name': row['product_name'].strip()})
    print(f'[STEP 1] {len(rows)} Group A/B/C ASINs loaded from policy_violations.csv')
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# Step 2 — Build ASIN → SKU map from listings report
# ═══════════════════════════════════════════════════════════════════════════════

def create_report(tokens: TokenManager, marketplace_id: str) -> str:
    print('[STEP 2] Requesting GET_MERCHANT_LISTINGS_ALL_DATA report for ASIN→SKU map...')
    r = requests.post(
        f'{SP_API_BASE}/reports/2021-06-30/reports',
        headers={'x-amz-access-token': tokens.get(),
                 'Accept': 'application/json',
                 'Content-Type': 'application/json'},
        data=json.dumps({'reportType': 'GET_MERCHANT_LISTINGS_ALL_DATA',
                         'marketplaceIds': [marketplace_id]}).encode(),
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f'Report create failed ({r.status_code}): {r.text[:400]}')
    report_id = r.json().get('reportId', '')
    print(f'         Report ID: {report_id}')
    return report_id


def poll_report(tokens: TokenManager, report_id: str) -> str:
    print('         Waiting for report...')
    deadline = time.time() + 20 * 60
    while time.time() < deadline:
        time.sleep(30)
        r = sp_get(f'/reports/2021-06-30/reports/{report_id}', tokens)
        status = r.json().get('processingStatus', 'UNKNOWN')
        print(f'         Status: {status}')
        if status == 'DONE':
            return r.json().get('reportDocumentId', '')
        if status in ('CANCELLED', 'FATAL'):
            raise RuntimeError(f'Report ended with status {status}')
    raise RuntimeError('Report not ready after 20 minutes')


def download_tsv(tokens: TokenManager, doc_id: str) -> list[dict]:
    r = sp_get(f'/reports/2021-06-30/documents/{doc_id}', tokens)
    doc = r.json()
    resp = requests.get(doc['url'], timeout=120)
    resp.raise_for_status()
    raw = gzip.decompress(resp.content).decode('utf-8', errors='replace') \
          if doc.get('compressionAlgorithm') == 'GZIP' \
          else resp.content.decode('utf-8', errors='replace')
    reader = csv.DictReader(io.StringIO(raw), delimiter='\t')
    return [{k.strip().lstrip('﻿'): v.strip()
             for k, v in row.items()} for row in reader]


def build_asin_sku_map(tokens: TokenManager, marketplace_id: str) -> dict[str, str]:
    report_id = create_report(tokens, marketplace_id)
    doc_id = poll_report(tokens, report_id)
    rows = download_tsv(tokens, doc_id)
    mapping = {}
    for row in rows:
        asin = (row.get('asin1') or row.get('asin2') or row.get('asin3', '')).strip()
        sku  = row.get('seller-sku', '').strip()
        if asin and sku:
            mapping[asin] = sku
    print(f'         ASIN→SKU map built: {len(mapping)} entries.')
    return mapping


# ═══════════════════════════════════════════════════════════════════════════════
# Step 3 — GET listing attributes for a SKU
# ═══════════════════════════════════════════════════════════════════════════════

def get_listing(tokens: TokenManager, seller_id: str,
                marketplace_id: str, sku: str) -> dict | None:
    encoded_sku = requests.utils.quote(sku, safe='')
    r = sp_get(
        f'/listings/2021-08-01/items/{seller_id}/{encoded_sku}',
        tokens,
        params={
            'marketplaceIds': marketplace_id,
            'includedData':   'attributes,summaries',
        },
    )
    if r.status_code == 404:
        return None
    if not r.ok:
        print(f'  [WARN] GET listing failed for {sku} ({r.status_code}): {r.text[:200]}')
        return None
    return r.json()


# ═══════════════════════════════════════════════════════════════════════════════
# Step 4 — Clean attributes
# ═══════════════════════════════════════════════════════════════════════════════

def clean_attr_value(attr_name: str, values: list[dict]) -> tuple[list[dict], list[str]]:
    """
    Returns (cleaned_values, removed_items).
    For bullet_point: removes whole bullets that are flagged.
    For item_name / product_description: strips flagged sentences.
    """
    removed = []
    cleaned = []
    for v in values:
        text = v.get('value', '')
        if attr_name == 'bullet_point':
            if is_flagged(text):
                removed.append(text)
            else:
                cleaned.append(v)
        else:
            # item_name or product_description — strip flagged sentences
            new_text = strip_flagged_sentences(text)
            if new_text != text:
                removed.append(text)
                if new_text:
                    cleaned.append({**v, 'value': new_text})
                # if new_text is empty, drop the whole value
            else:
                cleaned.append(v)
    return cleaned, removed


def compute_changes(attrs: dict) -> tuple[dict, dict]:
    """
    Returns (patched_attrs, changes_map) where changes_map is
    {attr_name: {'removed': [...], 'kept': [...]}}
    Only attrs that actually changed are in patched_attrs.
    """
    patched = {}
    changes = {}
    for attr_name in ('item_name', 'bullet_point', 'product_description'):
        if attr_name not in attrs:
            continue
        cleaned, removed = clean_attr_value(attr_name, attrs[attr_name])
        if removed:
            patched[attr_name] = cleaned
            changes[attr_name] = {'removed': removed,
                                   'kept': [v['value'] for v in cleaned]}
    return patched, changes


# ═══════════════════════════════════════════════════════════════════════════════
# Step 5 — PATCH listing
# ═══════════════════════════════════════════════════════════════════════════════

def patch_listing(tokens: TokenManager, seller_id: str,
                  marketplace_id: str, sku: str,
                  product_type: str, patched_attrs: dict) -> bool:
    patches = []
    for attr_name, new_values in patched_attrs.items():
        patches.append({
            'op':    'replace',
            'path':  f'/attributes/{attr_name}',
            'value': new_values,
        })
    if not patches:
        return True

    encoded_sku = requests.utils.quote(sku, safe='')
    r = sp_patch(
        f'/listings/2021-08-01/items/{seller_id}/{encoded_sku}',
        tokens,
        params={'marketplaceIds': marketplace_id, 'issueLocale': 'en_US'},
        body={'productType': product_type, 'patches': patches},
    )
    if r.status_code in (200, 202):
        status = r.json().get('status', 'ACCEPTED')
        print(f'    [PATCH] {status}')
        return True
    print(f'    [PATCH FAILED] {r.status_code}: {r.text[:300]}')
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def print_diff(sku: str, asin: str, product_name: str, changes: dict) -> None:
    print(f'\n  SKU: {sku}  ASIN: {asin}')
    print(f'  Title: {product_name[:70]}')
    for attr, info in changes.items():
        print(f'  [{attr}]')
        for r in info['removed']:
            print(f'    - REMOVE: {r[:120]}')
        for k in info['kept']:
            print(f'    + KEEP:   {k[:120]}')


def main():
    parser = argparse.ArgumentParser(
        description='Strip pesticidal phrases from Groups A/B/C Amazon listings.')
    parser.add_argument('--apply',   action='store_true',
                        help='Actually PATCH listings (default: dry-run only)')
    parser.add_argument('--asin',    default=None,
                        help='Process only this one ASIN (for testing)')
    parser.add_argument('--group',   default=None,
                        help='Comma-separated group letters to process, e.g. A,B  '
                             '(default: all of A, B, C)')
    args = parser.parse_args()

    mode = 'APPLY' if args.apply else 'DRY-RUN'
    print('=' * 60)
    print(f'  Listing Language Cleanup  [{mode}]')
    print(f'  {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60)
    if not args.apply:
        print('  NOTE: This is a dry-run. Pass --apply to write changes.\n')

    # Resolve group filter
    group_filter = None
    if args.group:
        letters = {g.strip().upper() for g in args.group.split(',')}
        group_filter = {
            f'{letter}_' for letter in letters
        }

    # Load target ASINs
    targets = load_target_asins(group_filter)
    if args.asin:
        targets = [t for t in targets if t['asin'] == args.asin]
        if not targets:
            print(f'[WARN] ASIN {args.asin} not found in Group A/B/C violations.')
            sys.exit(0)

    if not targets:
        print('[INFO] No targets matched.')
        sys.exit(0)

    creds  = load_credentials()
    tokens = TokenManager(creds)

    # Build ASIN → SKU map
    asin_sku_map = build_asin_sku_map(tokens, creds['marketplace_id'])

    # Match targets to SKUs
    unmatched = [t for t in targets if t['asin'] not in asin_sku_map]
    if unmatched:
        print(f'\n[WARN] {len(unmatched)} ASINs not found in listings report '
              f'(possibly fully removed):')
        for t in unmatched:
            print(f'       {t["asin"]}  {t["product_name"][:60]}')

    matched = [t for t in targets if t['asin'] in asin_sku_map]
    print(f'\n[STEP 3] Processing {len(matched)} listings...')

    backups   = {}
    fix_rows  = []
    n_changed = 0
    n_failed  = 0

    for t in matched:
        asin = t['asin']
        sku  = asin_sku_map[asin]
        time.sleep(REQUEST_GAP)

        listing = get_listing(tokens, creds['seller_id'],
                              creds['marketplace_id'], sku)
        if not listing:
            print(f'  [SKIP] {sku} — could not retrieve listing.')
            continue

        attrs = listing.get('attributes', {})
        product_type = ''
        summaries = listing.get('summaries', [])
        if summaries:
            product_type = summaries[0].get('productType', '')

        patched_attrs, changes = compute_changes(attrs)

        if not changes:
            print(f'  [CLEAN] {sku} — no flagged language found.')
            continue

        n_changed += 1
        print_diff(sku, asin, t['product_name'], changes)

        # Record for CSV output
        for attr, info in changes.items():
            for phrase in info['removed']:
                fix_rows.append({
                    'group':    t['group'],
                    'asin':     asin,
                    'sku':      sku,
                    'attr':     attr,
                    'removed':  phrase[:200],
                    'applied':  'NO',
                })

        if args.apply:
            # Save backup before touching anything
            backups[sku] = {'asin': asin, 'original_attributes': deepcopy(attrs)}

            ok = patch_listing(tokens, creds['seller_id'],
                               creds['marketplace_id'], sku,
                               product_type, patched_attrs)
            if ok:
                for row in fix_rows:
                    if row['sku'] == sku:
                        row['applied'] = 'YES'
            else:
                n_failed += 1

    # Save backup JSON
    if args.apply and backups:
        BACKUP_FILE.write_text(json.dumps(backups, indent=2), encoding='utf-8')
        print(f'\n[BACKUP] Original attributes saved to:\n         {BACKUP_FILE}')

    # Save changes CSV
    if fix_rows:
        with open(CHANGES_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['group','asin','sku','attr','removed','applied'])
            writer.writeheader()
            writer.writerows(fix_rows)
        print(f'[LOG]    Changes log saved to:\n         {CHANGES_FILE}')

    print(f'\n{"="*60}')
    print(f'  SUMMARY')
    print(f'  Targets:       {len(targets)}')
    print(f'  Matched SKUs:  {len(matched)}')
    print(f'  With changes:  {n_changed}')
    if args.apply:
        print(f'  Patched OK:    {n_changed - n_failed}')
        print(f'  Patch failed:  {n_failed}')
    else:
        print(f'  (Run with --apply to push these changes)')
    print('=' * 60)


if __name__ == '__main__':
    main()
