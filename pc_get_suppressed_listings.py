#!/usr/bin/env python3
"""
pc_get_suppressed_listings.py — Pull all search-suppressed listings from Amazon via SP-API.

Tries GET_MERCHANTS_LISTINGS_FYP_REPORT first (Amazon's current recommended report),
then falls back to GET_MERCHANT_LISTINGS_SUPPRESSED_DATA if that fails.

Output CSV columns:
    sku, asin, title, errors

Usage
-----
    python pc_get_suppressed_listings.py
    python pc_get_suppressed_listings.py --output my_suppressed.csv
    python pc_get_suppressed_listings.py --poll-interval 30   # seconds between status checks

Environment variables
---------------------
    LWA_CLIENT_ID
    LWA_CLIENT_SECRET
    LWA_REFRESH_TOKEN
    SELLER_ID
    MARKETPLACE_ID     (default: ATVPDKIKX0DER = US)
"""

import argparse
import csv
import gzip
import io
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
SCRIPT_DIR  = Path(__file__).parent
RUN_ID      = datetime.now().strftime('%Y%m%d_%H%M%S')
DEFAULT_OUT = SCRIPT_DIR / f'suppressed_listings_{RUN_ID}.csv'

# ─── SP-API constants ─────────────────────────────────────────────────────────
LWA_ENDPOINT      = 'https://api.amazon.com/auth/o2/token'
SP_API_BASE       = 'https://sellingpartnerapi-na.amazon.com'
MAX_RETRIES       = 3
REQUEST_GAP       = 0.5
DEFAULT_POLL_SECS = 30
MAX_POLL_MINUTES  = 30

# Primary: Amazon's current "Fix Your Products" report (replaces the old defect reports).
# Fallback: older suppressed-only report that may still be active on some accounts.
REPORT_PRIMARY  = 'GET_MERCHANTS_LISTINGS_FYP_REPORT'
REPORT_FALLBACK = 'GET_MERCHANT_LISTINGS_SUPPRESSED_DATA'


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
        for k in missing:
            print(f'        export {k.upper()}="..."')
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
        print('[AUTH] Access token refreshed.')
        return self._tok


# ═══════════════════════════════════════════════════════════════════════════════
# SP-API helper
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
                wait = 10 * (2 ** attempt)
                print(f'  [THROTTLE] Rate limited — waiting {wait}s')
                time.sleep(wait)
                headers['x-amz-access-token'] = tokens.get()
                continue
            return r
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** (attempt + 1))
            print(f'  [RETRY] {path} ({e})')
    raise RuntimeError(f'{path}: failed after {MAX_RETRIES} retries')


# ═══════════════════════════════════════════════════════════════════════════════
# Reports API flow
# ═══════════════════════════════════════════════════════════════════════════════

def create_report(tokens: TokenManager, marketplace_id: str, report_type: str) -> str:
    print(f'[STEP 1] Requesting {report_type} report...')
    r = sp_request('POST', '/reports/2021-06-30/reports', tokens, body={
        'reportType':     report_type,
        'marketplaceIds': [marketplace_id],
    })
    if not r.ok:
        raise RuntimeError(f'Failed to create report ({r.status_code}): {r.text[:400]}')
    report_id = r.json().get('reportId', '')
    if not report_id:
        raise RuntimeError(f'No reportId in response: {r.text[:400]}')
    print(f'         Report ID: {report_id}')
    return report_id


def poll_report(tokens: TokenManager, report_id: str, poll_secs: int) -> str:
    print(f'[STEP 2] Waiting for report to complete (checking every {poll_secs}s)...')
    deadline = time.time() + MAX_POLL_MINUTES * 60
    while time.time() < deadline:
        time.sleep(poll_secs)
        r = sp_request('GET', f'/reports/2021-06-30/reports/{report_id}', tokens)
        if not r.ok:
            print(f'  [WARN] Status check failed ({r.status_code}) — retrying')
            continue
        data   = r.json()
        status = data.get('processingStatus', 'UNKNOWN')
        print(f'         Status: {status}')
        if status == 'DONE':
            doc_id = data.get('reportDocumentId', '')
            if not doc_id:
                raise RuntimeError('Report DONE but no reportDocumentId returned.')
            return doc_id
        if status in ('CANCELLED', 'FATAL'):
            raise RuntimeError(f'Report ended with status {status}: {data}')
    raise RuntimeError(f'Report not ready after {MAX_POLL_MINUTES} minutes.')


def download_report(tokens: TokenManager, doc_id: str) -> str:
    print(f'[STEP 3] Fetching download URL for document {doc_id}...')
    r = sp_request('GET', f'/reports/2021-06-30/documents/{doc_id}', tokens)
    if not r.ok:
        raise RuntimeError(f'Failed to get document URL ({r.status_code}): {r.text[:400]}')
    doc_info     = r.json()
    download_url = doc_info.get('url', '')
    compression  = doc_info.get('compressionAlgorithm', '')
    if not download_url:
        raise RuntimeError(f'No download URL in document response: {doc_info}')

    print('         Downloading report content...')
    resp = requests.get(download_url, timeout=120)
    resp.raise_for_status()

    raw = gzip.decompress(resp.content).decode('utf-8', errors='replace') \
          if compression == 'GZIP' \
          else resp.content.decode('utf-8', errors='replace')

    print(f'         Downloaded {len(raw):,} characters.')
    return raw


# ═══════════════════════════════════════════════════════════════════════════════
# Parse report — handles both FYP and legacy suppressed-data formats
# ═══════════════════════════════════════════════════════════════════════════════

# FYP report: has an explicit status column — we only keep Search Suppressed rows.
FYP_SUPPRESSED_STATUS = 'search suppressed'

# Legacy report: no status column; infer errors from blank required fields.
LEGACY_REQUIRED_FIELDS = {
    'image-url': 'Missing main image',
    'item-name': 'Missing title',
    'price':     'Missing price',
    'quantity':  'Missing quantity',
}


def parse_tsv(raw: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(raw), delimiter='\t')
    return [{k.strip(): v.strip() for k, v in row.items()} for row in reader]


def build_output_rows_fyp(report_rows: list[dict]) -> list[dict]:
    # Print column headers and a sample row so we can see the report structure
    if report_rows:
        print(f'         Columns: {list(report_rows[0].keys())}')
        print(f'         Sample row: {report_rows[0]}')

    out = []
    for row in report_rows:
        status = row.get('status', row.get('listing-status', '')).lower()
        if not any(kw in status for kw in ('suppressed', 'search suppress')):
            continue
        sku    = row.get('seller-sku', row.get('sku', '')).strip()
        asin   = row.get('asin', row.get('asin1', '')).strip()
        title  = row.get('item-name', row.get('product-name', '')).strip()
        errors = row.get('issue', row.get('suppression-reason',
                 row.get('error-message', 'Search Suppressed — see Seller Central'))).strip()
        out.append({'sku': sku, 'asin': asin, 'title': title, 'errors': errors})
    return out


def build_output_rows_legacy(report_rows: list[dict]) -> list[dict]:
    out = []
    for row in report_rows:
        sku   = row.get('seller-sku', row.get('sku', '')).strip()
        asin  = (row.get('asin1', '') or row.get('asin2', '') or row.get('asin3', '')).strip()
        title = row.get('item-name', '').strip()
        errs  = [label for field, label in LEGACY_REQUIRED_FIELDS.items()
                 if not row.get(field, '').strip() or row.get(field, '') == '0']
        out.append({
            'sku':    sku,
            'asin':   asin,
            'title':  title,
            'errors': '; '.join(errs) if errs else 'Check Seller Central for details',
        })
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# Write output CSV
# ═══════════════════════════════════════════════════════════════════════════════

FIELDNAMES = ['sku', 'asin', 'title', 'errors']


def write_csv(rows: list[dict], output_path: Path) -> None:
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f'\n[DONE] {len(rows)} suppressed listing(s) written to:')
    print(f'       {output_path}')


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def run_report(tokens: TokenManager, marketplace_id: str,
               report_type: str, poll_secs: int, is_fyp: bool) -> list[dict]:
    report_id   = create_report(tokens, marketplace_id, report_type)
    time.sleep(REQUEST_GAP)
    doc_id      = poll_report(tokens, report_id, poll_secs)
    time.sleep(REQUEST_GAP)
    raw         = download_report(tokens, doc_id)
    print('[STEP 4] Parsing report...')
    report_rows = parse_tsv(raw)
    print(f'         {len(report_rows)} row(s) found in report.')
    return build_output_rows_fyp(report_rows) if is_fyp \
           else build_output_rows_legacy(report_rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Pull all search-suppressed Amazon listings to a CSV.')
    parser.add_argument('--output', default=str(DEFAULT_OUT),
                        help='Path for the output CSV (default: suppressed_listings_<timestamp>.csv)')
    parser.add_argument('--poll-interval', type=int, default=DEFAULT_POLL_SECS,
                        help=f'Seconds between report-status polls (default: {DEFAULT_POLL_SECS})')
    args = parser.parse_args()

    output_path = Path(args.output)

    print('=' * 60)
    print('  Amazon Suppressed Listings Pull')
    print(f'  {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60)

    creds  = load_credentials()
    tokens = TokenManager(creds)

    try:
        # Try the current FYP report first; fall back to legacy if it errors
        try:
            output_rows = run_report(tokens, creds['marketplace_id'],
                                     REPORT_PRIMARY, args.poll_interval, is_fyp=True)
        except RuntimeError as e:
            print(f'\n  [WARN] FYP report failed ({e})')
            print(f'  [WARN] Falling back to {REPORT_FALLBACK}...\n')
            output_rows = run_report(tokens, creds['marketplace_id'],
                                     REPORT_FALLBACK, args.poll_interval, is_fyp=False)

        if not output_rows:
            print('\n[INFO] No search-suppressed listings found. Your catalog looks clean!')
            return

        write_csv(output_rows, output_path)

        tally: dict[str, int] = {}
        for row in output_rows:
            for e in row['errors'].split('; '):
                tally[e] = tally.get(e, 0) + 1

        print('\n  Error breakdown:')
        for error, count in sorted(tally.items(), key=lambda x: -x[1]):
            print(f'    {count:>4}  {error}')

    except KeyboardInterrupt:
        print('\n[INTERRUPTED]')
        sys.exit(1)
    except Exception as e:
        print(f'\n[FATAL] {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
