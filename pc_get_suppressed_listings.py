#!/usr/bin/env python3
"""
pc_get_suppressed_listings.py — Pull all search-suppressed listings from Amazon via SP-API.

Uses the Reports API to request a GET_MERCHANT_LISTINGS_SUPPRESSED_DATA report,
polls until it's ready, downloads it, and writes a clean CSV for review/fixing.

Output CSV columns:
    sku, asin, title, suppression_reason, image_url, price, quantity, open_date

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
SCRIPT_DIR   = Path(__file__).parent
RUN_ID       = datetime.now().strftime('%Y%m%d_%H%M%S')
DEFAULT_OUT  = SCRIPT_DIR / f'suppressed_listings_{RUN_ID}.csv'

# ─── SP-API constants ─────────────────────────────────────────────────────────
LWA_ENDPOINT      = 'https://api.amazon.com/auth/o2/token'
SP_API_BASE       = 'https://sellingpartnerapi-na.amazon.com'
REPORT_TYPE       = 'GET_MERCHANT_LISTINGS_SUPPRESSED_DATA'
MAX_RETRIES       = 3
REQUEST_GAP       = 0.5    # seconds between API calls
DEFAULT_POLL_SECS = 30     # seconds between report-status polls
MAX_POLL_MINUTES  = 30     # give up after this long


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
        print('        Set them before running:')
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

def create_report(tokens: TokenManager, marketplace_id: str) -> str:
    """Request the suppressed listings report and return its reportId."""
    print(f'[STEP 1] Requesting {REPORT_TYPE} report...')
    body = {
        'reportType':     REPORT_TYPE,
        'marketplaceIds': [marketplace_id],
    }
    r = sp_request('POST', '/reports/2021-06-30/reports', tokens, body=body)
    if not r.ok:
        raise RuntimeError(f'Failed to create report ({r.status_code}): {r.text[:400]}')
    report_id = r.json().get('reportId', '')
    if not report_id:
        raise RuntimeError(f'No reportId in response: {r.text[:400]}')
    print(f'         Report ID: {report_id}')
    return report_id


def poll_report(tokens: TokenManager, report_id: str, poll_secs: int) -> str:
    """Poll until the report is DONE and return the reportDocumentId."""
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
        # IN_QUEUE or IN_PROGRESS — keep polling
    raise RuntimeError(f'Report not ready after {MAX_POLL_MINUTES} minutes.')


def download_report(tokens: TokenManager, doc_id: str) -> str:
    """Fetch the document URL and download the raw report content as a string."""
    print(f'[STEP 3] Fetching download URL for document {doc_id}...')
    r = sp_request('GET', f'/reports/2021-06-30/documents/{doc_id}', tokens)
    if not r.ok:
        raise RuntimeError(f'Failed to get document URL ({r.status_code}): {r.text[:400]}')
    doc_info      = r.json()
    download_url  = doc_info.get('url', '')
    compression   = doc_info.get('compressionAlgorithm', '')
    if not download_url:
        raise RuntimeError(f'No download URL in document response: {doc_info}')

    print(f'         Downloading report content...')
    resp = requests.get(download_url, timeout=120)
    resp.raise_for_status()

    if compression == 'GZIP':
        raw = gzip.decompress(resp.content).decode('utf-8', errors='replace')
    else:
        raw = resp.content.decode('utf-8', errors='replace')

    print(f'         Downloaded {len(raw):,} characters.')
    return raw


# ═══════════════════════════════════════════════════════════════════════════════
# Parse the suppressed listings TSV
# ═══════════════════════════════════════════════════════════════════════════════

# Amazon's suppressed listings report has no dedicated "suppression_reason" column.
# Instead, it omits required attributes for suppressed listings. We infer the reason
# from the columns that are blank.
REQUIRED_FIELDS = {
    'image-url':   'Missing main image',
    'item-name':   'Missing title',
    'price':       'Missing price',
    'quantity':    'Missing quantity',
}

def infer_suppression_reason(row: dict) -> str:
    reasons = []
    for field, label in REQUIRED_FIELDS.items():
        val = row.get(field, '').strip()
        if not val or val == '0':
            reasons.append(label)
    return '; '.join(reasons) if reasons else 'See Seller Central for details'


def parse_report(raw: str) -> list[dict]:
    """Parse tab-delimited report into a list of cleaned row dicts."""
    reader = csv.DictReader(io.StringIO(raw), delimiter='\t')
    rows   = []
    for row in reader:
        # Normalise key whitespace (Amazon sometimes pads column names)
        row = {k.strip(): v.strip() for k, v in row.items()}
        rows.append(row)
    return rows


def build_output_rows(report_rows: list[dict]) -> list[dict]:
    out = []
    for row in report_rows:
        sku   = row.get('seller-sku', row.get('sku', '')).strip()
        asin  = (row.get('asin1', '') or row.get('asin2', '') or row.get('asin3', '')).strip()
        title = row.get('item-name', '').strip()
        img   = row.get('image-url', '').strip()
        price = row.get('price', '').strip()
        qty   = row.get('quantity', '').strip()
        date  = row.get('open-date', '').strip()
        reason = infer_suppression_reason(row)

        out.append({
            'sku':                sku,
            'asin':               asin,
            'title':              title,
            'suppression_reason': reason,
            'image_url':          img,
            'price':              price,
            'quantity':           qty,
            'open_date':          date,
        })
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# Write output CSV
# ═══════════════════════════════════════════════════════════════════════════════

FIELDNAMES = ['sku', 'asin', 'title', 'suppression_reason',
              'image_url', 'price', 'quantity', 'open_date']

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
        report_id = create_report(tokens, creds['marketplace_id'])
        time.sleep(REQUEST_GAP)

        doc_id = poll_report(tokens, report_id, args.poll_interval)
        time.sleep(REQUEST_GAP)

        raw = download_report(tokens, doc_id)

        print('[STEP 4] Parsing report...')
        report_rows = parse_report(raw)
        print(f'         {len(report_rows)} row(s) found in report.')

        output_rows = build_output_rows(report_rows)

        if not output_rows:
            print('\n[INFO] No suppressed listings found. Your catalog looks clean!')
            return

        write_csv(output_rows, output_path)

        # Quick summary by suppression reason
        reasons: dict[str, int] = {}
        for row in output_rows:
            for r in row['suppression_reason'].split('; '):
                reasons[r] = reasons.get(r, 0) + 1

        print('\n  Suppression reason breakdown:')
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f'    {count:>4}  {reason}')

    except KeyboardInterrupt:
        print('\n[INTERRUPTED]')
        sys.exit(1)
    except Exception as e:
        print(f'\n[FATAL] {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
