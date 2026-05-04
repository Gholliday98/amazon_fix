#!/usr/bin/env python3
"""
pc_pull_issues.py — Pull live listing issues directly from Amazon SP-API.

Loops through every SKU in your feed CSVs (or a specific input file), calls
GET /listings/2021-08-01/items/{seller}/{sku}?includedData=issues for each one,
and writes a full report of every active violation Amazon currently sees.

This is the real-time view — not just what failed at push time, but every flag
Amazon has raised since then through their async review process.

Usage:
    python pc_pull_issues.py                        # scan all SKUs in feed CSVs
    python pc_pull_issues.py --input my_feed.csv   # scan specific feed
    python pc_pull_issues.py --limit 100           # cap to first 100 SKUs
    python pc_pull_issues.py --fix                 # auto-feed errors into pc_fix_errors.py after

Output:
    pc_issues_TIMESTAMP.csv   — full issue report (one row per issue per SKU)
    pc_issues_summary_TIMESTAMP.txt — grouped summary by issue type

Environment variables required:
    LWA_CLIENT_ID, LWA_CLIENT_SECRET, LWA_REFRESH_TOKEN, SELLER_ID
    MARKETPLACE_ID  (default: ATVPDKIKX0DER)
"""

import argparse
import csv
import glob
import json
import os
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

try:
    import requests
except ImportError:
    print('[ERROR] requests not installed.  Run: pip install requests')
    sys.exit(1)

SCRIPT_DIR   = Path(__file__).parent
RUN_ID       = datetime.now().strftime('%Y%m%d_%H%M%S')
LWA_ENDPOINT = 'https://api.amazon.com/auth/o2/token'
SP_API_BASE  = 'https://sellingpartnerapi-na.amazon.com'
MAX_RETRIES  = 3
REQUEST_GAP  = 0.5   # conservative — issues endpoint is read-only but still throttled


# ═══════════════════════════════════════════════════════════════════════════════
# Auth (identical to push scripts)
# ═══════════════════════════════════════════════════════════════════════════════

def load_credentials() -> dict:
    c = {
        'lwa_client_id':     os.environ.get('LWA_CLIENT_ID', ''),
        'lwa_client_secret': os.environ.get('LWA_CLIENT_SECRET', ''),
        'lwa_refresh_token': os.environ.get('LWA_REFRESH_TOKEN', ''),
        'seller_id':         os.environ.get('SELLER_ID', ''),
        'marketplace_id':    os.environ.get('MARKETPLACE_ID', 'ATVPDKIKX0DER'),
    }
    missing = [k for k in ['lwa_client_id', 'lwa_client_secret', 'lwa_refresh_token', 'seller_id']
               if not c[k]]
    if missing:
        print(f'\n[ERROR] Missing env vars: {", ".join(missing)}')
        sys.exit(1)
    return c


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


def sp_request(method: str, path: str, tokens: TokenManager,
               params: dict | None = None) -> requests.Response:
    url     = f'{SP_API_BASE}{path}'
    headers = {
        'x-amz-access-token': tokens.get(),
        'Accept':             'application/json',
    }
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.request(method, url, headers=headers,
                                 params=params, timeout=60)
            if r.status_code == 429:
                wait = 10 * (2 ** attempt)
                print(f'    [THROTTLE] waiting {wait}s...')
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


# ═══════════════════════════════════════════════════════════════════════════════
# Feed loading
# ═══════════════════════════════════════════════════════════════════════════════

def load_skus(input_hint: str = '') -> list[dict]:
    """Return list of {sku, asin} dicts from feed CSVs, deduplicated by SKU."""
    if input_hint:
        p = Path(input_hint)
        if not p.is_absolute():
            p = SCRIPT_DIR / p
        feeds = [p]
    else:
        matches = glob.glob(str(SCRIPT_DIR / 'pc_amazon_feed_v4_*.csv'))
        if not matches:
            print('[ERROR] No pc_amazon_feed_v4_*.csv found. Use --input to specify a feed.')
            raise SystemExit(1)
        feeds = [Path(f) for f in sorted(matches, key=os.path.getmtime, reverse=True)]

    seen  = set()
    rows  = []
    for feed in feeds:
        with open(feed, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                sku  = (row.get('sku')  or '').strip()
                asin = (row.get('asin') or '').strip().upper()
                if sku and sku not in seen:
                    rows.append({'sku': sku, 'asin': asin})
                    seen.add(sku)
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# Issues pull
# ═══════════════════════════════════════════════════════════════════════════════

def get_issues(tokens: TokenManager, seller_id: str,
               marketplace_id: str, sku: str) -> list[dict]:
    """
    Call GET /listings/.../items/{seller}/{sku}?includedData=issues
    Returns list of issue dicts from Amazon, or [] if none / not found.
    Each issue dict has: code, severity, message, attributeNames (list)
    """
    path = f'/listings/2021-08-01/items/{seller_id}/{quote(sku, safe="")}'
    r = sp_request('GET', path, tokens, params={
        'marketplaceIds': marketplace_id,
        'includedData':   'issues',
    })

    if r.status_code == 404:
        return []
    if not r.ok:
        return [{'code': f'HTTP_{r.status_code}', 'severity': 'ERROR',
                 'message': r.text[:300], 'attributeNames': []}]

    try:
        data   = r.json()
        issues = data.get('issues') or []
        return issues
    except ValueError:
        return [{'code': 'ParseError', 'severity': 'ERROR',
                 'message': r.text[:300], 'attributeNames': []}]


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description='Pull live listing issues from Amazon SP-API.')
    ap.add_argument('--input',  metavar='FILE', help='Specific feed CSV (default: all feed CSVs)')
    ap.add_argument('--limit',  type=int, default=0, metavar='N', help='Cap to first N SKUs')
    ap.add_argument('--fix',    action='store_true',
                    help='After pulling issues, auto-run pc_fix_errors.py on affected ASINs')
    ap.add_argument('--severity', default='', metavar='LEVEL',
                    help='Filter to ERROR or WARNING only (default: both)')
    args = ap.parse_args()

    creds  = load_credentials()
    tokens = TokenManager(creds)
    seller = creds['seller_id']
    mkt    = creds['marketplace_id']

    skus = load_skus(args.input or '')
    if args.limit:
        skus = skus[:args.limit]

    print(f'\n{"═" * 60}')
    print('  PC Pull Issues')
    print(f'  Run ID    : {RUN_ID}')
    print(f'  SKUs      : {len(skus)}')
    print(f'  Seller    : {seller}')
    if args.severity:
        print(f'  Severity  : {args.severity.upper()} only')
    print(f'{"═" * 60}\n')

    issues_out  = SCRIPT_DIR / f'pc_issues_{RUN_ID}.csv'
    fieldnames  = ['sku', 'asin', 'severity', 'code', 'message', 'attributes']

    # Counters
    total_skus    = len(skus)
    skus_with_issues = 0
    skus_clean    = 0
    skus_error    = 0

    # For summary grouping
    by_code: dict[str, list[dict]] = defaultdict(list)

    with open(issues_out, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()

        for n, item in enumerate(skus, 1):
            sku  = item['sku']
            asin = item['asin']

            if n % 50 == 0 or n == 1:
                print(f'  [{n}/{total_skus}] scanning...')

            try:
                issues = get_issues(tokens, seller, mkt, sku)
            except Exception as exc:
                print(f'    [ERROR] {sku}: {exc}')
                skus_error += 1
                time.sleep(REQUEST_GAP)
                continue

            if not issues:
                skus_clean += 1
                time.sleep(REQUEST_GAP)
                continue

            filtered = issues
            if args.severity:
                filtered = [i for i in issues
                            if i.get('severity', '').upper() == args.severity.upper()]

            if not filtered:
                skus_clean += 1
                time.sleep(REQUEST_GAP)
                continue

            skus_with_issues += 1
            for issue in filtered:
                code     = issue.get('code', '')
                severity = issue.get('severity', '')
                message  = issue.get('message', '')
                attrs    = ', '.join(issue.get('attributeNames') or [])

                writer.writerow({
                    'sku':        sku,
                    'asin':       asin,
                    'severity':   severity,
                    'code':       code,
                    'message':    message,
                    'attributes': attrs,
                })
                by_code[code].append({
                    'sku': sku, 'asin': asin,
                    'severity': severity, 'message': message, 'attributes': attrs,
                })

            time.sleep(REQUEST_GAP)

    # ── Summary report ────────────────────────────────────────────────────────
    summary_out = SCRIPT_DIR / f'pc_issues_summary_{RUN_ID}.txt'
    lines = []
    lines.append('=' * 70)
    lines.append('  AMAZON LIVE ISSUES REPORT')
    lines.append(f'  Generated : {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append(f'  SKUs scanned       : {total_skus}')
    lines.append(f'  SKUs with issues   : {skus_with_issues}')
    lines.append(f'  SKUs clean         : {skus_clean}')
    if skus_error:
        lines.append(f'  API errors         : {skus_error}')
    lines.append('=' * 70)

    for code, entries in sorted(by_code.items(), key=lambda x: -len(x[1])):
        asins    = sorted({e['asin'] for e in entries if e['asin']})
        sample   = entries[0]['message']
        severity = entries[0]['severity']
        lines.append(f'\n{"─" * 70}')
        lines.append(f'  CODE     : {code}  [{severity}]')
        lines.append(f'  COUNT    : {len(entries)} issue(s) across {len(asins)} ASIN(s)')
        lines.append(f'  MESSAGE  : {sample}')
        lines.append(f'  ASINs    : {", ".join(asins[:20])}{"..." if len(asins) > 20 else ""}')

    lines.append(f'\n{"=" * 70}')
    summary_text = '\n'.join(lines)
    summary_out.write_text(summary_text, encoding='utf-8')

    print(f'\n{"═" * 60}')
    print(f'  SKUs scanned     : {total_skus}')
    print(f'  SKUs with issues : {skus_with_issues}')
    print(f'  SKUs clean       : {skus_clean}')
    print(f'\n  Full report  : {issues_out.name}')
    print(f'  Summary      : {summary_out.name}')
    print(f'{"═" * 60}\n')

    if skus_with_issues == 0:
        print('  No active issues found.')
        return

    print(summary_text)

    # ── Auto-fix ──────────────────────────────────────────────────────────────
    if args.fix and skus_with_issues:
        print(f'\n  Launching pc_fix_errors.py on affected listings...\n')
        subprocess.run([sys.executable, str(SCRIPT_DIR / 'pc_fix_errors.py')],
                       cwd=str(SCRIPT_DIR))


if __name__ == '__main__':
    main()
