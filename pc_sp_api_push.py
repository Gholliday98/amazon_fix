#!/usr/bin/env python3
"""
pc_sp_api_push.py — Simple Amazon SP-API push for approved listing fields.

Reads the most recent pc_amazon_feed_v4_*.csv in the script folder and
submits ONLY these attributes per SKU via JSON_LISTINGS_FEED (PATCH):

    new_title              → item_name
    bullet1..bullet5       → bullet_point (list, empties skipped)
    backend_search_terms   → generic_keyword
    item_weight_lbs        → item_weight
    chargeable_weight_lbs  → item_package_weight
    box_length_in/_width_/_height_ → item_package_dimensions
    item_type_keyword      → item_type_keyword
    shipping_template      → merchant_shipping_group_name

Nothing else is touched — no description, price, quantity, or variations.

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
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION              (default: us-east-1)
    SELLER_ID
    MARKETPLACE_ID          (default: ATVPDKIKX0DER)
"""

import argparse
import csv
import glob
import gzip
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
from datetime import datetime, timezone
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
LWA_ENDPOINT      = 'https://api.amazon.com/auth/o2/token'
SP_API_HOST       = 'sellingpartnerapi-na.amazon.com'
SP_API_BASE       = f'https://{SP_API_HOST}'
AWS_SERVICE       = 'execute-api'
FEED_TYPE         = 'JSON_LISTINGS_FEED'
FEED_CONTENT_TYPE = 'application/json; charset=UTF-8'
BATCH_SIZE        = 50
POLL_INTERVAL     = 30      # seconds between status checks
POLL_TIMEOUT      = 900     # 15 min max per feed
MAX_RETRIES       = 3


# ═══════════════════════════════════════════════════════════════════════════════
# Credentials
# ═══════════════════════════════════════════════════════════════════════════════

def load_credentials() -> dict:
    c = {
        'lwa_client_id':     os.environ.get('LWA_CLIENT_ID', ''),
        'lwa_client_secret': os.environ.get('LWA_CLIENT_SECRET', ''),
        'lwa_refresh_token': os.environ.get('LWA_REFRESH_TOKEN', ''),
        'aws_access_key':    os.environ.get('AWS_ACCESS_KEY_ID', ''),
        'aws_secret_key':    os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
        'aws_region':        os.environ.get('AWS_REGION', 'us-east-1'),
        'seller_id':         os.environ.get('SELLER_ID', ''),
        'marketplace_id':    os.environ.get('MARKETPLACE_ID', 'ATVPDKIKX0DER'),
    }
    required = ['lwa_client_id', 'lwa_client_secret', 'lwa_refresh_token',
                'aws_access_key', 'aws_secret_key', 'seller_id']
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
# AWS Signature V4
# ═══════════════════════════════════════════════════════════════════════════════

def _hmac256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode('utf-8'), digestmod=hashlib.sha256).digest()


def _signing_key(secret: str, date: str, region: str) -> bytes:
    k = _hmac256(('AWS4' + secret).encode(), date)
    k = _hmac256(k, region)
    k = _hmac256(k, AWS_SERVICE)
    return _hmac256(k, 'aws4_request')


def sign_request(method: str, url: str, headers: dict, payload: bytes,
                 access_key: str, secret_key: str, region: str) -> dict:
    parsed   = urllib.parse.urlparse(url)
    now      = datetime.now(timezone.utc)
    amz_date = now.strftime('%Y%m%dT%H%M%SZ')
    date_str = now.strftime('%Y%m%d')

    h = {'host': parsed.netloc, 'x-amz-date': amz_date,
         **{k.lower(): v for k, v in headers.items()}}
    sh   = sorted(h.items())
    ch   = ''.join(f'{k}:{v}\n' for k, v in sh)
    sig_hdrs = ';'.join(k for k, _ in sh)
    p_hash   = hashlib.sha256(payload).hexdigest()
    qs       = urllib.parse.urlencode(sorted(urllib.parse.parse_qsl(parsed.query or '')))
    creq     = '\n'.join([method,
                          urllib.parse.quote(parsed.path or '/', safe='/-_.~'),
                          qs, ch, sig_hdrs, p_hash])
    scope    = f'{date_str}/{region}/{AWS_SERVICE}/aws4_request'
    sts      = '\n'.join(['AWS4-HMAC-SHA256', amz_date, scope,
                          hashlib.sha256(creq.encode()).hexdigest()])
    sig      = hmac.new(_signing_key(secret_key, date_str, region),
                        sts.encode(), digestmod=hashlib.sha256).hexdigest()

    return {**headers,
            'Host': parsed.netloc,
            'X-Amz-Date': amz_date,
            'Authorization': (f'AWS4-HMAC-SHA256 Credential={access_key}/{scope}, '
                              f'SignedHeaders={sig_hdrs}, Signature={sig}')}


# ═══════════════════════════════════════════════════════════════════════════════
# SP-API calls
# ═══════════════════════════════════════════════════════════════════════════════

def sp_call(method: str, path: str, creds: dict, tokens: TokenManager,
            body: dict | None = None) -> requests.Response:
    """SP-API request — LWA token only (SigV4 deprecated as of late 2023)."""
    url     = f'{SP_API_BASE}{path}'
    payload = json.dumps(body).encode() if body else b''
    headers = {'x-amz-access-token': tokens.get(),
               'Content-Type':       'application/json',
               'Accept':             'application/json'}
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.request(method, url, headers=headers,
                                 data=payload, timeout=60)
            if r.status_code == 429:
                wait = 5 * (2 ** attempt)
                print(f'    [THROTTLE] {path}: waiting {wait}s')
                time.sleep(wait)
                headers['x-amz-access-token'] = tokens.get()
                continue
            if not r.ok:
                # Print response body on error — helps diagnose 403 / 400
                print(f'    [HTTP {r.status_code}] {path}')
                if r.text:
                    print(f'    Response: {r.text[:500]}')
            return r
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** (attempt + 1))
            print(f'    [RETRY] {path} ({e})')
    raise RuntimeError(f'{path}: failed after retries')


def create_feed_document(creds, tokens) -> dict:
    r = sp_call('POST', '/feeds/2021-06-30/feedDocuments', creds, tokens,
                body={'contentType': FEED_CONTENT_TYPE})
    r.raise_for_status()
    return r.json()


def upload_feed(url: str, data: bytes) -> None:
    r = requests.put(url, data=data,
                     headers={'Content-Type': FEED_CONTENT_TYPE}, timeout=120)
    r.raise_for_status()


def create_feed(creds, tokens, doc_id: str) -> str:
    r = sp_call('POST', '/feeds/2021-06-30/feeds', creds, tokens, body={
        'feedType':            FEED_TYPE,
        'marketplaceIds':      [creds['marketplace_id']],
        'inputFeedDocumentId': doc_id,
    })
    r.raise_for_status()
    return r.json()['feedId']


def poll_feed(creds, tokens, feed_id: str) -> dict:
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        r = sp_call('GET', f'/feeds/2021-06-30/feeds/{feed_id}', creds, tokens)
        r.raise_for_status()
        s  = r.json()
        ps = s.get('processingStatus', '')
        print(f'    [POLL] {feed_id}: {ps}')
        if ps in ('DONE', 'FATAL', 'CANCELLED'):
            return s
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f'Feed {feed_id} did not finish within {POLL_TIMEOUT}s')


def get_result(creds, tokens, doc_id: str) -> str:
    r = sp_call('GET', f'/feeds/2021-06-30/feedDocuments/{doc_id}', creds, tokens)
    r.raise_for_status()
    d = r.json()
    dl = requests.get(d['url'], timeout=120)
    dl.raise_for_status()
    body = dl.content
    if d.get('compressionAlgorithm') == 'GZIP':
        body = gzip.decompress(body)
    return body.decode('utf-8', errors='replace')


# ═══════════════════════════════════════════════════════════════════════════════
# Build JSON_LISTINGS_FEED  (only the requested fields)
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

    if g('shipping_template'):
        p.append({'op': 'replace', 'path': '/attributes/merchant_shipping_group_name',
                  'value': _plain(g('shipping_template'), mkt)})

    return p


def build_feed_body(rows: list[dict], seller_id: str, mkt: str) -> bytes:
    msgs = []
    for idx, row in enumerate(rows, start=1):
        sku = (row.get('sku', '') or '').strip()
        pts = build_patches(row, mkt)
        if not sku or not pts:
            continue
        msgs.append({
            'messageId':     idx,
            'sku':           sku,
            'operationType': 'PATCH',
            'productType':   'PRODUCT',
            'patches':       pts,
        })
    doc = {
        'header':   {'sellerId': seller_id, 'version': '2.0', 'issueLocale': 'en_US'},
        'messages': msgs,
    }
    return json.dumps(doc, ensure_ascii=False, indent=2).encode('utf-8')


# ═══════════════════════════════════════════════════════════════════════════════
# Parse processing report
# ═══════════════════════════════════════════════════════════════════════════════

def parse_result(body: str) -> dict[str, str]:
    if not body.strip():
        return {}
    try:
        doc = json.loads(body)
    except json.JSONDecodeError:
        return {}
    errs = {}
    for issue in doc.get('issues', []):
        sev = issue.get('severity', '')
        if sev not in ('ERROR', 'WARNING'):
            continue
        sku = issue.get('sku', '')
        msg = f"{sev} [{issue.get('code','')}] {issue.get('message','')}"
        if errs.get(sku, '').startswith('ERROR') and sev == 'WARNING':
            continue
        errs[sku] = msg
    return errs


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
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[1],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--dry-run', action='store_true',
                    help='Build + validate JSON without submitting to Amazon')
    ap.add_argument('--limit', type=int, default=0, metavar='N',
                    help='Cap to first N listings')
    return ap.parse_args()


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    args  = parse_args()
    creds = load_credentials()
    csv_path = find_csv()

    print('\n' + '═' * 60)
    print(f'  PC SP-API Push  ({"DRY RUN" if args.dry_run else "LIVE"})')
    print(f'  Run ID : {RUN_ID}')
    print(f'  Input  : {csv_path.name}')
    print(f'  Seller : {creds["seller_id"]}')
    print(f'  Market : {creds["marketplace_id"]}')
    print('═' * 60)

    rows = load_rows(csv_path, args.limit)
    if not rows:
        print('\n  No eligible rows (need non-empty sku + new_title). Exiting.')
        return
    print(f'\n  Loaded {len(rows)} listing(s)')

    batches = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
    print(f'  Batches: {len(batches)} × ≤{BATCH_SIZE}\n')

    # Open results CSV
    fh = open(RESULTS_FILE, 'w', newline='', encoding='utf-8')
    writer = csv.DictWriter(fh, fieldnames=[
        'sku', 'asin', 'title', 'feed_id', 'status', 'error_message', 'pushed_at',
    ])
    writer.writeheader()
    fh.flush()

    def record(sku, asin, title, feed_id, status, error=''):
        writer.writerow({
            'sku': sku, 'asin': asin, 'title': title[:120],
            'feed_id': feed_id, 'status': status, 'error_message': error,
            'pushed_at': datetime.now().isoformat(timespec='seconds'),
        })
        fh.flush()

    tokens = TokenManager(creds)
    stats  = {'success': 0, 'warning': 0, 'error': 0, 'feeds': 0}

    try:
        for n, batch in enumerate(batches, 1):
            print(f'  Batch {n}/{len(batches)}  ({len(batch)} listings)')
            body = build_feed_body(batch, creds['seller_id'], creds['marketplace_id'])

            if args.dry_run:
                if n == 1:
                    print('\n  [DRY RUN] JSON preview (first 1500 bytes):')
                    for line in body[:1500].decode('utf-8', errors='replace').splitlines():
                        print(f'    {line}')
                    print()
                for r in batch:
                    record((r.get('sku', '') or '').strip(),
                           (r.get('asin', '') or '').strip(),
                           (r.get('new_title', '') or '').strip(),
                           '', 'dry_run')
                    stats['success'] += 1
                print(f'    [DRY RUN] Batch {n} validated — not submitted.\n')
                continue

            feed_id = ''
            try:
                print(f'    Creating feed document …', end=' ', flush=True)
                doc = create_feed_document(creds, tokens)
                print(f'OK ({doc["feedDocumentId"]})')

                print(f'    Uploading JSON ({len(body):,} bytes) …', end=' ', flush=True)
                upload_feed(doc['url'], body)
                print('OK')

                print(f'    Creating feed …', end=' ', flush=True)
                feed_id = create_feed(creds, tokens, doc['feedDocumentId'])
                stats['feeds'] += 1
                print(f'OK (feedId={feed_id})')

                print(f'    Polling (timeout {POLL_TIMEOUT}s) …')
                final = poll_feed(creds, tokens, feed_id)
                ps    = final.get('processingStatus', 'UNKNOWN')
                print(f'    Status: {ps}')
            except Exception as exc:
                print(f'FAILED: {exc}')
                for r in batch:
                    record((r.get('sku', '') or '').strip(),
                           (r.get('asin', '') or '').strip(),
                           (r.get('new_title', '') or '').strip(),
                           feed_id, 'error', str(exc))
                    stats['error'] += 1
                print()
                continue

            if ps in ('FATAL', 'CANCELLED'):
                for r in batch:
                    record((r.get('sku', '') or '').strip(),
                           (r.get('asin', '') or '').strip(),
                           (r.get('new_title', '') or '').strip(),
                           feed_id, ps.lower(), f'Feed {ps}')
                    stats['error'] += 1
                print()
                continue

            sku_errors = {}
            rid = final.get('resultFeedDocumentId', '')
            if rid:
                try:
                    print(f'    Fetching result …', end=' ', flush=True)
                    sku_errors = parse_result(get_result(creds, tokens, rid))
                    print(f'OK ({len(sku_errors)} issue(s))')
                except Exception as exc:
                    print(f'WARN: {exc}')

            for r in batch:
                sku   = (r.get('sku', '') or '').strip()
                asin  = (r.get('asin', '') or '').strip()
                title = (r.get('new_title', '') or '').strip()
                err   = sku_errors.get(sku, '')
                if not err:
                    record(sku, asin, title, feed_id, 'success')
                    stats['success'] += 1
                elif err.startswith('WARNING'):
                    record(sku, asin, title, feed_id, 'warning', err)
                    stats['warning'] += 1
                    print(f'    [WARN]  {sku}: {err}')
                else:
                    record(sku, asin, title, feed_id, 'error', err)
                    stats['error'] += 1
                    print(f'    [ERROR] {sku}: {err}')
            print()

    finally:
        fh.close()

    print('═' * 60)
    print('  DONE')
    print(f'  Success  : {stats["success"]}')
    print(f'  Warnings : {stats["warning"]}')
    print(f'  Errors   : {stats["error"]}')
    print(f'  Feeds    : {stats["feeds"]}')
    print(f'\n  Results  → {RESULTS_FILE.name}')
    print('═' * 60 + '\n')


if __name__ == '__main__':
    main()
