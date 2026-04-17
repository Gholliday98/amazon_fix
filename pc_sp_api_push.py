#!/usr/bin/env python3
"""
pc_sp_api_push.py — Push optimized Amazon listing content via SP-API Feeds API.

Reads pc_amazon_feed_v4_[timestamp].csv (output of pc_listing_optimizer_v4.py)
and submits product data to Amazon in batches of 50 using POST_PRODUCT_DATA.

On per-SKU errors from Amazon's processing report the script automatically
applies rule-based corrections (length truncation, HTML stripping, bad-char
removal, search-term pruning) and resubmits a retry batch — one correction
pass per batch before marking remaining failures as permanent.

Usage
-----
    python pc_sp_api_push.py                          # Active listings, auto-detect CSV
    python pc_sp_api_push.py --status Inactive
    python pc_sp_api_push.py --input /path/feed.csv --dry-run
    python pc_sp_api_push.py --limit 10 --seller-id AXXXXXXXXXXXXX
    python pc_sp_api_push.py --no-self-correct        # skip auto-fix retry

Required environment variables
-------------------------------
    SP_API_REFRESH_TOKEN      LWA refresh token
    SP_API_CLIENT_ID          LWA client ID
    SP_API_CLIENT_SECRET      LWA client secret
    AWS_ACCESS_KEY_ID         AWS IAM access key
    AWS_SECRET_ACCESS_KEY     AWS IAM secret key
    SP_API_SELLER_ID          Seller ID  (or use --seller-id flag)

Output files (written to same directory as this script)
-------------------------------------------------------
    pc_push_results_[RUN_ID].csv   per-SKU push results
    pc_push_checkpoint.json        resume state (completed SKUs + batch history)
"""

import argparse
import csv
import glob
import gzip
import hashlib
import hmac
import json
import os
import re
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print('[ERROR] requests is not installed.  Run: pip install requests')
    sys.exit(1)

# ─── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR      = Path(__file__).parent
RUN_ID          = datetime.now().strftime('%Y%m%d_%H%M%S')
RESULTS_FILE    = SCRIPT_DIR / f'pc_push_results_{RUN_ID}.csv'
CHECKPOINT_FILE = SCRIPT_DIR / 'pc_push_checkpoint.json'

RESULTS_FIELDNAMES = ['sku', 'asin', 'status', 'feed_id', 'error_message', 'corrections']

# ─── SP-API Configuration ─────────────────────────────────────────────────────
LWA_ENDPOINT   = 'https://api.amazon.com/auth/o2/token'
SP_API_HOST    = 'sellingpartnerapi-na.amazon.com'
SP_API_BASE    = f'https://{SP_API_HOST}'
MARKETPLACE_ID = 'ATVPDKIKX0DER'   # US marketplace
AWS_REGION     = 'us-east-1'
AWS_SERVICE    = 'execute-api'

FEED_TYPE     = 'POST_PRODUCT_DATA'
BATCH_SIZE    = 50
MAX_RETRIES   = 5
POLL_INTERVAL = 30    # seconds between feed-status polls
POLL_TIMEOUT  = 900   # 15 min max wait per feed

# Requests-per-second limits per SP-API operation
TPS = {
    'createFeedDocument': 0.5,
    'createFeed':         0.0083,   # 1 per ~120 s
    'getFeed':            2.0,
    'getFeedDocument':    0.0083,
}

VALID_STATUSES = {'Active', 'Inactive', 'Incomplete', 'All'}


# ═══════════════════════════════════════════════════════════════════════════════
# 1. CREDENTIALS
# ═══════════════════════════════════════════════════════════════════════════════

def load_credentials(seller_id_arg: str = '') -> dict:
    """Load SP-API and AWS credentials from environment variables."""
    creds = {
        'refresh_token':  os.environ.get('SP_API_REFRESH_TOKEN', ''),
        'client_id':      os.environ.get('SP_API_CLIENT_ID', ''),
        'client_secret':  os.environ.get('SP_API_CLIENT_SECRET', ''),
        'aws_access_key': os.environ.get('AWS_ACCESS_KEY_ID', ''),
        'aws_secret_key': os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
        'seller_id':      seller_id_arg or os.environ.get('SP_API_SELLER_ID', ''),
    }
    missing = [k for k, v in creds.items() if not v]
    if missing:
        print(f'\n[ERROR] Missing required credentials: {", ".join(missing)}')
        print('  Set the corresponding environment variables.')
        print('  For seller_id use --seller-id flag or SP_API_SELLER_ID env var.')
        sys.exit(1)
    return creds


# ═══════════════════════════════════════════════════════════════════════════════
# 2. LWA TOKEN MANAGER
# ═══════════════════════════════════════════════════════════════════════════════

class TokenManager:
    """Login-with-Amazon access token with automatic refresh before expiry."""

    def __init__(self, creds: dict) -> None:
        self._creds      = creds
        self._token      = ''
        self._expires_at = 0.0

    def get(self) -> str:
        if self._token and time.time() < self._expires_at - 60:
            return self._token
        return self._refresh()

    def _refresh(self) -> str:
        resp = requests.post(
            LWA_ENDPOINT,
            data={
                'grant_type':    'refresh_token',
                'refresh_token': self._creds['refresh_token'],
                'client_id':     self._creds['client_id'],
                'client_secret': self._creds['client_secret'],
            },
            timeout=30,
        )
        if not resp.ok:
            raise RuntimeError(
                f'LWA token refresh failed {resp.status_code}: {resp.text[:300]}'
            )
        data = resp.json()
        self._token      = data['access_token']
        self._expires_at = time.time() + data.get('expires_in', 3600)
        return self._token


# ═══════════════════════════════════════════════════════════════════════════════
# 3. AWS SIGNATURE V4
# ═══════════════════════════════════════════════════════════════════════════════

def _hmac256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode('utf-8'), digestmod=hashlib.sha256).digest()


def _signing_key(secret_key: str, date_str: str) -> bytes:
    k = _hmac256(('AWS4' + secret_key).encode('utf-8'), date_str)
    k = _hmac256(k, AWS_REGION)
    k = _hmac256(k, AWS_SERVICE)
    return _hmac256(k, 'aws4_request')


def sign_request(
    method: str,
    url: str,
    headers: dict,
    payload: bytes,
    access_key: str,
    secret_key: str,
) -> dict:
    """Return a copy of *headers* augmented with SigV4 Authorization + X-Amz-Date."""
    parsed   = urllib.parse.urlparse(url)
    now      = datetime.now(timezone.utc)
    amz_date = now.strftime('%Y%m%dT%H%M%SZ')
    date_str = now.strftime('%Y%m%d')

    hdr_map = {
        'host':        parsed.netloc,
        'x-amz-date':  amz_date,
        **{k.lower(): v for k, v in headers.items()},
    }
    sorted_hdr        = sorted(hdr_map.items())
    canonical_headers = ''.join(f'{k}:{v}\n' for k, v in sorted_hdr)
    signed_headers    = ';'.join(k for k, _ in sorted_hdr)
    payload_hash      = hashlib.sha256(payload).hexdigest()
    canonical_qs      = urllib.parse.urlencode(
        sorted(urllib.parse.parse_qsl(parsed.query or ''))
    )
    canonical_request = '\n'.join([
        method,
        urllib.parse.quote(parsed.path or '/', safe='/-_.~'),
        canonical_qs,
        canonical_headers,
        signed_headers,
        payload_hash,
    ])
    credential_scope = f'{date_str}/{AWS_REGION}/{AWS_SERVICE}/aws4_request'
    string_to_sign   = '\n'.join([
        'AWS4-HMAC-SHA256',
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode('utf-8')).hexdigest(),
    ])
    sig_key   = _signing_key(secret_key, date_str)
    signature = hmac.new(
        sig_key, string_to_sign.encode('utf-8'), digestmod=hashlib.sha256
    ).hexdigest()

    return {
        **headers,
        'Host':          parsed.netloc,
        'X-Amz-Date':    amz_date,
        'Authorization': (
            f'AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, '
            f'SignedHeaders={signed_headers}, Signature={signature}'
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 4. SP-API CLIENT
# ═══════════════════════════════════════════════════════════════════════════════

class SPAPIClient:
    """Thin SP-API wrapper: rate-limiting, SigV4 signing, 429 backoff."""

    def __init__(self, creds: dict, dry_run: bool = False) -> None:
        self._creds    = creds
        self._tokens   = TokenManager(creds)
        self._dry_run  = dry_run
        self._last: dict[str, float] = {}

    # ── internals ─────────────────────────────────────────────────────────────

    def _throttle(self, op: str) -> None:
        tps     = TPS.get(op, 2.0)
        min_gap = 1.0 / tps
        elapsed = time.time() - self._last.get(op, 0.0)
        if elapsed < min_gap:
            time.sleep(min_gap - elapsed)
        self._last[op] = time.time()

    def _call(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        op: str = '',
        content_type: str = 'application/json',
    ) -> 'requests.Response':
        self._throttle(op or path)
        url     = f'{SP_API_BASE}{path}'
        payload = json.dumps(body).encode() if body else b''
        headers = {
            'x-amz-access-token': self._tokens.get(),
            'Content-Type':       content_type,
        }
        signed = sign_request(method, url, headers, payload,
                               self._creds['aws_access_key'],
                               self._creds['aws_secret_key'])

        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.request(
                    method, url, headers=signed, data=payload, timeout=60
                )
                if resp.status_code == 429:
                    wait = min(5 * (2 ** attempt), 120)
                    print(f'    [THROTTLE] {op}: waiting {wait}s (attempt {attempt + 1})')
                    time.sleep(wait)
                    headers['x-amz-access-token'] = self._tokens.get()
                    signed = sign_request(method, url, headers, payload,
                                          self._creds['aws_access_key'],
                                          self._creds['aws_secret_key'])
                    continue
                return resp
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES - 1:
                    raise
                wait = 2 ** (attempt + 1)
                print(f'    [RETRY] {op} attempt {attempt + 2}/{MAX_RETRIES}: {exc}')
                time.sleep(wait)

        raise RuntimeError(f'{op}: failed after {MAX_RETRIES} retries')

    # ── public feed workflow methods ───────────────────────────────────────────

    def create_feed_document(self) -> dict:
        if self._dry_run:
            return {'feedDocumentId': 'dry-run-doc-id', 'url': 'https://dry-run'}
        resp = self._call(
            'POST', '/feeds/2021-06-30/feedDocuments',
            body={'contentType': 'text/xml; charset=UTF-8'},
            op='createFeedDocument',
        )
        resp.raise_for_status()
        return resp.json()

    def upload_feed(self, upload_url: str, xml_bytes: bytes) -> None:
        if self._dry_run:
            return
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.put(
                    upload_url,
                    data=xml_bytes,
                    headers={'Content-Type': 'text/xml; charset=UTF-8'},
                    timeout=120,
                )
                resp.raise_for_status()
                return
            except requests.RequestException as exc:
                if attempt == MAX_RETRIES - 1:
                    raise
                wait = 2 ** (attempt + 1)
                print(f'    [RETRY] upload attempt {attempt + 2}/{MAX_RETRIES}: {exc}')
                time.sleep(wait)

    def create_feed(self, doc_id: str) -> str:
        if self._dry_run:
            return 'dry-run-feed-id'
        resp = self._call(
            'POST', '/feeds/2021-06-30/feeds',
            body={
                'feedType':            FEED_TYPE,
                'marketplaceIds':      [MARKETPLACE_ID],
                'inputFeedDocumentId': doc_id,
            },
            op='createFeed',
        )
        resp.raise_for_status()
        return resp.json()['feedId']

    def poll_until_done(self, feed_id: str) -> dict:
        if self._dry_run:
            return {'processingStatus': 'DONE'}
        deadline = time.time() + POLL_TIMEOUT
        while time.time() < deadline:
            resp = self._call('GET', f'/feeds/2021-06-30/feeds/{feed_id}',
                              op='getFeed')
            resp.raise_for_status()
            status = resp.json()
            ps     = status.get('processingStatus', '')
            print(f'    [POLL] feed {feed_id}: {ps}')
            if ps in ('DONE', 'FATAL', 'CANCELLED'):
                return status
            time.sleep(POLL_INTERVAL)
        raise TimeoutError(
            f'Feed {feed_id} did not finish within {POLL_TIMEOUT}s'
        )

    def get_result_document(self, doc_id: str) -> str:
        """Download the processing report and return its text content."""
        if self._dry_run:
            return ''
        resp = self._call(
            'GET', f'/feeds/2021-06-30/feedDocuments/{doc_id}',
            op='getFeedDocument',
        )
        resp.raise_for_status()
        doc     = resp.json()
        dl      = requests.get(doc['url'], timeout=120)
        dl.raise_for_status()
        content = dl.content
        if doc.get('compressionAlgorithm') == 'GZIP':
            content = gzip.decompress(content)
        return content.decode('utf-8', errors='replace')


# ═══════════════════════════════════════════════════════════════════════════════
# 5. FEED XML BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def _sub(parent: ET.Element, tag: str, text: str) -> None:
    """Append a child element only when text is non-empty."""
    val = (text or '').strip()
    if val:
        ET.SubElement(parent, tag).text = val


def build_feed_xml(rows: list[dict], seller_id: str) -> bytes:
    """
    Build an Amazon POST_PRODUCT_DATA XML envelope from a list of CSV row dicts.

    Fields pushed (all via DescriptionData):
        Title, Brand, Manufacturer, Description, BulletPoint x5,
        ItemType, SearchTerms, CountryOfOrigin,
        ManufacturerPartNumber, MerchantCatalogNumber,
        PackageDimensions (L/W/H + chargeable weight),
        ItemDimensions/Weight (item_weight_lbs),
        MerchantShippingGroupName

    Note: color, size, style, material, finish, contains_liquid,
    age_restriction, and item_form are category-specific and live in the
    ProductData section whose schema varies per Amazon category.  Extend
    build_feed_xml with a ProductData block for your specific category if
    those fields are required.
    """
    root = ET.Element('AmazonEnvelope')
    root.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
    root.set('xsi:noNamespaceSchemaLocation', 'amzn-envelope.xsd')

    hdr = ET.SubElement(root, 'Header')
    ET.SubElement(hdr, 'DocumentVersion').text = '1.01'
    ET.SubElement(hdr, 'MerchantIdentifier').text = seller_id

    ET.SubElement(root, 'MessageType').text    = 'Product'
    ET.SubElement(root, 'PurgeAndReplace').text = 'false'

    for idx, row in enumerate(rows, start=1):
        sku  = (row.get('sku', '') or '').strip()
        asin = (row.get('asin', '') or '').strip()
        if not sku:
            continue

        msg = ET.SubElement(root, 'Message')
        ET.SubElement(msg, 'MessageID').text    = str(idx)
        ET.SubElement(msg, 'OperationType').text = 'Update'

        product = ET.SubElement(msg, 'Product')
        ET.SubElement(product, 'SKU').text = sku

        if asin:
            std = ET.SubElement(product, 'StandardProductID')
            ET.SubElement(std, 'Type').text  = 'ASIN'
            ET.SubElement(std, 'Value').text = asin

        dd = ET.SubElement(product, 'DescriptionData')

        _sub(dd, 'Title',                row.get('new_title', ''))
        _sub(dd, 'Brand',                row.get('brand', ''))
        _sub(dd, 'Manufacturer',         row.get('manufacturer', ''))
        _sub(dd, 'Description',          row.get('description', ''))

        for i in range(1, 6):
            _sub(dd, 'BulletPoint', row.get(f'bullet{i}', ''))

        _sub(dd, 'ItemType',               row.get('item_type_keyword', ''))
        _sub(dd, 'SearchTerms',            row.get('backend_search_terms', ''))
        _sub(dd, 'CountryOfOrigin',        row.get('country_of_origin', ''))
        _sub(dd, 'ManufacturerPartNumber', row.get('part_number', ''))
        _sub(dd, 'MerchantCatalogNumber',  row.get('model_number', ''))

        # Package dimensions (box dims + chargeable weight)
        box_l = (row.get('box_length_in', '') or '').strip()
        box_w = (row.get('box_width_in',  '') or '').strip()
        box_h = (row.get('box_height_in', '') or '').strip()
        if box_l or box_w or box_h:
            pkg = ET.SubElement(dd, 'PackageDimensions')
            for tag, val in [('Length', box_l), ('Width', box_w), ('Height', box_h)]:
                if val:
                    el = ET.SubElement(pkg, tag)
                    el.set('unitOfMeasure', 'IN')
                    el.text = val
            ship_wt = (row.get('chargeable_weight_lbs', '') or '').strip()
            if ship_wt:
                w_el = ET.SubElement(pkg, 'Weight')
                w_el.set('unitOfMeasure', 'LB')
                w_el.text = ship_wt

        # Item weight
        item_wt = (row.get('item_weight_lbs', '') or '').strip()
        if item_wt:
            item_dims = ET.SubElement(dd, 'ItemDimensions')
            w_el = ET.SubElement(item_dims, 'Weight')
            w_el.set('unitOfMeasure', 'LB')
            w_el.text = item_wt

        _sub(dd, 'MerchantShippingGroupName', row.get('shipping_template', ''))

    try:
        ET.indent(root, space='  ')   # Python 3.9+
    except AttributeError:
        pass

    xml_body = ET.tostring(root, encoding='unicode', xml_declaration=False)
    return b"<?xml version='1.0' encoding='UTF-8'?>\n" + xml_body.encode('utf-8')


# ═══════════════════════════════════════════════════════════════════════════════
# 6. PROCESSING REPORT PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def parse_processing_report(xml_str: str) -> dict[str, str]:
    """
    Parse an Amazon ProcessingReport XML string.
    Returns {sku: error_message} for every Result with ResultCode Error or Warning.
    SKUs with no Result entry are assumed successful.
    """
    errors: dict[str, str] = {}
    if not xml_str.strip():
        return errors
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError as exc:
        print(f'  [WARN] Could not parse processing report XML: {exc}')
        return errors

    for result in root.iter('Result'):
        code = result.findtext('ResultCode', '')
        if code not in ('Error', 'Warning'):
            continue
        sku      = ''
        add_info = result.find('AdditionalInfo')
        if add_info is not None:
            sku = add_info.findtext('SKU', '')
        msg_code = result.findtext('ResultMessageCode', '')
        desc     = result.findtext('ResultDescription', '')
        errors[sku] = f'[{msg_code}] {desc}' if msg_code else desc

    return errors


# ═══════════════════════════════════════════════════════════════════════════════
# 7. SELF-CORRECTION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

# Regex patterns that match Amazon error descriptions → handler key
_ERR_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'title.{0,40}(exceed|too long|length|character)', re.I), 'title_length'),
    (re.compile(r'bullet.{0,40}(exceed|too long|length|character)', re.I), 'bullet_length'),
    (re.compile(r'description.{0,40}(exceed|too long|length|character)', re.I), 'desc_length'),
    (re.compile(r'search.{0,20}(exceed|too long|byte)',              re.I), 'search_length'),
    (re.compile(r'(html|<[a-z])',                                    re.I), 'html_in_desc'),
    (re.compile(r'(invalid|prohibited|illegal).{0,20}character',     re.I), 'bad_chars'),
    (re.compile(r'search.{0,30}(prohibited|restricted).{0,30}term',  re.I), 'bad_search_terms'),
]

# Amazon's documented field limits
_LIMITS = {
    'title':       200,
    'bullet':      500,
    'description': 2000,
    'search_bytes': 249,
}

# Terms prohibited in search terms per Amazon policy
_PROHIBITED_SEARCH = {
    'best', 'top', 'guaranteed', 'free', 'sale', 'discount', 'cheap',
    'amazing', 'wonderful', 'awesome', 'great', 'good', 'the', 'a', 'an',
}

_HTML_TAG_RE     = re.compile(r'<[^>]+>')
_CTRL_CHAR_RE    = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')
_AMP_RE          = re.compile(r'&(?!amp;|lt;|gt;|quot;|apos;)')


def _classify_error(error_msg: str) -> list[str]:
    """Return list of handler keys that match an Amazon error message."""
    matched = []
    for pattern, key in _ERR_PATTERNS:
        if pattern.search(error_msg):
            matched.append(key)
    return matched


def _truncate_to_words(text: str, max_len: int) -> str:
    """Trim *text* to at most *max_len* chars, cutting at a word boundary."""
    if len(text) <= max_len:
        return text
    truncated = text[:max_len]
    # Step back to last space to avoid cutting mid-word
    last_space = truncated.rfind(' ')
    return truncated[:last_space].rstrip() if last_space > 0 else truncated


def _truncate_to_bytes(text: str, max_bytes: int) -> str:
    """Trim *text* so its UTF-8 encoding is at most *max_bytes* bytes."""
    encoded = text.encode('utf-8')
    if len(encoded) <= max_bytes:
        return text
    # Trim words from the right until we're within budget
    words = text.split()
    while words:
        candidate = ' '.join(words)
        if len(candidate.encode('utf-8')) <= max_bytes:
            return candidate
        words.pop()
    return ''


def _strip_html(text: str) -> str:
    return _HTML_TAG_RE.sub('', text).strip()


def _strip_bad_chars(text: str) -> str:
    text = _CTRL_CHAR_RE.sub('', text)
    text = _AMP_RE.sub('&amp;', text)
    return text


def _remove_prohibited_search_terms(terms: str) -> str:
    words   = terms.split()
    cleaned = [w for w in words if w.lower() not in _PROHIBITED_SEARCH]
    return ' '.join(cleaned)


class SelfCorrector:
    """
    Apply rule-based corrections to a CSV row based on Amazon error messages.

    Usage
    -----
        corrector = SelfCorrector()
        fixed_row, corrections = corrector.correct(row, error_msg)
        if corrections:
            # row was changed — resubmit
    """

    def __init__(self) -> None:
        self.total_corrections = 0

    def correct(self, row: dict, error_msg: str) -> tuple[dict, list[str]]:
        """
        Return (fixed_row, corrections_applied).
        *row* is never mutated; a shallow copy is returned.
        If no corrections apply returns (row, []).
        """
        keys     = _classify_error(error_msg)
        if not keys:
            return row, []

        fixed        = dict(row)
        applied: list[str] = []

        for key in keys:
            if key == 'title_length':
                orig = fixed.get('new_title', '')
                fixed['new_title'] = _truncate_to_words(
                    _strip_bad_chars(orig), _LIMITS['title']
                )
                if fixed['new_title'] != orig:
                    applied.append(f'title truncated to {len(fixed["new_title"])} chars')

            elif key == 'bullet_length':
                for i in range(1, 6):
                    k    = f'bullet{i}'
                    orig = fixed.get(k, '')
                    fixed[k] = _truncate_to_words(orig, _LIMITS['bullet'])
                    if fixed[k] != orig:
                        applied.append(f'bullet{i} truncated')

            elif key == 'desc_length':
                orig = fixed.get('description', '')
                fixed['description'] = _truncate_to_words(orig, _LIMITS['description'])
                if fixed['description'] != orig:
                    applied.append('description truncated')

            elif key == 'search_length':
                orig = fixed.get('backend_search_terms', '')
                fixed['backend_search_terms'] = _truncate_to_bytes(
                    orig, _LIMITS['search_bytes']
                )
                if fixed['backend_search_terms'] != orig:
                    applied.append('search terms trimmed to 249 bytes')

            elif key == 'html_in_desc':
                for field in ['description'] + [f'bullet{i}' for i in range(1, 6)]:
                    orig = fixed.get(field, '')
                    fixed[field] = _strip_html(orig)
                    if fixed[field] != orig:
                        applied.append(f'HTML stripped from {field}')

            elif key == 'bad_chars':
                for field in (
                    ['new_title', 'description', 'backend_search_terms']
                    + [f'bullet{i}' for i in range(1, 6)]
                ):
                    orig = fixed.get(field, '')
                    fixed[field] = _strip_bad_chars(orig)
                    if fixed[field] != orig:
                        applied.append(f'bad chars stripped from {field}')

            elif key == 'bad_search_terms':
                orig = fixed.get('backend_search_terms', '')
                fixed['backend_search_terms'] = _remove_prohibited_search_terms(orig)
                if fixed['backend_search_terms'] != orig:
                    applied.append('prohibited search terms removed')

        self.total_corrections += len(applied)
        return fixed, applied


# ═══════════════════════════════════════════════════════════════════════════════
# 8. CHECKPOINT
# ═══════════════════════════════════════════════════════════════════════════════

def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        try:
            return json.loads(CHECKPOINT_FILE.read_text(encoding='utf-8'))
        except (json.JSONDecodeError, OSError):
            pass
    return {'completed_skus': [], 'batches': []}


def save_checkpoint(data: dict) -> None:
    try:
        CHECKPOINT_FILE.write_text(
            json.dumps(data, indent=2), encoding='utf-8'
        )
    except OSError as exc:
        print(f'  [WARN] Could not save checkpoint: {exc}')


# ═══════════════════════════════════════════════════════════════════════════════
# 9. RESULTS WRITER
# ═══════════════════════════════════════════════════════════════════════════════

def open_results_writer(path: Path):
    """Open the results CSV and return (file_handle, DictWriter)."""
    fh = open(path, 'w', newline='', encoding='utf-8')
    w  = csv.DictWriter(fh, fieldnames=RESULTS_FIELDNAMES)
    w.writeheader()
    fh.flush()
    return fh, w


def write_result(
    writer: 'csv.DictWriter',
    fh,
    sku:         str,
    asin:        str,
    status:      str,
    feed_id:     str = '',
    error:       str = '',
    corrections: str = '',
) -> None:
    writer.writerow({
        'sku':         sku,
        'asin':        asin,
        'status':      status,
        'feed_id':     feed_id,
        'error_message': error,
        'corrections': corrections,
    })
    fh.flush()


# ═══════════════════════════════════════════════════════════════════════════════
# 10. INPUT LOADER
# ═══════════════════════════════════════════════════════════════════════════════

def find_input_file(path_arg: str = '') -> Path:
    """Return the feed CSV path: explicit arg > most-recent auto-detected."""
    if path_arg:
        p = Path(path_arg)
        if not p.exists():
            print(f'[ERROR] Input file not found: {p}')
            sys.exit(1)
        return p

    pattern = str(SCRIPT_DIR / 'pc_amazon_feed_v4_*.csv')
    matches = sorted(glob.glob(pattern))
    if not matches:
        print(f'[ERROR] No pc_amazon_feed_v4_*.csv found in {SCRIPT_DIR}')
        print('  Run pc_listing_optimizer_v4.py first, or pass --input.')
        sys.exit(1)
    return Path(matches[-1])   # alphabetical order == chronological for RUN_ID timestamps


def load_rows(csv_path: Path, status_filter: str, limit: int = 0) -> list[dict]:
    """
    Read the feed CSV and return rows that pass the status filter.

    Safety rule: rows with listing_status == 'Incomplete' are NEVER returned
    unless --status Incomplete was explicitly passed.
    """
    rows: list[dict] = []
    with open(csv_path, newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # listing_status carries the original Amazon status (Active/Inactive/Incomplete)
            # Fall back to 'status' column if listing_status is absent
            listing_status = (
                row.get('listing_status', '') or row.get('status', '')
            ).strip()

            # Enforce safety rule before any other check
            if (
                listing_status.lower() == 'incomplete'
                and status_filter.lower() != 'incomplete'
            ):
                continue

            if status_filter.lower() != 'all':
                if listing_status.lower() != status_filter.lower():
                    continue

            sku = (row.get('sku', '') or '').strip()
            if not sku:
                continue

            rows.append(row)
            if limit and len(rows) >= limit:
                break

    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# 11. CLI
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description='Push optimized Amazon listings via SP-API Feeds API.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        '--status',
        choices=['Active', 'Inactive', 'Incomplete', 'All'],
        default='Active',
        help='Filter rows by listing_status column (default: Active)',
    )
    p.add_argument(
        '--input', metavar='PATH', default='',
        help='Feed CSV path (default: most recent pc_amazon_feed_v4_*.csv)',
    )
    p.add_argument(
        '--dry-run', action='store_true',
        help='Build + validate XML without submitting to Amazon',
    )
    p.add_argument(
        '--limit', type=int, default=0, metavar='N',
        help='Process at most N listings (useful for testing)',
    )
    p.add_argument(
        '--seller-id', default='', metavar='ID',
        help='Amazon Seller ID (overrides SP_API_SELLER_ID env var)',
    )
    p.add_argument(
        '--no-self-correct', action='store_true',
        help='Disable automatic error correction + retry pass',
    )
    return p.parse_args()


# ═══════════════════════════════════════════════════════════════════════════════
# 12. BATCH SUBMISSION HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def submit_batch(
    client:    SPAPIClient,
    batch:     list[dict],
    seller_id: str,
    label:     str,
) -> tuple[str, str, dict[str, str]]:
    """
    Run the full feed lifecycle for one batch.

    Returns
    -------
    feed_id         : str   — '' if submission failed before feed was created
    proc_status     : str   — processingStatus from Amazon ('DONE'/'FATAL'/...)
    sku_errors      : dict  — {sku: error_message} from the processing report
    Raises on unrecoverable errors so the caller can catch and log.
    """
    # 1. Create feed document
    print(f'    {label} Creating feed document …', end=' ', flush=True)
    feed_doc   = client.create_feed_document()
    doc_id     = feed_doc['feedDocumentId']
    upload_url = feed_doc['url']
    print(f'OK ({doc_id})')

    # 2. Build + upload XML
    xml_bytes = build_feed_xml(batch, seller_id)
    print(f'    {label} Uploading XML ({len(xml_bytes):,} bytes) …', end=' ', flush=True)
    client.upload_feed(upload_url, xml_bytes)
    print('OK')

    # 3. Submit feed
    print(f'    {label} Submitting feed …', end=' ', flush=True)
    feed_id = client.create_feed(doc_id)
    print(f'OK  feedId={feed_id}')

    # 4. Poll
    print(f'    {label} Polling (timeout {POLL_TIMEOUT}s) …')
    final   = client.poll_until_done(feed_id)
    ps      = final.get('processingStatus', 'UNKNOWN')
    print(f'    {label} Processing status: {ps}')

    # 5. Fetch processing report
    sku_errors: dict[str, str] = {}
    result_doc_id = final.get('resultFeedDocumentId', '')
    if result_doc_id and ps == 'DONE':
        print(f'    {label} Fetching processing report …', end=' ', flush=True)
        try:
            result_xml = client.get_result_document(result_doc_id)
            sku_errors  = parse_processing_report(result_xml)
            print(f'OK  ({len(sku_errors)} error/warning(s))')
        except Exception as exc:                        # non-fatal — log and continue
            print(f'WARN: {exc}')

    return feed_id, ps, sku_errors


# ═══════════════════════════════════════════════════════════════════════════════
# 13. MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    args = parse_args()

    # ── Banner ─────────────────────────────────────────────────────────────────
    print('\n' + '═' * 64)
    print('  PC Amazon SP-API Push Tool')
    print(f'  Run ID  : {RUN_ID}')
    print(f'  Status  : {args.status}')
    print(f'  Mode    : {"DRY RUN — nothing sent to Amazon" if args.dry_run else "LIVE"}')
    print(f'  Self-correct: {"disabled" if args.no_self_correct else "enabled"}')
    print('═' * 64)

    # ── Credentials & input ────────────────────────────────────────────────────
    creds    = load_credentials(args.seller_id)
    csv_path = find_input_file(args.input)
    print(f'\n  Seller : {creds["seller_id"]}')
    print(f'  Input  : {csv_path.name}')

    rows = load_rows(csv_path, args.status, args.limit)
    if not rows:
        print(f'\n  No rows with listing_status={args.status!r}. Nothing to push.')
        return

    total   = len(rows)
    batches = [rows[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    print(f'\n  Loaded {total} listing(s) → {len(batches)} batch(es) of ≤{BATCH_SIZE}')

    # ── Resume from checkpoint ─────────────────────────────────────────────────
    checkpoint     = load_checkpoint()
    completed_skus = set(checkpoint.get('completed_skus', []))
    if completed_skus:
        print(f'  Resuming: {len(completed_skus)} SKU(s) already done — skipping.')

    filtered_batches: list[list[dict]] = []
    skipped_count = 0
    for batch in batches:
        remaining = [r for r in batch if (r.get('sku', '') or '').strip() not in completed_skus]
        skipped_count += len(batch) - len(remaining)
        if remaining:
            filtered_batches.append(remaining)

    if skipped_count:
        print(f'  Skipped {skipped_count} already-completed SKU(s).')

    if not filtered_batches:
        print('\n  All listings already pushed. Delete pc_push_checkpoint.json to re-push.')
        return

    # ── Prepare writer & state ─────────────────────────────────────────────────
    client     = SPAPIClient(creds, dry_run=args.dry_run)
    corrector  = SelfCorrector()
    results_fh, results_writer = open_results_writer(RESULTS_FILE)
    print(f'\n  Results → {RESULTS_FILE.name}')
    print(f'\n{"─" * 64}')

    stats = {
        'success':         0,
        'warning':         0,
        'error':           0,
        'auto_corrected':  0,
        'feeds_submitted': 0,
    }

    try:
        for batch_num, batch in enumerate(filtered_batches, start=1):
            batch_skus = [(r.get('sku', '') or '').strip() for r in batch]
            sku_range  = f'{batch_skus[0]} … {batch_skus[-1]}' if len(batch_skus) > 1 else batch_skus[0]
            print(f'\n  Batch {batch_num}/{len(filtered_batches)}  '
                  f'({len(batch)} listing(s):  {sku_range})')

            # ── Validate XML can be built ──────────────────────────────────────
            try:
                build_feed_xml(batch, creds['seller_id'])   # dry-validate
            except Exception as exc:
                print(f'    [ERROR] XML build failed: {exc}')
                for row in batch:
                    write_result(results_writer, results_fh,
                                 (row.get('sku', '') or '').strip(),
                                 (row.get('asin', '') or '').strip(),
                                 'error', error=f'XML build: {exc}')
                    stats['error'] += 1
                continue

            # ── Dry run ────────────────────────────────────────────────────────
            if args.dry_run:
                xml_bytes = build_feed_xml(batch, creds['seller_id'])
                if batch_num == 1:
                    preview = xml_bytes[:2000].decode('utf-8', errors='replace')
                    print('\n  [DRY RUN] XML preview (first 2000 bytes):')
                    for line in preview.splitlines():
                        print(f'    {line}')
                for row in batch:
                    write_result(results_writer, results_fh,
                                 (row.get('sku', '') or '').strip(),
                                 (row.get('asin', '') or '').strip(),
                                 'dry_run')
                    stats['success'] += 1
                print(f'    [DRY RUN] Batch {batch_num} validated — not submitted.')
                continue

            # ── Live submission ────────────────────────────────────────────────
            feed_id = ''
            ps      = 'UNKNOWN'
            sku_errors: dict[str, str] = {}

            try:
                feed_id, ps, sku_errors = submit_batch(
                    client, batch, creds['seller_id'],
                    label=f'[{batch_num}/{len(filtered_batches)}]',
                )
                stats['feeds_submitted'] += 1
            except Exception as exc:
                print(f'    [ERROR] Batch {batch_num} submission failed: {exc}')
                for row in batch:
                    write_result(results_writer, results_fh,
                                 (row.get('sku', '') or '').strip(),
                                 (row.get('asin', '') or '').strip(),
                                 'error', feed_id=feed_id,
                                 error=str(exc))
                    stats['error'] += 1
                continue

            # ── Non-DONE terminal states ───────────────────────────────────────
            if ps in ('FATAL', 'CANCELLED'):
                for row in batch:
                    write_result(results_writer, results_fh,
                                 (row.get('sku', '') or '').strip(),
                                 (row.get('asin', '') or '').strip(),
                                 ps.lower(), feed_id=feed_id,
                                 error=f'Feed processingStatus={ps}')
                    stats['error'] += 1
                continue

            # ── Self-correction pass ───────────────────────────────────────────
            # Partition batch into clean SKUs and errored SKUs
            errored_rows:    list[dict] = []
            corrected_rows:  list[dict] = []   # fixed versions for retry
            correction_map:  dict[str, list[str]] = {}   # sku → corrections applied

            if sku_errors and not args.no_self_correct:
                print(f'    [SELF-CORRECT] Attempting fixes for '
                      f'{len(sku_errors)} errored SKU(s) …')
                for row in batch:
                    sku = (row.get('sku', '') or '').strip()
                    if sku in sku_errors:
                        fixed, applied = corrector.correct(row, sku_errors[sku])
                        if applied:
                            corrected_rows.append(fixed)
                            correction_map[sku] = applied
                            errored_rows.append(row)
                            print(f'      {sku}: {"; ".join(applied)}')
                        # else: non-fixable, will be logged below

            # ── Retry corrected SKUs ───────────────────────────────────────────
            retry_results: dict[str, str] = {}   # sku → 'success' | error_msg

            if corrected_rows:
                print(f'    [RETRY] Resubmitting {len(corrected_rows)} corrected SKU(s) …')
                try:
                    r_feed_id, r_ps, r_errors = submit_batch(
                        client, corrected_rows, creds['seller_id'],
                        label=f'[{batch_num}/{len(filtered_batches)}-RETRY]',
                    )
                    stats['feeds_submitted'] += 1
                    for row in corrected_rows:
                        sku = (row.get('sku', '') or '').strip()
                        if sku in r_errors:
                            retry_results[sku] = r_errors[sku]
                        else:
                            retry_results[sku] = 'success'
                except Exception as exc:
                    print(f'    [ERROR] Retry batch failed: {exc}')
                    for row in corrected_rows:
                        sku = (row.get('sku', '') or '').strip()
                        retry_results[sku] = f'retry failed: {exc}'

            # ── Record per-SKU results ─────────────────────────────────────────
            for row in batch:
                sku  = (row.get('sku', '') or '').strip()
                asin = (row.get('asin', '') or '').strip()
                err  = sku_errors.get(sku, '')

                if not err:
                    # No error reported → success
                    write_result(results_writer, results_fh, sku, asin,
                                 'success', feed_id=feed_id)
                    stats['success'] += 1

                elif sku in retry_results:
                    # Was corrected and retried
                    retry_outcome = retry_results[sku]
                    corr_str      = '; '.join(correction_map.get(sku, []))
                    if retry_outcome == 'success':
                        write_result(results_writer, results_fh, sku, asin,
                                     'success_after_correction',
                                     feed_id=feed_id,
                                     corrections=corr_str)
                        stats['success']        += 1
                        stats['auto_corrected'] += 1
                    else:
                        write_result(results_writer, results_fh, sku, asin,
                                     'error', feed_id=feed_id,
                                     error=retry_outcome,
                                     corrections=corr_str)
                        stats['error'] += 1
                        print(f'    [ERROR] {sku} (after correction): {retry_outcome}')

                else:
                    # Error but not fixable (or self-correct disabled)
                    result_status = 'warning' if 'Warning' in err else 'error'
                    write_result(results_writer, results_fh, sku, asin,
                                 result_status, feed_id=feed_id, error=err)
                    if result_status == 'warning':
                        stats['warning'] += 1
                        print(f'    [WARN]  {sku}: {err}')
                    else:
                        stats['error'] += 1
                        print(f'    [ERROR] {sku}: {err}')

            # ── Checkpoint ────────────────────────────────────────────────────
            checkpoint['completed_skus'] = list(
                set(checkpoint.get('completed_skus', [])) | set(batch_skus)
            )
            checkpoint.setdefault('batches', []).append({
                'batch_num': batch_num,
                'feed_id':   feed_id,
                'skus':      batch_skus,
                'status':    ps,
            })
            save_checkpoint(checkpoint)
            print(f'    Checkpoint saved  ({len(checkpoint["completed_skus"])} total SKUs done)')

    finally:
        results_fh.close()

    # ── Summary ────────────────────────────────────────────────────────────────
    total_processed = stats['success'] + stats['warning'] + stats['error']
    print(f'\n{"═" * 64}')
    print('  PUSH COMPLETE')
    print(f'  Listings processed : {total_processed}')
    print(f'  Success            : {stats["success"]}')
    print(f'  Warnings           : {stats["warning"]}')
    print(f'  Errors             : {stats["error"]}')
    print(f'  Auto-corrected     : {stats["auto_corrected"]}')
    print(f'  Feeds submitted    : {stats["feeds_submitted"]}')
    print(f'\n  Results  → {RESULTS_FILE.name}')
    print(f'  Checkpoint → {CHECKPOINT_FILE.name}')
    print('═' * 64 + '\n')


if __name__ == '__main__':
    main()
