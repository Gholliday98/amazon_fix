#!/usr/bin/env python3
"""
pc_scrape_oem_images.py — Scrape OEM cutting board images from manufacturer/parts sites.

Reads the suppressed listings CSV, extracts the manufacturer name and dimensions
from each title, searches partstown.com (primary) and webstaurantstore.com (fallback)
for a product image, and saves it as {SKU}.jpg in the output folder.

Usage
-----
    py pc_scrape_oem_images.py
    py pc_scrape_oem_images.py --csv suppressed_listings_20260521_115921.csv
    py pc_scrape_oem_images.py --out "X:\\Gabrielle\\cutting_board_oem_pics"

Output
------
    X:\\Gabrielle\\cutting_board_oem_pics\\{SKU}.jpg   — downloaded image
    X:\\Gabrielle\\cutting_board_oem_pics\\results.csv — log of found/not found
"""

import argparse
import csv
import io
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote_plus

try:
    import requests
except ImportError:
    print('[ERROR] requests not installed. Run: pip install requests')
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print('[ERROR] beautifulsoup4 not installed. Run: pip install beautifulsoup4')
    sys.exit(1)

# ─── Defaults ─────────────────────────────────────────────────────────────────
SCRIPT_DIR  = Path(__file__).parent
DEFAULT_OUT = Path(r'X:\Gabrielle\cutting_board_oem_pics')
DEFAULT_CSV = None   # auto-detect latest suppressed_listings_*.csv

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}
REQUEST_TIMEOUT = 20
DELAY_BETWEEN   = 2.5   # seconds between requests — be polite


# ═══════════════════════════════════════════════════════════════════════════════
# Title parsing
# ═══════════════════════════════════════════════════════════════════════════════

MANUFACTURER_ALIASES = {
    'commercial equipment': 'commercial equipment',
    'master':               'master-bilt',
    'turbo air':            'turbo air',
    'beverage air':         'beverage air',
    'beverage-air':         'beverage air',
    'henny penny':          'henny penny',
    'norlake':              'norlake',
    'continental':          'continental refrigerator',
    'true':                 'true',
    'delfield':             'delfield',
    'fagor':                'fagor',
    'victory':              'victory refrigeration',
    'randell':              'randell',
    'garland':              'garland',
    'lang':                 'lang',
}

def parse_title(title: str) -> tuple[str, str]:
    """Return (manufacturer, dimensions_string) from a listing title."""
    m = re.search(r'OEM Replacement for ([^,]+),\s*(.+)', title, re.IGNORECASE)
    if not m:
        return '', title
    raw_mfr = m.group(1).strip()
    dims    = m.group(2).strip()
    # Normalise manufacturer name
    lower = raw_mfr.lower()
    for key, val in MANUFACTURER_ALIASES.items():
        if key in lower:
            return val, dims
    return raw_mfr, dims


def build_search_query(manufacturer: str, dims: str) -> str:
    # Strip parenthetical decimal equivalents like (19.5") to keep query clean
    clean_dims = re.sub(r'\([^)]+\)', '', dims).strip().rstrip(',').strip()
    return f'{manufacturer} cutting board {clean_dims}'


# ═══════════════════════════════════════════════════════════════════════════════
# Image search — partstown.com
# ═══════════════════════════════════════════════════════════════════════════════

def search_partstown(session: requests.Session, query: str) -> str | None:
    url = f'https://www.partstown.com/search?q={quote_plus(query)}'
    try:
        r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if not r.ok:
            return None
        soup = BeautifulSoup(r.text, 'html.parser')
        # Product cards on partstown have images inside .product-card or similar
        for img in soup.select('img[src]'):
            src = img.get('src', '')
            if any(kw in src.lower() for kw in ['product', 'part', 'catalog']):
                if src.startswith('//'):
                    src = 'https:' + src
                if src.startswith('http'):
                    return src
        # Broader fallback: any non-icon image
        for img in soup.select('img[src]'):
            src = img.get('src', '')
            if src.startswith('//'):
                src = 'https:' + src
            if (src.startswith('http') and
                    not any(x in src.lower() for x in ['logo', 'icon', 'banner', 'sprite', 'svg'])):
                return src
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Image search — webstaurantstore.com
# ═══════════════════════════════════════════════════════════════════════════════

def search_webstaurant(session: requests.Session, query: str) -> str | None:
    url = f'https://www.webstaurantstore.com/search/{quote_plus(query)}/'
    try:
        r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if not r.ok:
            return None
        soup = BeautifulSoup(r.text, 'html.parser')
        for img in soup.select('img.product-img, img[itemprop="image"], .product-card img'):
            src = img.get('src', img.get('data-src', ''))
            if src.startswith('//'):
                src = 'https:' + src
            if src.startswith('http') and 'cdnimg' in src:
                return src
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Download image
# ═══════════════════════════════════════════════════════════════════════════════

def download_image(session: requests.Session, url: str, dest: Path) -> bool:
    try:
        r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
        if not r.ok:
            return False
        content_type = r.headers.get('content-type', '')
        if 'image' not in content_type and not url.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
            return False
        dest.write_bytes(r.content)
        return True
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# Find latest suppressed listings CSV
# ═══════════════════════════════════════════════════════════════════════════════

def find_csv() -> Path | None:
    candidates = sorted(SCRIPT_DIR.glob('suppressed_listings_*.csv'), reverse=True)
    return candidates[0] if candidates else None


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description='Scrape OEM cutting board images.')
    parser.add_argument('--csv', help='Path to suppressed listings CSV')
    parser.add_argument('--out', default=str(DEFAULT_OUT),
                        help=r'Output folder (default: X:\Gabrielle\cutting_board_oem_pics)')
    args = parser.parse_args()

    csv_path = Path(args.csv) if args.csv else find_csv()
    if not csv_path or not csv_path.exists():
        print('[ERROR] Could not find suppressed listings CSV. Use --csv to specify one.')
        sys.exit(1)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f'Output folder: {out_dir}')
    print(f'Reading from:  {csv_path}\n')

    rows = list(csv.DictReader(csv_path.open(encoding='utf-8-sig')))
    total  = len(rows)
    found  = 0
    missed = []

    session = requests.Session()
    results = []

    for i, row in enumerate(rows, 1):
        sku   = row.get('sku', row.get('SKU', '')).strip()
        title = row.get('title', row.get('Product name', '')).strip()

        dest = out_dir / f'{sku}.jpg'
        if dest.exists():
            print(f'[{i:>3}/{total}] SKIP  {sku} (already downloaded)')
            found += 1
            results.append({'sku': sku, 'status': 'already exists', 'source': ''})
            continue

        manufacturer, dims = parse_title(title)
        query = build_search_query(manufacturer, dims)
        print(f'[{i:>3}/{total}] {sku}')
        print(f'         Searching: {query}')

        img_url = None
        source  = ''

        # Try partstown first
        img_url = search_partstown(session, query)
        if img_url:
            source = 'partstown.com'
        else:
            time.sleep(DELAY_BETWEEN)
            img_url = search_webstaurant(session, query)
            if img_url:
                source = 'webstaurantstore.com'

        if img_url:
            ok = download_image(session, img_url, dest)
            if ok:
                print(f'         SAVED  ({source})')
                found += 1
                results.append({'sku': sku, 'status': 'downloaded', 'source': source})
            else:
                print(f'         FAIL   could not download image')
                missed.append(sku)
                results.append({'sku': sku, 'status': 'download failed', 'source': img_url})
        else:
            print(f'         NOT FOUND on any source')
            missed.append(sku)
            results.append({'sku': sku, 'status': 'not found', 'source': ''})

        time.sleep(DELAY_BETWEEN)

    # Write results log
    log_path = out_dir / 'results.csv'
    with log_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['sku', 'status', 'source'])
        writer.writeheader()
        writer.writerows(results)

    print(f'\n{"="*50}')
    print(f'  Done. {found}/{total} images saved.')
    if missed:
        print(f'  {len(missed)} not found: {", ".join(missed)}')
    print(f'  Results log: {log_path}')


if __name__ == '__main__':
    main()
