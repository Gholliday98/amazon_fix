#!/usr/bin/env python3
"""
pc_image_audit.py — Count and list all image-related errors from the issues CSV.

Usage:
    python pc_image_audit.py --issues pc_issues_20260506_124012.csv
    python pc_image_audit.py --issues pc_issues_20260506_124012.csv --export
"""

import argparse
import csv
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent

# All Amazon error codes related to images
IMAGE_CODES = {
    '18320':  'Broken/invalid image URL (host migration)',
    '300060': 'Main image missing or invalid',
    '300403': 'Image does not meet requirements',
    '300404': 'Image URL not accessible',
    '300429': 'Image failed quality check',
    '8506':   'No image submitted',
    '100008': 'Invalid image URL format',
}


def main():
    ap = argparse.ArgumentParser(description='Audit image errors from issues CSV.')
    ap.add_argument('--issues', required=True, metavar='FILE',
                    help='Issues CSV from pc_pull_issues.py')
    ap.add_argument('--export', action='store_true',
                    help='Export the list of affected SKUs to pc_image_errors.csv')
    args = ap.parse_args()

    issues_path = Path(args.issues)
    if not issues_path.is_absolute():
        issues_path = SCRIPT_DIR / issues_path
    if not issues_path.exists():
        print(f'[ERROR] File not found: {issues_path}')
        raise SystemExit(1)

    # {code: {sku: asin}}
    by_code: dict[str, dict[str, str]] = defaultdict(dict)
    all_skus: dict[str, str] = {}  # sku -> asin

    with open(issues_path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            code = (row.get('code') or '').strip()
            if code not in IMAGE_CODES:
                continue
            sku  = (row.get('sku') or '').strip()
            asin = (row.get('asin') or '').strip().upper()
            if sku:
                by_code[code][sku] = asin
                all_skus[sku] = asin

    print(f'\n{"=" * 58}')
    print('  IMAGE ERROR AUDIT')
    print(f'  Source: {issues_path.name}')
    print(f'{"=" * 58}\n')

    if not all_skus:
        print('  No image errors found in this issues file.')
        print(f'{"=" * 58}\n')
        return

    for code, label in IMAGE_CODES.items():
        skus = by_code.get(code, {})
        if skus:
            print(f'  Code {code} — {label}')
            print(f'  {len(skus)} SKU(s)\n')

    unique_skus = len(all_skus)
    print(f'{"─" * 58}')
    print(f'  TOTAL unique SKUs with image errors : {unique_skus}')
    print(f'{"=" * 58}\n')

    if args.export:
        out = SCRIPT_DIR / 'pc_image_errors.csv'
        with open(out, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(['sku', 'asin', 'error_codes'])
            for sku, asin in sorted(all_skus.items()):
                codes = ','.join(c for c in IMAGE_CODES if sku in by_code.get(c, {}))
                w.writerow([sku, asin, codes])
        print(f'  Exported {unique_skus} SKUs to pc_image_errors.csv\n')


if __name__ == '__main__':
    main()
