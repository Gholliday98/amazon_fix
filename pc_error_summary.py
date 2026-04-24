#!/usr/bin/env python3
"""
pc_error_summary.py — Summarize all push errors grouped by error code.

Reads all pc_push_v2_results_*.csv files and produces a report grouping
affected ASINs by error type — ready to use for Amazon Seller Central cases.

Usage:
    python pc_error_summary.py
    python pc_error_summary.py --output my_summary.txt
"""

import argparse
import csv
import glob
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent

# Human-readable labels for known Amazon error codes
ERROR_LABELS = {
    '90225': 'Field value too long (size_description exceeds 50 characters)',
    '99010': 'Prohibited content — listing policy violation (pesticide / antimicrobial claim)',
    '99300': 'Prohibited content — listing policy violation (unverified claim or restricted language)',
    '8058':  'Listing suppressed — policy violation',
    '8541':  'Duplicate ASIN / catalog matching conflict',
    '8560':  'Invalid attribute value',
    '5461':  'SKU not found / listing does not exist',
    '6023':  'Listing inactive or suppressed',
}

# Group similar error codes into case buckets
CASE_BUCKETS = {
    'Pesticide / Antimicrobial Policy (99010)': ['99010'],
    'Prohibited / Restricted Content (99300)': ['99300'],
    'Field Too Long — Size Description (90225)': ['90225'],
    'Listing Suppressed / Policy Violation (8058 / 6023)': ['8058', '6023'],
    'Catalog / Duplicate ASIN Issue (8541)': ['8541'],
    'Invalid Attribute (8560)': ['8560'],
    'SKU Not Found (5461)': ['5461'],
}


def normalize_code(raw: str) -> str:
    """Extract just the numeric error code from messages like '90225: Value...'"""
    m = re.match(r'(\d+)', (raw or '').strip())
    return m.group(1) if m else (raw or '').strip()


def main():
    ap = argparse.ArgumentParser(description='Summarize push errors by type for Amazon cases.')
    ap.add_argument('--output', metavar='FILE', help='Output file (default: print to screen)')
    ap.add_argument('--latest-only', action='store_true',
                    help='Only read the most recent push results file')
    args = ap.parse_args()

    files = sorted(
        glob.glob(str(SCRIPT_DIR / 'pc_push_v2_results_*.csv')) +
        glob.glob(str(SCRIPT_DIR / 'pc_push_results_*.csv')),
        key=os.path.getmtime,
        reverse=True
    )

    if not files:
        print('[ERROR] No push result CSVs found.')
        raise SystemExit(1)

    if args.latest_only:
        files = [files[0]]

    print(f'  Reading {len(files)} push result file(s)...')

    # Track: per ASIN, collect all unique error codes seen
    # Use most recent error per ASIN (files sorted newest first)
    asin_errors  = defaultdict(set)   # asin -> set of error codes
    asin_skus    = {}                  # asin -> sku
    seen_asins   = set()

    for fpath in files:
        with open(fpath, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                status = (row.get('status') or '').strip().lower()
                asin   = (row.get('asin') or '').strip().upper()
                sku    = (row.get('sku') or '').strip()
                code   = normalize_code(row.get('error_code') or '')

                if not asin or status != 'error' or not code:
                    continue

                asin_errors[asin].add(code)
                if asin not in asin_skus:
                    asin_skus[asin] = sku

    if not asin_errors:
        print('\n  No errors found across all push result files.')
        return

    # Group ASINs into case buckets
    bucket_asins = defaultdict(set)
    uncategorized = defaultdict(set)  # code -> set of asins

    for asin, codes in asin_errors.items():
        placed = False
        for bucket_name, bucket_codes in CASE_BUCKETS.items():
            if any(c in codes for c in bucket_codes):
                bucket_asins[bucket_name].add(asin)
                placed = True
        if not placed:
            for code in codes:
                uncategorized[code].add(asin)

    lines = []
    lines.append('=' * 70)
    lines.append('  AMAZON PUSH ERROR SUMMARY — CASE FILING GUIDE')
    lines.append(f'  Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append(f'  Source files: {len(files)}')
    lines.append(f'  Total affected ASINs: {len(asin_errors)}')
    lines.append('=' * 70)

    for bucket_name, asins in sorted(bucket_asins.items(), key=lambda x: -len(x[1])):
        asin_list = sorted(asins)
        lines.append(f'\n{"─" * 70}')
        lines.append(f'  ISSUE: {bucket_name}')
        lines.append(f'  COUNT: {len(asin_list)} ASIN(s)')
        lines.append(f'{"─" * 70}')
        lines.append('  ASINs:')
        lines.append('  ' + ', '.join(asin_list))
        lines.append('')
        lines.append('  CASE SUBJECT:')
        lines.append(f'  Listing Reinstatement Request — {bucket_name}')
        lines.append('')
        lines.append('  WHERE TO FILE:')
        lines.append('  Seller Central → Help → Contact Us → Products & Inventory')
        lines.append('  → "Fix a Product page" or "My listing was removed"')

    if uncategorized:
        lines.append(f'\n{"─" * 70}')
        lines.append('  OTHER ERRORS (review manually)')
        lines.append(f'{"─" * 70}')
        for code, asins in sorted(uncategorized.items()):
            lines.append(f'  Code {code}: {len(asins)} ASIN(s)')
            lines.append('  ' + ', '.join(sorted(asins)))

    lines.append(f'\n{"=" * 70}')

    output = '\n'.join(lines)

    if args.output:
        out_path = SCRIPT_DIR / args.output
        out_path.write_text(output, encoding='utf-8')
        print(f'\n  Summary written to: {args.output}')
    else:
        print('\n' + output)


if __name__ == '__main__':
    main()
