#!/usr/bin/env python3
"""
pc_combine_feeds.py — Combine inactive listings + push errors into one optimizer feed.

Usage:
    python pc_combine_feeds.py --inactive "C:\path\to\Inactive+Listings+Report.txt"
"""

import argparse
import csv
import glob
import os
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RUN_ID     = datetime.now().strftime('%Y%m%d_%H%M%S')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--inactive', metavar='FILE', help='Inactive Listings Report from Seller Central')
    ap.add_argument('--output',   metavar='FILE', help='Output filename (auto-named if omitted)')
    args = ap.parse_args()

    target_skus = {}

    # ── 1. Push errors from results CSVs ──────────────────────────────────────
    result_files = sorted(
        glob.glob(str(SCRIPT_DIR / 'pc_push_results_*.csv')) +
        glob.glob(str(SCRIPT_DIR / 'pc_push_v2_results_*.csv')),
        key=os.path.getmtime
    )
    error_count = 0
    for fpath in result_files:
        with open(fpath, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                sku    = (row.get('sku') or '').strip()
                status = (row.get('status') or '').strip().lower()
                if sku and status == 'error':
                    target_skus[sku] = 'push_error'
                    error_count += 1
    print(f'Push errors       : {error_count} (from {len(result_files)} result files)')

    # ── 2. Inactive listings report ───────────────────────────────────────────
    inactive_count = 0
    if args.inactive:
        p = Path(args.inactive)
        with open(p, newline='', encoding='utf-8-sig', errors='replace') as f:
            for row in csv.DictReader(f, delimiter='\t'):
                sku = (row.get('seller-sku') or '').strip()
                if sku:
                    target_skus[sku] = target_skus.get(sku, 'inactive')
                    inactive_count += 1
        print(f'Inactive SKUs     : {inactive_count} (from {p.name})')

    print(f'Total unique SKUs : {len(target_skus)}')

    if not target_skus:
        print('Nothing to do.')
        return

    # ── 3. Find All Listings Report ───────────────────────────────────────────
    all_reports = (
        glob.glob(str(SCRIPT_DIR / '*All*Listings*Report*.txt')) +
        glob.glob(str(SCRIPT_DIR / '*All_Listings*.txt')) +
        glob.glob(str(SCRIPT_DIR / '*.txt'))
    )
    if not all_reports:
        print('[ERROR] No All Listings Report .txt found in the amazon-fix folder.')
        return
    report_path = Path(max(all_reports, key=os.path.getmtime))
    print(f'All Listings file : {report_path.name}')

    # ── 4. Filter and write ───────────────────────────────────────────────────
    output_path = Path(args.output) if args.output else SCRIPT_DIR / f'pc_error_feed_{RUN_ID}.txt'
    written = 0
    with open(report_path, 'r', encoding='utf-8-sig', errors='replace') as f:
        reader     = csv.DictReader(f, delimiter='\t')
        fieldnames = reader.fieldnames or []
        with open(output_path, 'w', newline='', encoding='utf-8') as out:
            writer = csv.DictWriter(out, fieldnames=fieldnames, delimiter='\t',
                                    extrasaction='ignore')
            writer.writeheader()
            found = set()
            for row in reader:
                sku = (row.get('seller-sku') or '').strip()
                if sku in target_skus:
                    writer.writerow(row)
                    found.add(sku)
                    written += 1

    missing = len(target_skus) - len(found)
    if missing:
        print(f'[WARN] {missing} SKUs not found in All Listings Report (may be deleted)')

    print(f'\nRows written : {written}')
    print(f'Output file  : {output_path.name}')
    print()
    print('Next step — update optimizer INPUT_FILE:')
    print(f"    INPUT_FILE = str(SCRIPT_DIR / '{output_path.name}')")
    print('Then run: python "pc_listing_optimizer_v4 (5).py"')


if __name__ == '__main__':
    main()
