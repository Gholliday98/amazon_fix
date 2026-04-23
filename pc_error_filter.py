#!/usr/bin/env python3
"""
pc_error_filter.py — Build a filtered optimizer input from push errors.

Reads every pc_push_results_*.csv and pc_push_v2_results_*.csv in the
same folder, collects every SKU that had an error or a non-empty
error_message, then pulls those rows from the original Amazon All
Listings Report and writes a new tab-delimited file the optimizer
can process directly.

Usage
-----
    python pc_error_filter.py                      # auto-detects listings report
    python pc_error_filter.py --report my_report.txt
    python pc_error_filter.py --results-dir C:\path\to\results
    python pc_error_filter.py --include-warnings   # also grab ASIN rematch warnings

Output
------
    pc_error_feed_YYYYMMDD_HHMMSS.txt  — tab-delimited, optimizer-ready
    (point the optimizer's INPUT_FILE at this file)
"""

import argparse
import csv
import glob
import os
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RUN_ID     = datetime.now().strftime('%Y%m%d_%H%M%S')


# ─── Find the Amazon All Listings Report ──────────────────────────────────────

def find_listings_report(hint: str = '') -> Path:
    if hint:
        p = Path(hint)
        if not p.is_absolute():
            p = SCRIPT_DIR / p
        if p.exists():
            return p
        print(f'[ERROR] Report not found: {p}')
        sys.exit(1)

    patterns = [
        '*_All_Listings_Report_*.txt',
        '*_All_Listings*.txt',
        '*listings*.txt',
        '*.txt',
    ]
    for pat in patterns:
        matches = glob.glob(str(SCRIPT_DIR / pat))
        if matches:
            chosen = Path(max(matches, key=os.path.getmtime))
            print(f'  Listings report : {chosen.name}')
            return chosen

    print('[ERROR] No Amazon All Listings Report (.txt) found.')
    print('        Re-download from Seller Central → Reports → Inventory → All Listings.')
    print('        Or pass --report <filename>')
    sys.exit(1)


# ─── Collect error SKUs from push results ─────────────────────────────────────

def collect_error_skus(results_dir: Path, include_warnings: bool) -> dict:
    """
    Returns {sku: [reason1, reason2, ...]} for every SKU that failed.
    Covers both v1 (pc_push_results_*.csv) and v2 (pc_push_v2_results_*.csv).
    """
    error_skus: dict = {}

    patterns = ['pc_push_results_*.csv', 'pc_push_v2_results_*.csv']
    result_files = []
    for pat in patterns:
        result_files.extend(glob.glob(str(results_dir / pat)))

    if not result_files:
        print(f'[ERROR] No push result CSVs found in {results_dir}')
        print('        Run pc_sp_api_push.py and/or pc_sp_api_push_v2.py first.')
        sys.exit(1)

    result_files.sort(key=os.path.getmtime)
    print(f'\n  Found {len(result_files)} result file(s):')
    for f in result_files:
        print(f'    {Path(f).name}')

    for filepath in result_files:
        with open(filepath, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                sku    = (row.get('sku', '') or '').strip()
                status = (row.get('status', '') or '').strip().lower()
                errmsg = (row.get('error_message', '') or '').strip()
                errcod = (row.get('error_code', '') or '').strip()

                if not sku:
                    continue

                reason = None
                if status == 'error':
                    reason = f'{errcod}: {errmsg}' if errcod else errmsg or 'unknown error'
                elif status == 'not_found':
                    reason = 'SKU not found on marketplace'
                elif include_warnings and errmsg:
                    reason = f'WARNING: {errmsg}'

                if reason:
                    error_skus.setdefault(sku, [])
                    if reason not in error_skus[sku]:
                        error_skus[sku].append(reason)

    return error_skus


# ─── Filter the All Listings Report ───────────────────────────────────────────

def build_filtered_feed(report_path: Path, error_skus: dict, output_path: Path) -> int:
    rows_written = 0
    not_found    = []

    with open(report_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        fieldnames = reader.fieldnames or []

        with open(output_path, 'w', newline='', encoding='utf-8') as out:
            writer = csv.DictWriter(out, fieldnames=fieldnames, delimiter='\t',
                                    extrasaction='ignore')
            writer.writeheader()

            found_skus = set()
            for row in reader:
                sku = (row.get('seller-sku', '') or '').strip()
                if sku in error_skus:
                    writer.writerow(row)
                    found_skus.add(sku)
                    rows_written += 1

    not_found = [s for s in error_skus if s not in found_skus]
    if not_found:
        print(f'\n  [WARN] {len(not_found)} error SKU(s) not in listings report '
              f'(may be deleted or use different SKU format):')
        for s in not_found[:20]:
            print(f'    {s}')
        if len(not_found) > 20:
            print(f'    … and {len(not_found) - 20} more')

    return rows_written


# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Build optimizer input containing only error SKUs.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    ap.add_argument('--report', metavar='FILE',
                    help='Amazon All Listings Report .txt file (auto-detected if omitted)')
    ap.add_argument('--results-dir', metavar='DIR',
                    help='Folder containing push result CSVs (default: same folder as script)')
    ap.add_argument('--include-warnings', action='store_true',
                    help='Also include SKUs that had warnings (not just hard errors)')
    ap.add_argument('--output', metavar='FILE',
                    help='Output filename (default: pc_error_feed_TIMESTAMP.txt)')
    args = ap.parse_args()

    results_dir = Path(args.results_dir) if args.results_dir else SCRIPT_DIR
    output_path = Path(args.output) if args.output else SCRIPT_DIR / f'pc_error_feed_{RUN_ID}.txt'

    print('\n' + '═' * 60)
    print('  PC Error Filter')
    print(f'  Run ID       : {RUN_ID}')
    print(f'  Results from : {results_dir}')
    print(f'  Warnings     : {"included" if args.include_warnings else "excluded"}')
    print('═' * 60)

    # Step 1: collect error SKUs
    error_skus = collect_error_skus(results_dir, args.include_warnings)

    if not error_skus:
        print('\n  No errors found in any push results — nothing to re-optimize!')
        print('  (Use --include-warnings to also catch warning-only SKUs)\n')
        return

    print(f'\n  Error SKUs found : {len(error_skus)}')

    # Step 2: find listings report
    report_path = find_listings_report(args.report or '')

    # Step 3: filter
    count = build_filtered_feed(report_path, error_skus, output_path)

    print('\n' + '═' * 60)
    print('  DONE')
    print(f'  Error SKUs      : {len(error_skus)}')
    print(f'  Rows written    : {count}')
    print(f'  Output file     : {output_path.name}')
    print('═' * 60)
    print()
    print('  Next step — set optimizer INPUT_FILE to this file:')
    print(f'    INPUT_FILE = str(SCRIPT_DIR / \'{output_path.name}\')')
    print('  Then run: python "pc_listing_optimizer_v4 (5).py"')
    print()


if __name__ == '__main__':
    main()
