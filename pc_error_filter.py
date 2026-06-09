#!/usr/bin/env python3
"""
pc_error_filter.py — Build a filtered optimizer/push input from problem SKUs.

Collects SKUs from three sources:
  1. pc_push_results_*.csv / pc_push_v2_results_*.csv  — API push errors
  2. --inactive <file>  — Inactive Listings Report from Seller Central
  3. --suppressed <file> — Suppressed Listings Report from Seller Central

Cross-references all problem SKUs against the Amazon All Listings Report
and writes a single tab-delimited file the optimizer or push scripts
can process directly.

Usage
-----
    python pc_error_filter.py
    python pc_error_filter.py --inactive "Inactive_Listings_Report.txt"
    python pc_error_filter.py --inactive "Inactive_Listings_Report.txt" --include-warnings
    python pc_error_filter.py --report my_all_listings.txt --inactive inactive.txt

Output
------
    pc_error_feed_YYYYMMDD_HHMMSS.txt  — tab-delimited, optimizer-ready
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
            print(f'  All Listings report : {chosen.name}')
            return chosen

    print('[ERROR] No Amazon All Listings Report (.txt) found.')
    print('        Re-download from Seller Central → Reports → Inventory → All Listings.')
    print('        Or pass --report <filename>')
    sys.exit(1)


# ─── Collect error SKUs from push results ─────────────────────────────────────

def collect_error_skus(results_dir: Path, include_warnings: bool) -> dict:
    """Returns {sku: [reason, ...]} for every SKU that failed in a push run."""
    error_skus: dict = {}

    patterns = ['pc_push_results_*.csv', 'pc_push_v2_results_*.csv']
    result_files = []
    for pat in patterns:
        result_files.extend(glob.glob(str(results_dir / pat)))

    if not result_files:
        print('  [INFO] No push result CSVs found — skipping API error collection.')
        return error_skus

    result_files.sort(key=os.path.getmtime)
    print(f'\n  Push result files ({len(result_files)}):')
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


# ─── Collect SKUs from an Inactive / Suppressed report ────────────────────────

def collect_report_skus(report_path: Path, label: str) -> dict:
    """
    Read an Inactive or Suppressed Listings Report from Seller Central.
    Returns {sku: [reason]} using the status/reason column if present.
    Handles both tab-delimited and comma-delimited files.
    """
    skus: dict = {}
    if not report_path.exists():
        print(f'[ERROR] {label} report not found: {report_path}')
        return skus

    # Amazon reports use UTF-8 with BOM — utf-8-sig strips it automatically
    with open(report_path, 'r', encoding='utf-8-sig', errors='replace') as f:
        sample = f.read(2048)
    delimiter = '\t' if sample.count('\t') > sample.count(',') else ','

    with open(report_path, newline='', encoding='utf-8-sig', errors='replace') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            # Seller Central uses 'seller-sku' or 'SKU' depending on report type
            sku = (row.get('seller-sku') or row.get('SKU') or row.get('sku') or '').strip()
            if not sku:
                continue
            # Grab whatever reason column exists
            reason = (
                row.get('status-message') or
                row.get('Suppression Reason') or
                row.get('inactive-reason') or
                row.get('Reason') or
                label
            ).strip()
            skus.setdefault(sku, [])
            if reason not in skus[sku]:
                skus[sku].append(reason)

    print(f'  {label} report: {len(skus)} SKUs loaded from {report_path.name}')
    return skus


# ─── Filter the All Listings Report ───────────────────────────────────────────

def build_filtered_feed(report_path: Path, target_skus: dict, output_path: Path) -> int:
    rows_written = 0

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
                if sku in target_skus:
                    writer.writerow(row)
                    found_skus.add(sku)
                    rows_written += 1

    not_found = [s for s in target_skus if s not in found_skus]
    if not_found:
        print(f'\n  [WARN] {len(not_found)} SKU(s) not found in All Listings Report:')
        for s in not_found[:20]:
            print(f'    {s}  ← {"; ".join(target_skus[s][:1])}')
        if len(not_found) > 20:
            print(f'    … and {len(not_found) - 20} more')

    return rows_written


# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description='Build optimizer/push input from error + inactive + suppressed SKUs.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    ap.add_argument('--report', metavar='FILE',
                    help='Amazon All Listings Report .txt (auto-detected if omitted)')
    ap.add_argument('--inactive', metavar='FILE',
                    help='Inactive Listings Report downloaded from Seller Central')
    ap.add_argument('--suppressed', metavar='FILE',
                    help='Suppressed Listings Report downloaded from Seller Central')
    ap.add_argument('--results-dir', metavar='DIR',
                    help='Folder with push result CSVs (default: script folder)')
    ap.add_argument('--include-warnings', action='store_true',
                    help='Also include SKUs that only had warnings (not hard errors)')
    ap.add_argument('--output', metavar='FILE',
                    help='Output filename (default: pc_error_feed_TIMESTAMP.txt)')
    args = ap.parse_args()

    results_dir = Path(args.results_dir) if args.results_dir else SCRIPT_DIR
    output_path = Path(args.output) if args.output else SCRIPT_DIR / f'pc_error_feed_{RUN_ID}.txt'

    print('\n' + '═' * 60)
    print('  PC Error Filter')
    print(f'  Run ID   : {RUN_ID}')
    print('═' * 60)

    # Collect from all sources
    all_skus: dict = {}

    def merge(source: dict):
        for sku, reasons in source.items():
            all_skus.setdefault(sku, [])
            for r in reasons:
                if r not in all_skus[sku]:
                    all_skus[sku].append(r)

    # 1. Push errors
    merge(collect_error_skus(results_dir, args.include_warnings))

    # 2. Inactive report
    if args.inactive:
        p = Path(args.inactive)
        if not p.is_absolute():
            p = SCRIPT_DIR / p
        merge(collect_report_skus(p, 'Inactive'))

    # 3. Suppressed report
    if args.suppressed:
        p = Path(args.suppressed)
        if not p.is_absolute():
            p = SCRIPT_DIR / p
        merge(collect_report_skus(p, 'Suppressed'))

    if not all_skus:
        print('\n  No problem SKUs found from any source.')
        print('  Pass --inactive and/or --suppressed to include those reports.\n')
        return

    # Breakdown
    print(f'\n  Total problem SKUs : {len(all_skus)}')

    # Find the All Listings Report and filter it
    report_path = find_listings_report(args.report or '')
    count = build_filtered_feed(report_path, all_skus, output_path)

    print('\n' + '═' * 60)
    print('  DONE')
    print(f'  Problem SKUs    : {len(all_skus)}')
    print(f'  Rows written    : {count}')
    print(f'  Output          : {output_path.name}')
    print('═' * 60)
    print()
    print('  Next steps:')
    print(f'  1. Set optimizer INPUT_FILE = str(SCRIPT_DIR / \'{output_path.name}\')')
    print('     Then run: python "pc_listing_optimizer_v4 (5).py"')
    print()
    print(f'  2. Or push directly (skips optimizer):')
    print(f'     python pc_sp_api_push.py --input <optimizer_output_csv>')
    print(f'     python pc_sp_api_push_v2.py --input <optimizer_output_csv>')
    print()


if __name__ == '__main__':
    main()
