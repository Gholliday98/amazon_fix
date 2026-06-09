#!/usr/bin/env python3
"""
pc_build_run.py — Build a targeted optimizer input from the All Listings Report.

Filters by material keyword and/or to only SKUs that errored in previous push runs.

Usage:
    python pc_build_run.py --material "cutting board"
    python pc_build_run.py --material HDPE
    python pc_build_run.py --material "Polypropylene" --errors-only
    python pc_build_run.py --errors-only
    python pc_build_run.py --material HDPE --report "My_All_Listings.txt"
"""

import argparse
import csv
import glob
import os
from datetime import datetime
from pathlib import Path



SCRIPT_DIR = Path(__file__).parent
RUN_ID     = datetime.now().strftime('%Y%m%d_%H%M%S')


def load_error_skus() -> set:
    """Collect SKUs that errored in any previous push result CSV."""
    error_skus = set()
    files = sorted(
        glob.glob(str(SCRIPT_DIR / 'pc_push_results_*.csv')) +
        glob.glob(str(SCRIPT_DIR / 'pc_push_v2_results_*.csv')),
        key=os.path.getmtime
    )
    if not files:
        print('  [INFO] No push result CSVs found.')
        return error_skus

    print(f'  Push result files: {len(files)}')
    for fpath in files:
        with open(fpath, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                status = (row.get('status') or '').strip().lower()
                sku    = (row.get('sku') or '').strip()
                if sku and status == 'error':
                    error_skus.add(sku)

    print(f'  Error SKUs from push results: {len(error_skus)}')
    return error_skus


def find_listings_report(hint: str = '') -> Path:
    if hint:
        p = Path(hint)
        if not p.is_absolute():
            p = SCRIPT_DIR / p
        if p.exists():
            return p
        print(f'[ERROR] Report not found: {p}')
        raise SystemExit(1)

    candidates = [
        p for p in (
            glob.glob(str(SCRIPT_DIR / '*All*Listings*Report*.txt')) +
            glob.glob(str(SCRIPT_DIR / '*All_Listings*.txt')) +
            glob.glob(str(SCRIPT_DIR / '*listings*.txt'))
        )
        if 'pc_error_feed' not in p and 'pc_build_run' not in p
    ]
    if not candidates:
        print('[ERROR] No All Listings Report found. Pass --report <filename>')
        raise SystemExit(1)

    chosen = Path(max(candidates, key=os.path.getmtime))
    print(f'  All Listings Report: {chosen.name}')
    return chosen


def main():
    ap = argparse.ArgumentParser(
        description='Build targeted optimizer input from All Listings Report.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    ap.add_argument('--material', metavar='KEYWORD',
                    help='Filter by material keyword in title (e.g. HDPE, "cutting board", Polypropylene)')
    ap.add_argument('--asins', metavar='ASIN1,ASIN2,...',
                    help='Comma-separated list of ASINs to extract')
    ap.add_argument('--errors-only', action='store_true',
                    help='Only include SKUs that errored in previous push runs')
    ap.add_argument('--report', metavar='FILE',
                    help='All Listings Report .txt (auto-detected if omitted)')
    ap.add_argument('--output', metavar='FILE',
                    help='Output filename (auto-named if omitted)')
    args = ap.parse_args()

    if not args.material and not args.errors_only and not args.asins:
        print('[ERROR] Specify --material, --asins, and/or --errors-only')
        raise SystemExit(1)

    asin_set = set()
    if args.asins:
        asin_set = {a.strip().upper() for a in args.asins.split(',') if a.strip()}

    print(f'\n{"═" * 60}')
    print('  PC Build Run')
    print(f'  Material filter : {args.material or "none"}')
    print(f'  ASIN filter     : {len(asin_set) if asin_set else "none"}')
    print(f'  Errors only     : {args.errors_only}')
    print(f'{"═" * 60}\n')

    # Load error SKUs if needed
    error_skus = load_error_skus() if args.errors_only else set()

    # Find report
    report_path = find_listings_report(args.report or '')

    # Output path
    material_slug = (args.material or 'asins' if asin_set else 'all').lower().replace(' ', '_')
    suffix = '_errors' if args.errors_only else ''
    output_path = Path(args.output) if args.output else \
        SCRIPT_DIR / f'pc_run_{material_slug}{suffix}_{RUN_ID}.txt'

    # Filter
    written = 0
    skipped_material = 0
    skipped_errors = 0
    not_found = asin_set.copy()

    with open(report_path, 'r', encoding='utf-8-sig', errors='replace') as f:
        reader     = csv.DictReader(f, delimiter='\t')
        fieldnames = reader.fieldnames or []

        with open(output_path, 'w', newline='', encoding='utf-8') as out:
            writer = csv.DictWriter(out, fieldnames=fieldnames, delimiter='\t',
                                    extrasaction='ignore')
            writer.writeheader()

            for row in reader:
                title    = (row.get('item-name') or '').strip()
                sku      = (row.get('seller-sku') or '').strip()
                row_asin = (row.get('asin1') or '').strip().upper()

                # ASIN filter
                if asin_set:
                    if row_asin in asin_set:
                        not_found.discard(row_asin)
                    elif not (args.material and args.material.lower() in title.lower()):
                        skipped_material += 1
                        continue

                # Material filter
                if args.material and args.material.lower() not in title.lower():
                    if row_asin not in asin_set:
                        skipped_material += 1
                        continue

                # Errors-only filter
                if args.errors_only and sku not in error_skus:
                    skipped_errors += 1
                    continue

                writer.writerow(row)
                written += 1

    print(f'\n{"═" * 60}')
    print(f'  Rows written    : {written}')
    if skipped_material: print(f'  Skipped (no match)        : {skipped_material}')
    if skipped_errors:   print(f'  Skipped (no error)        : {skipped_errors}')
    if not_found:
        print(f'  ASINs not in report ({len(not_found)}):')
        for a in sorted(not_found):
            print(f'    {a}')
    print(f'  Output file     : {output_path.name}')
    print(f'{"═" * 60}')
    print()
    print('  Next step — set optimizer INPUT_FILE:')
    print(f"      INPUT_FILE = str(SCRIPT_DIR / '{output_path.name}')")
    print(f'  Then run: python "pc_listing_optimizer_v4 (5).py"')
    print()


if __name__ == '__main__':
    main()
