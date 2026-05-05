#!/usr/bin/env python3
"""
pc_build_designation_map.py — Build cast/extruded designation map from an Excel product file.

Reads an Excel file, finds the title/description column, searches each row
for "cast" or "extruded", and outputs a designation CSV ready for use with:
    python pc_fix_99300.py --rerun-blocked ... --designations-csv output.csv

Usage:
    # Auto-detect title column and show headers
    python pc_build_designation_map.py --excel "amazon descriptions test.xlsx" --show-headers

    # Build the map (auto-detect title column)
    python pc_build_designation_map.py --excel "amazon descriptions test.xlsx" --rerun-results pc_fix_99300_rerun_*.csv

    # Specify title column manually if auto-detect fails
    python pc_build_designation_map.py --excel "amazon descriptions test.xlsx" --rerun-results pc_fix_99300_rerun_*.csv --title-col "Product Title"
"""

import argparse
import csv
import re
import sys
from pathlib import Path

try:
    import openpyxl
except ImportError:
    print('[ERROR] openpyxl not installed. Run: pip install openpyxl')
    sys.exit(1)

SCRIPT_DIR = Path(__file__).parent

TITLE_COL_CANDIDATES = [
    'item-name', 'item_name', 'title', 'product_name', 'product title',
    'product name', 'name', 'item name', 'listing title', 'new_title',
    'new title', 'description', 'item_description', 'product description',
]

SKU_COL_CANDIDATES = [
    'seller-sku', 'seller_sku', 'sku', 'item sku', 'item_sku',
    'seller sku', 'listing sku',
]


def find_title_col(headers: list[str]) -> str | None:
    """Return the first header that looks like a title/name column."""
    lower = [h.lower().strip() for h in headers]
    for candidate in TITLE_COL_CANDIDATES:
        if candidate in lower:
            return headers[lower.index(candidate)]
    return None


def extract_designation(text: str) -> str | None:
    if not text:
        return None
    tl = text.lower()
    if 'extruded' in tl:
        return 'Extruded'
    if 'cast' in tl:
        return 'Cast'
    return None


def main():
    ap = argparse.ArgumentParser(
        description='Build cast/extruded designation map from an Excel product file.')
    ap.add_argument('--excel', required=True, metavar='FILE',
                    help='Excel file (e.g. "amazon descriptions test.xlsx")')
    ap.add_argument('--rerun-results', metavar='FILE',
                    help='Rerun results CSV — only output SKUs that appear in this file')
    ap.add_argument('--title-col', metavar='COLUMN',
                    help='Column name containing the product title (auto-detected if omitted)')
    ap.add_argument('--show-headers', action='store_true',
                    help='Print column headers and sample rows then exit')
    ap.add_argument('--sku-col', metavar='COLUMN', default='',
                    help='Column name containing the SKU (auto-detected if omitted)')
    args = ap.parse_args()

    excel_path = Path(args.excel)
    if not excel_path.is_absolute():
        excel_path = SCRIPT_DIR / excel_path
    if not excel_path.exists():
        print(f'[ERROR] File not found: {excel_path}')
        sys.exit(1)

    print(f'  Reading {excel_path.name} ...')
    wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        print('[ERROR] Excel file appears empty.')
        sys.exit(1)

    headers = [str(h).strip() if h is not None else '' for h in rows[0]]
    data_rows = rows[1:]

    print(f'  {len(data_rows)} data rows, {len(headers)} columns\n')

    if args.show_headers:
        print('  Column headers:')
        for i, h in enumerate(headers):
            print(f'    [{i}] {h}')
        print()
        print('  First 3 data rows:')
        for row in data_rows[:3]:
            print('  ', dict(zip(headers, [str(v)[:40] if v else '' for v in row])))
        return

    # Find SKU column
    sku_col = args.sku_col or ''
    if not sku_col:
        lower_h = [h.lower() for h in headers]
        for candidate in SKU_COL_CANDIDATES:
            if candidate in lower_h:
                sku_col = headers[lower_h.index(candidate)]
                break
    if not sku_col:
        print('[ERROR] Could not find SKU column. Use --sku-col to specify it.')
        print(f'  Available columns: {", ".join(headers)}')
        sys.exit(1)

    # Find title column
    title_col = args.title_col or find_title_col(headers)
    if not title_col:
        print('[ERROR] Could not auto-detect title column. Use --title-col to specify it.')
        print(f'  Available columns: {", ".join(headers)}')
        sys.exit(1)

    print(f'  SKU column   : {sku_col}')
    print(f'  Title column : {title_col}')

    sku_idx   = headers.index(sku_col)
    title_idx = headers.index(title_col)

    # Load target SKUs from rerun results if provided
    target_skus: set[str] = set()
    rerun_rows: list[dict] = []
    if args.rerun_results:
        rp = Path(args.rerun_results)
        if not rp.is_absolute():
            rp = SCRIPT_DIR / rp
        with open(rp, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                if row.get('status', '').upper() in ('OK', 'NEEDS_MANUAL', 'BLOCKED'):
                    sku = row.get('sku', '').strip()
                    asin = row.get('asin', '').strip()
                    if sku:
                        target_skus.add(sku)
                        rerun_rows.append({'sku': sku, 'asin': asin})
        print(f'  Target SKUs  : {len(target_skus)} from {rp.name}\n')

    # Build designation map from Excel
    excel_map: dict[str, str] = {}
    no_designation = []

    for row in data_rows:
        sku   = str(row[sku_idx]).strip() if row[sku_idx] is not None else ''
        title = str(row[title_idx]).strip() if row[title_idx] is not None else ''

        if not sku or sku == 'None':
            continue

        # If filtering by target SKUs, skip non-targets
        if target_skus and sku not in target_skus:
            continue

        desig = extract_designation(title)
        if desig:
            excel_map[sku] = desig
        elif target_skus and sku in target_skus:
            no_designation.append({'sku': sku, 'title': title[:80]})

    print(f'  Found designation for : {len(excel_map)} SKUs')
    if no_designation:
        print(f'  No designation found  : {len(no_designation)} SKUs')

    if not excel_map and not rerun_rows:
        print('\n  No matches found. Check --title-col is correct.')
        return

    # Write output CSV
    out_path = SCRIPT_DIR / 'pc_designations_from_excel.csv'
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['sku', 'asin', 'designation'])

        if rerun_rows:
            asin_map = {r['sku']: r['asin'] for r in rerun_rows}
            for row in rerun_rows:
                sku  = row['sku']
                asin = row['asin']
                desig = excel_map.get(sku, '')
                writer.writerow([sku, asin, desig])
        else:
            for sku, desig in sorted(excel_map.items()):
                writer.writerow([sku, '', desig])

    print(f'\n  Output: {out_path.name}')

    if no_designation:
        print(f'\n  SKUs with no designation in Excel title (need manual fill):')
        for r in no_designation[:20]:
            print(f'    {r["sku"]:30s}  title: {r["title"]}')
        if len(no_designation) > 20:
            print(f'    ... and {len(no_designation) - 20} more')

    print(f'\n  Next step:')
    print(f'    1. Open {out_path.name} in Excel and fill any blank designation cells')
    print(f'    2. Run: python pc_fix_99300.py --rerun-blocked <results_csv> --designations-csv {out_path.name}')


if __name__ == '__main__':
    main()
