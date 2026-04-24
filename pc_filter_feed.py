#!/usr/bin/env python3
"""
pc_filter_feed.py — Extract a subset of rows from ALL feed CSVs.

Searches every pc_amazon_feed_v4_*.csv (newest first), deduplicates by ASIN,
and outputs a single push-ready CSV. The push script runs the policy validator
on every field before pushing, so flagged language is stripped automatically.

Usage:
    python pc_filter_feed.py --material "cutting board"
    python pc_filter_feed.py --asins B0ABC123,B0DEF456,B0GHI789
    python pc_filter_feed.py --material "cutting board" --asins B0ABC123,...
    python pc_filter_feed.py --asins B0ABC123,... --feed my_feed.csv

Then push the output:
    python pc_sp_api_push_v2.py --input pc_filtered_feed_<timestamp>.csv --resume
"""

import argparse
import csv
import glob
import os
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RUN_ID     = datetime.now().strftime('%Y%m%d_%H%M%S')


def find_feeds(hint: str = '') -> list[Path]:
    if hint:
        p = Path(hint)
        if not p.is_absolute():
            p = SCRIPT_DIR / p
        if p.exists():
            return [p]
        print(f'[ERROR] Feed file not found: {p}')
        raise SystemExit(1)

    matches = glob.glob(str(SCRIPT_DIR / 'pc_amazon_feed_v4_*.csv'))
    if not matches:
        print('[ERROR] No pc_amazon_feed_v4_*.csv found.')
        raise SystemExit(1)

    # Newest first so we use the most recent content when deduplicating
    return [Path(p) for p in sorted(matches, key=os.path.getmtime, reverse=True)]


def main():
    ap = argparse.ArgumentParser(description='Filter feed CSVs by material keyword or ASIN list.')
    ap.add_argument('--material', metavar='KEYWORD',
                    help='Keyword to match in original_title or new_title (case-insensitive)')
    ap.add_argument('--asins', metavar='ASIN1,ASIN2,...',
                    help='Comma-separated list of ASINs to extract')
    ap.add_argument('--feed', metavar='FILE',
                    help='Specific feed CSV to use (default: searches all pc_amazon_feed_v4_*.csv)')
    ap.add_argument('--output', metavar='FILE',
                    help='Output filename (auto-named if omitted)')
    args = ap.parse_args()

    if not args.material and not args.asins:
        print('[ERROR] Specify --material and/or --asins')
        raise SystemExit(1)

    kw = args.material.lower() if args.material else None

    asin_set = set()
    if args.asins:
        asin_set = {a.strip().upper() for a in args.asins.split(',') if a.strip()}
        print(f'  ASIN filter   : {len(asin_set)} ASINs')

    feed_paths = find_feeds(args.feed or '')
    print(f'  Feed files    : {len(feed_paths)} (searching all)')

    output_path = Path(args.output) if args.output else \
        SCRIPT_DIR / f'pc_filtered_feed_{RUN_ID}.csv'

    # Collect rows — deduplicate by ASIN (newest feed wins)
    seen_asins   = set()
    matched_rows = []
    fieldnames   = []
    not_found    = asin_set.copy()

    for feed_path in feed_paths:
        with open(feed_path, newline='', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            if not fieldnames and reader.fieldnames:
                fieldnames = reader.fieldnames

            for row in reader:
                row_asin = (row.get('asin') or '').strip().upper()
                if row_asin in seen_asins:
                    continue

                title = (
                    (row.get('original_title') or '') + ' ' +
                    (row.get('new_title') or '')
                ).lower()

                matched = False
                if asin_set and row_asin in asin_set:
                    matched = True
                    not_found.discard(row_asin)
                if kw and kw in title:
                    matched = True

                if matched:
                    matched_rows.append(row)
                    seen_asins.add(row_asin)

    with open(output_path, 'w', newline='', encoding='utf-8') as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in matched_rows:
            writer.writerow(row)

    print(f'\n{"═" * 60}')
    print(f'  Rows matched  : {len(matched_rows)}')
    if not_found:
        print(f'  ASINs not found in any feed ({len(not_found)}) — need optimizer:')
        for a in sorted(not_found):
            print(f'    {a}')
    print(f'  Output file   : {output_path.name}')
    print(f'{"═" * 60}')
    print()
    print('  Next step — push (--resume skips already-done SKUs):')
    print(f'      python pc_sp_api_push_v2.py --input "{output_path.name}" --resume')
    print()


if __name__ == '__main__':
    main()
