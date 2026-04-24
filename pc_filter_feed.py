#!/usr/bin/env python3
"""
pc_filter_feed.py — Extract a subset of rows from the latest feed CSV.

The push script (pc_sp_api_push_v2.py) already runs the policy validator
on every field before pushing, so just filtering and passing --input is enough.

Usage:
    python pc_filter_feed.py --material "cutting board"
    python pc_filter_feed.py --material HDPE
    python pc_filter_feed.py --material "cutting board" --feed my_feed.csv

Then push the output:
    python pc_sp_api_push_v2.py --input pc_filtered_feed_<timestamp>.csv
"""

import argparse
import csv
import glob
import os
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RUN_ID     = datetime.now().strftime('%Y%m%d_%H%M%S')


def find_feed(hint: str = '') -> Path:
    if hint:
        p = Path(hint)
        if not p.is_absolute():
            p = SCRIPT_DIR / p
        if p.exists():
            return p
        print(f'[ERROR] Feed file not found: {p}')
        raise SystemExit(1)

    matches = glob.glob(str(SCRIPT_DIR / 'pc_amazon_feed_v4_*.csv'))
    if not matches:
        print('[ERROR] No pc_amazon_feed_v4_*.csv found. Pass --feed <filename>')
        raise SystemExit(1)

    chosen = Path(max(matches, key=os.path.getmtime))
    print(f'  Feed file : {chosen.name}')
    return chosen


def main():
    ap = argparse.ArgumentParser(description='Filter feed CSV by material keyword.')
    ap.add_argument('--material', metavar='KEYWORD', required=True,
                    help='Keyword to match in original_title or new_title (case-insensitive)')
    ap.add_argument('--feed', metavar='FILE',
                    help='Feed CSV to filter (default: most-recent pc_amazon_feed_v4_*.csv)')
    ap.add_argument('--output', metavar='FILE',
                    help='Output filename (auto-named if omitted)')
    args = ap.parse_args()

    kw = args.material.lower()

    feed_path = find_feed(args.feed or '')

    output_path = Path(args.output) if args.output else \
        SCRIPT_DIR / f'pc_filtered_feed_{RUN_ID}.csv'

    written  = 0
    skipped  = 0

    with open(feed_path, newline='', encoding='utf-8', errors='replace') as f:
        reader     = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        with open(output_path, 'w', newline='', encoding='utf-8') as out:
            writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()

            for row in reader:
                title = (
                    (row.get('original_title') or '') + ' ' +
                    (row.get('new_title') or '')
                ).lower()

                if kw in title:
                    writer.writerow(row)
                    written += 1
                else:
                    skipped += 1

    print(f'\n{"═" * 60}')
    print(f'  Rows matched  : {written}')
    print(f'  Rows skipped  : {skipped}')
    print(f'  Output file   : {output_path.name}')
    print(f'{"═" * 60}')
    print()
    print('  Next step — push the filtered file:')
    print(f'      python pc_sp_api_push_v2.py --input "{output_path.name}"')
    print()


if __name__ == '__main__':
    main()
