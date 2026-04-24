#!/usr/bin/env python3
"""
pc_fix_errors.py — One-shot script to clean and re-push all errored listings.

Reads all push result CSVs, collects every ASIN that errored, finds their
content across all feed CSVs (newest first), and pushes cleaned versions
through the policy validator.

Usage:
    python pc_fix_errors.py
    python pc_fix_errors.py --dry-run      # test without pushing
    python pc_fix_errors.py --limit 50     # push first 50 only
"""

import argparse
import csv
import glob
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RUN_ID     = datetime.now().strftime('%Y%m%d_%H%M%S')


def load_error_asins() -> dict:
    """Return {asin: error_code} for every errored ASIN across all push results."""
    files = sorted(
        glob.glob(str(SCRIPT_DIR / 'pc_push_v2_results_*.csv')) +
        glob.glob(str(SCRIPT_DIR / 'pc_push_results_*.csv')),
        key=os.path.getmtime,
        reverse=True
    )
    if not files:
        print('[ERROR] No push result CSVs found.')
        raise SystemExit(1)

    print(f'  Reading {len(files)} push result file(s)...')

    error_asins  = {}
    success_asins = set()

    for fpath in files:
        with open(fpath, newline='', encoding='utf-8', errors='replace') as f:
            for row in csv.DictReader(f):
                status = (row.get('status') or '').strip().lower()
                asin   = (row.get('asin') or '').strip().upper()
                code   = (row.get('error_code') or '').strip()
                if not asin:
                    continue
                if status == 'success':
                    success_asins.add(asin)
                elif status == 'error' and asin not in success_asins:
                    error_asins[asin] = code

    # Remove any that have since succeeded in a later run
    for asin in success_asins:
        error_asins.pop(asin, None)

    return error_asins


def find_in_feeds(target_asins: set) -> tuple[list[dict], list, list]:
    """Search all feed CSVs for target ASINs. Returns (rows, fieldnames, not_found)."""
    feeds = sorted(
        glob.glob(str(SCRIPT_DIR / 'pc_amazon_feed_v4_*.csv')),
        key=os.path.getmtime,
        reverse=True
    )
    if not feeds:
        print('[ERROR] No pc_amazon_feed_v4_*.csv found.')
        raise SystemExit(1)

    print(f'  Searching {len(feeds)} feed file(s)...')

    seen      = set()
    rows      = []
    fieldnames = []
    remaining = target_asins.copy()

    for fpath in feeds:
        if not remaining:
            break
        with open(fpath, newline='', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            if not fieldnames and reader.fieldnames:
                fieldnames = reader.fieldnames
            for row in reader:
                asin = (row.get('asin') or '').strip().upper()
                if asin in remaining and asin not in seen:
                    rows.append(row)
                    seen.add(asin)
                    remaining.discard(asin)

    not_found = sorted(remaining)
    return rows, fieldnames, not_found


def main():
    ap = argparse.ArgumentParser(description='Re-push all errored listings with policy validator.')
    ap.add_argument('--dry-run', action='store_true', help='Build output but do not push')
    ap.add_argument('--limit', type=int, default=0, metavar='N', help='Cap to first N listings')
    ap.add_argument('--code', metavar='CODE',
                    help='Only fix listings with this error code (e.g. Rejected, GetFailed)')
    args = ap.parse_args()

    print(f'\n{"═" * 60}')
    print('  PC Fix Errors')
    print(f'  Run ID : {RUN_ID}')
    print(f'  Mode   : {"DRY RUN" if args.dry_run else "LIVE"}')
    if args.code:
        print(f'  Filter : error code = {args.code}')
    print(f'{"═" * 60}\n')

    # Step 1: collect errored ASINs
    error_asins = load_error_asins()
    print(f'  Total errored ASINs (not yet fixed): {len(error_asins)}')

    if args.code:
        error_asins = {a: c for a, c in error_asins.items()
                       if args.code.lower() in c.lower()}
        print(f'  Filtered to "{args.code}": {len(error_asins)} ASINs')

    if not error_asins:
        print('\n  Nothing to fix.')
        return

    # Step 2: find content in feed CSVs
    rows, fieldnames, not_found = find_in_feeds(set(error_asins.keys()))

    print(f'\n  Found in feed CSVs : {len(rows)}')
    print(f'  Not in any feed    : {len(not_found)} (need optimizer or Amazon case)')

    if not_found:
        nf_path = SCRIPT_DIR / f'pc_fix_errors_not_found_{RUN_ID}.txt'
        nf_path.write_text('\n'.join(not_found), encoding='utf-8')
        print(f'  Not-found list     : {nf_path.name}')

    if args.limit and len(rows) > args.limit:
        rows = rows[:args.limit]
        print(f'  Capped to          : {args.limit} rows')

    if not rows:
        print('\n  No rows to push — open Amazon cases for the not-found ASINs.')
        return

    # Step 3: write filtered feed CSV
    feed_out = SCRIPT_DIR / f'pc_fix_errors_feed_{RUN_ID}.csv'
    with open(feed_out, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f'\n  Feed written       : {feed_out.name}')

    if args.dry_run:
        print('\n  [DRY RUN] Push command would be:')
        print(f'  python pc_sp_api_push_v2.py --input "{feed_out.name}" --resume')
        return

    # Step 4: run push
    print(f'\n  Launching push...\n')
    cmd = [
        sys.executable, str(SCRIPT_DIR / 'pc_sp_api_push_v2.py'),
        '--input', str(feed_out),
        '--resume'
    ]
    subprocess.run(cmd, cwd=str(SCRIPT_DIR))


if __name__ == '__main__':
    main()
