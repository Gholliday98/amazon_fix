#!/usr/bin/env python3
"""
pc_export_rules.py — Export all Amazon content policy rules to a CSV.

Usage:
    python pc_export_rules.py
    python pc_export_rules.py --output my_rules.csv
"""

import argparse
import csv
from pathlib import Path
from pc_policy_validator import _HARD_PATTERNS, _SOFT_PATTERNS

SCRIPT_DIR = Path(__file__).parent


def main():
    ap = argparse.ArgumentParser(description='Export policy rules to CSV.')
    ap.add_argument('--output', default='pc_policy_rules.csv', metavar='FILE')
    args = ap.parse_args()

    out_path = SCRIPT_DIR / args.output
    rows = []

    for pattern, replacement, label in _HARD_PATTERNS:
        rows.append({
            'severity':    'HARD',
            'description': label,
            'example_pattern': pattern.pattern,
            'action':      'REMOVE' if replacement == '' else f'REPLACE with "{replacement}"',
        })

    for pattern, replacement, label in _SOFT_PATTERNS:
        rows.append({
            'severity':    'SOFT',
            'description': label,
            'example_pattern': pattern.pattern,
            'action':      'REMOVE' if replacement == '' else f'REPLACE with "{replacement}"',
        })

    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['severity', 'description', 'example_pattern', 'action'])
        writer.writeheader()
        writer.writerows(rows)

    hard_count = sum(1 for r in rows if r['severity'] == 'HARD')
    soft_count = sum(1 for r in rows if r['severity'] == 'SOFT')
    print(f'  Exported {hard_count} HARD rules and {soft_count} SOFT rules to {out_path.name}')


if __name__ == '__main__':
    main()
