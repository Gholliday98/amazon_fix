#!/usr/bin/env python3
"""
pc_preflight.py — Pre-push validation for every listing.

Runs a full suite of checks before any listing reaches Amazon.
Blocks listings that would cause immediate errors. Logs warnings for
things that need manual review.

Can be imported by push scripts OR run standalone to audit a feed CSV
before pushing.

Usage:
    python pc_preflight.py --input pc_filtered_feed_TIMESTAMP.csv
    python pc_preflight.py --input pc_filtered_feed_TIMESTAMP.csv --fix-truncate

Import:
    from pc_preflight import preflight_check, PreflightResult
    result = preflight_check(row)
    if result.blocked:
        skip this row
"""

import argparse
import csv
import re
from dataclasses import dataclass, field
from pathlib import Path

try:
    from pc_policy_validator import validate_and_fix, check_backend_terms
    _VALIDATOR = True
except ImportError:
    _VALIDATOR = False

SCRIPT_DIR = Path(__file__).parent

# ── Amazon field limits ────────────────────────────────────────────────────────
TITLE_MAX         = 200
BULLET_MAX        = 500
DESCRIPTION_MAX   = 2000
BACKEND_TERMS_MAX = 249   # bytes, not chars
SIZE_DESC_MAX     = 50    # Amazon error 90225

BULLET_FIELDS     = [f'bullet{i}' for i in range(1, 6)]
PRODUCT_FORMS     = ('sheet', 'rod', 'tube', 'bar', 'block', 'panel', 'strip', 'film', 'board')
CAST_EXTRUDED_MATS = ('acrylic', 'nylon')

# ── Result dataclass ───────────────────────────────────────────────────────────

@dataclass
class PreflightResult:
    sku:      str = ''
    asin:     str = ''
    blocked:  bool = False          # True = do NOT push
    errors:   list[str] = field(default_factory=list)   # blocking issues
    warnings: list[str] = field(default_factory=list)   # non-blocking, log only

    def add_error(self, msg: str):
        self.errors.append(msg)
        self.blocked = True

    def add_warning(self, msg: str):
        self.warnings.append(msg)

    def summary(self) -> str:
        status = 'BLOCKED' if self.blocked else ('WARN' if self.warnings else 'OK')
        lines  = [f'[{status}] {self.sku}']
        for e in self.errors:
            lines.append(f'  ERROR   : {e}')
        for w in self.warnings:
            lines.append(f'  WARNING : {w}')
        return '\n'.join(lines)


# ── Core check function ────────────────────────────────────────────────────────

def preflight_check(row: dict, fix_truncate: bool = False) -> PreflightResult:
    """
    Run all preflight checks on a feed row.
    If fix_truncate=True, truncate fields that exceed length limits rather than blocking.
    Returns PreflightResult with blocked=True if the row should NOT be pushed.
    """
    g   = lambda k: (row.get(k) or '').strip()
    r   = PreflightResult(sku=g('sku'), asin=g('asin').upper())

    # ── Required fields ────────────────────────────────────────────────────────
    if not r.sku:
        r.add_error('Missing SKU — cannot push without SKU')
    if not g('new_title') and not g('description'):
        r.add_error('No pushable content — new_title and description both empty')

    # ── Title checks ──────────────────────────────────────────────────────────
    title = g('new_title')
    if title:
        if len(title) > TITLE_MAX:
            if fix_truncate:
                row['new_title'] = title[:TITLE_MAX].rstrip(' ,')
                r.add_warning(f'Title truncated from {len(title)} to {TITLE_MAX} chars')
            else:
                r.add_error(f'Title too long: {len(title)} chars (max {TITLE_MAX})')

        title_lower = title.lower()

        # Cast/Extruded recommended for Acrylic and Nylon (warning only — handled separately)
        if any(m in title_lower for m in CAST_EXTRUDED_MATS):
            if 'cast' not in title_lower and 'extruded' not in title_lower:
                r.add_warning(
                    f'Acrylic/Nylon title missing "Cast" or "Extruded" — add via pc_correct_designation.py')

        # Product form (sheet/rod/tube) required in title
        if not any(f in title_lower for f in PRODUCT_FORMS):
            r.add_warning(f'Title missing product form (sheet/rod/tube/bar/block/panel)')

        # No promotional symbols
        if re.search(r'[!$]{2,}', title):
            r.add_error('Title contains promotional symbols (!! or $$)')

        # No ALL CAPS words (Amazon flags listings with excessive caps)
        caps_words = re.findall(r'\b[A-Z]{4,}\b', title)
        # Allow known acceptable caps (HDPE, UHMW, PVC, PTFE, etc.)
        allowed_caps = {'HDPE', 'UHMW', 'PVC', 'PTFE', 'PEEK', 'CPVC', 'ABS',
                        'PET', 'PETG', 'PP', 'PE', 'PA', 'PVDF', 'PEI', 'PSU',
                        'HIPS', 'SAN', 'EVA', 'TPU', 'TPE', 'LDPE', 'LLDPE',
                        'HMWPE', 'XLPE', 'FRP', 'GRP', 'FR4', 'G10', 'NEMA',
                        'ASTM', 'ANSI', 'USA', 'UV', 'CNC', 'NSF', 'FDA'}
        bad_caps = [w for w in caps_words if w not in allowed_caps]
        if bad_caps:
            r.add_warning(f'Title has unexpected ALL CAPS words: {", ".join(bad_caps)}')

    # ── Bullet checks ─────────────────────────────────────────────────────────
    bullet_texts = []
    for bf in BULLET_FIELDS:
        b = g(bf)
        if not b:
            continue
        if len(b) > BULLET_MAX:
            if fix_truncate:
                row[bf] = b[:BULLET_MAX].rstrip(' ,')
                r.add_warning(f'{bf} truncated from {len(b)} to {BULLET_MAX} chars')
            else:
                r.add_error(f'{bf} too long: {len(b)} chars (max {BULLET_MAX})')
        bullet_texts.append(b)

    # Duplicate bullets
    seen_bullets = set()
    for i, bt in enumerate(bullet_texts):
        normalized = bt.lower().strip()
        if normalized in seen_bullets:
            r.add_warning(f'Duplicate bullet content detected in bullet{i+1}')
        seen_bullets.add(normalized)

    # ── Description checks ────────────────────────────────────────────────────
    desc = g('description')
    if desc:
        if len(desc) > DESCRIPTION_MAX:
            if fix_truncate:
                row['description'] = desc[:DESCRIPTION_MAX].rstrip(' ,')
                r.add_warning(f'Description truncated from {len(desc)} to {DESCRIPTION_MAX} chars')
            else:
                r.add_error(f'Description too long: {len(desc)} chars (max {DESCRIPTION_MAX})')

        # Must mention product form somewhere in description
        if title and not any(f in desc.lower() for f in PRODUCT_FORMS):
            r.add_warning('Description missing product form (sheet/rod/tube)')

    # ── Size description check ────────────────────────────────────────────────
    size_desc = g('size_description')
    if size_desc and len(size_desc) > SIZE_DESC_MAX:
        if fix_truncate:
            row['size_description'] = size_desc[:SIZE_DESC_MAX].rstrip(' ,')
            r.add_warning(f'size_description truncated from {len(size_desc)} to {SIZE_DESC_MAX} chars (error 90225)')
        else:
            r.add_error(f'size_description too long: {len(size_desc)} chars (max {SIZE_DESC_MAX}) — error 90225')

    # ── Backend search terms ──────────────────────────────────────────────────
    bst = g('backend_search_terms')
    if bst:
        if len(bst.encode('utf-8')) > BACKEND_TERMS_MAX:
            if fix_truncate:
                encoded = bst.encode('utf-8')[:BACKEND_TERMS_MAX]
                row['backend_search_terms'] = encoded.decode('utf-8', errors='ignore').rstrip()
                r.add_warning(f'backend_search_terms truncated to {BACKEND_TERMS_MAX} bytes')
            else:
                r.add_warning(f'backend_search_terms exceeds {BACKEND_TERMS_MAX} bytes — will be rejected by Amazon')

    # ── Policy validator ──────────────────────────────────────────────────────
    if _VALIDATOR:
        all_policy_violations = []
        for field_name in ['new_title'] + BULLET_FIELDS + ['description', 'size_description']:
            val = g(field_name)
            if val:
                _, viols = validate_and_fix(val, field_name)
                hard = [v for v in viols if v.startswith('[HARD]')]
                if hard:
                    for v in hard:
                        r.add_error(f'Policy violation in {field_name}: {v}')

    return r


# ── Standalone runner ──────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description='Pre-push preflight check for feed CSVs.')
    ap.add_argument('--input', required=True, metavar='FILE',
                    help='Feed CSV to check')
    ap.add_argument('--fix-truncate', action='store_true',
                    help='Auto-truncate fields that exceed length limits instead of blocking')
    ap.add_argument('--blocked-only', action='store_true',
                    help='Only show rows that would be blocked')
    args = ap.parse_args()

    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = SCRIPT_DIR / input_path
    if not input_path.exists():
        print(f'[ERROR] File not found: {input_path}')
        raise SystemExit(1)

    rows = []
    fieldnames = []
    with open(input_path, newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        for row in reader:
            rows.append(row)

    blocked_count  = 0
    warning_count  = 0
    ok_count       = 0

    print(f'\n{"═" * 60}')
    print(f'  PC Preflight Check')
    print(f'  Input : {input_path.name}  ({len(rows)} rows)')
    print(f'{"═" * 60}\n')

    for row in rows:
        result = preflight_check(row, fix_truncate=args.fix_truncate)

        if result.blocked:
            blocked_count += 1
            print(result.summary())
        elif result.warnings:
            warning_count += 1
            if not args.blocked_only:
                print(result.summary())
        else:
            ok_count += 1

    print(f'\n{"═" * 60}')
    print(f'  OK      : {ok_count}')
    print(f'  Warnings: {warning_count}')
    print(f'  Blocked : {blocked_count}')
    print(f'{"═" * 60}')

    if blocked_count:
        print(f'\n  {blocked_count} listings would be blocked from pushing.')
        print(f'  Fix the errors above, or re-run with --fix-truncate for length issues.')
    else:
        print(f'\n  All listings passed. Safe to push.')


if __name__ == '__main__':
    main()
