#!/usr/bin/env python3
"""
pc_strip_listing.py — Nuclear clean: strip all marketing copy from listings.

For chronically failing ASINs. Replaces every text field (bullets, description,
subject_matter, intended_use, target_audience, recommended_uses) with minimal
factual content derived from the title and material fields. Keeps all structural
fields (ASIN, SKU, brand, dimensions, item_type_keyword, etc.) untouched.

Usage:
    python pc_strip_listing.py --input pc_filtered_feed_TIMESTAMP.csv
    python pc_strip_listing.py --input pc_filtered_feed_TIMESTAMP.csv --asins B0ABC123,B0DEF456
    python pc_strip_listing.py --input pc_filtered_feed_TIMESTAMP.csv --dry-run

Then push:
    python pc_sp_api_push_v2.py --input pc_stripped_TIMESTAMP.csv --resume
    python pc_sp_api_push.py   --input pc_stripped_TIMESTAMP.csv --resume
"""

import argparse
import csv
import re
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RUN_ID     = datetime.now().strftime('%Y%m%d_%H%M%S')

BULLET_FIELDS  = [f'bullet{i}' for i in range(1, 6)]
TEXT_FIELDS    = BULLET_FIELDS + [
    'description', 'subject_matter', 'intended_use',
    'target_audience', 'recommended_uses', 'backend_search_terms',
]


def _parse_title(title: str) -> dict:
    """
    Extract material, product form, color/finish, and dimensions from a
    Plastic-Craft title like:
      Plastic-Craft | HDPE Sheet, White, 1/4" x 24" x 48"
      Plastic-Craft | Acrylic Rod, Clear, 1" Diameter x 48"
    Returns dict with keys: material, product_form, color, dimensions
    """
    out = {'material': '', 'product_form': '', 'color': '', 'dimensions': ''}
    if not title:
        return out

    # Strip brand prefix
    body = re.sub(r'^Plastic[\s-]Craft\s*[|\-]\s*', '', title, flags=re.IGNORECASE).strip()

    # Dimensions: anything with " or inch, or fractions followed by x
    dim_match = re.search(
        r'([\d/\.]+"?\s*(?:diameter\s*)?(?:x\s*[\d/\.]+"?\s*(?:diameter\s*)?)+)',
        body, re.IGNORECASE
    )
    if dim_match:
        out['dimensions'] = dim_match.group(1).strip().rstrip(',')

    # Split on comma to get parts
    parts = [p.strip() for p in body.split(',') if p.strip()]

    if parts:
        # First part is "Material ProductForm"
        mp = parts[0].split()
        if len(mp) >= 2:
            out['material']      = mp[0]
            out['product_form']  = ' '.join(mp[1:])
        else:
            out['material'] = parts[0]

    if len(parts) >= 2:
        # Second part is typically color/finish (skip if it looks like dimensions)
        candidate = parts[1]
        if not re.search(r'[\d/]', candidate):
            out['color'] = candidate

    return out


def _make_bullets(row: dict, parsed: dict) -> list[str]:
    """Generate minimal factual bullet points."""
    mat   = (row.get('material_type') or parsed['material'] or '').strip()
    form  = parsed['product_form'] or 'plastic'
    color = (row.get('color') or parsed['color'] or '').strip()
    dims  = parsed['dimensions'] or (row.get('size_description') or '').strip()
    mfr   = (row.get('manufacturer') or 'Plastic-Craft Products').strip()
    coo   = (row.get('country_of_origin') or '').strip()

    bullets = []

    # Bullet 1: material and form
    if mat:
        bullets.append(f'Material: {mat} {form}'.strip())
    else:
        bullets.append(f'Product type: {form}'.strip())

    # Bullet 2: dimensions
    if dims:
        bullets.append(f'Dimensions: {dims}')
    elif color:
        bullets.append(f'Color/finish: {color}')

    # Bullet 3: manufacturer / origin
    origin_line = f'Manufactured by {mfr}'
    if coo:
        origin_line += f' — made in the {coo}' if coo.upper() not in ('US', 'USA', 'UNITED STATES') \
            else ' — made in the USA'
    bullets.append(origin_line)

    return bullets


_FORM_KEYWORDS = ('sheet', 'rod', 'tube', 'bar', 'block', 'panel', 'strip', 'film')


def _make_description(row: dict, parsed: dict) -> str:
    mat   = (row.get('material_type') or parsed['material'] or 'plastic').strip()
    form  = parsed['product_form'] or 'sheet'
    dims  = parsed['dimensions'] or (row.get('size_description') or '').strip()
    color = (row.get('color') or parsed['color'] or '').strip()
    brand = (row.get('brand') or 'Plastic-Craft').strip()

    # Ensure product form (rod/sheet/tube) is present
    if not any(kw in form.lower() for kw in _FORM_KEYWORDS):
        form = 'sheet'  # safe fallback

    # For Acrylic/Nylon, flag if Cast/Extruded missing from form
    mat_lower = mat.lower()
    if any(m in mat_lower for m in ('acrylic', 'nylon')):
        form_lower = form.lower()
        if 'cast' not in form_lower and 'extruded' not in form_lower:
            existing = (row.get('new_title') or row.get('original_title') or '').lower()
            if 'cast' in existing:
                form = 'Cast ' + form
            elif 'extruded' in existing:
                form = 'Extruded ' + form

    desc = f'{brand} {mat} {form}'
    if color:
        desc += f', {color}'
    if dims:
        desc += f'. Dimensions: {dims}'
    desc += '.'
    return desc


def strip_row(row: dict) -> tuple[dict, list[str]]:
    """Return (stripped_row, list_of_changed_fields)."""
    new_row = dict(row)
    changed = []

    parsed = _parse_title(row.get('new_title') or row.get('original_title') or '')

    bullets  = _make_bullets(row, parsed)
    desc     = _make_description(row, parsed)

    for i, field in enumerate(BULLET_FIELDS):
        replacement = bullets[i] if i < len(bullets) else ''
        if new_row.get(field, '') != replacement:
            new_row[field] = replacement
            changed.append(field)

    for field in ['description']:
        if new_row.get(field, '') != desc:
            new_row[field] = desc
            changed.append(field)

    for field in ['subject_matter', 'intended_use', 'target_audience', 'recommended_uses']:
        if new_row.get(field, ''):
            new_row[field] = ''
            changed.append(field)

    # Leave backend_search_terms as-is (helps discoverability, not flagged)

    return new_row, changed


def main():
    ap = argparse.ArgumentParser(description='Strip all marketing copy from failing listings.')
    ap.add_argument('--input', required=True, metavar='FILE',
                    help='Feed CSV to strip (e.g. pc_filtered_feed_TIMESTAMP.csv)')
    ap.add_argument('--asins', metavar='ASIN1,ASIN2,...',
                    help='Only strip these ASINs (default: strip all rows)')
    ap.add_argument('--output', metavar='FILE',
                    help='Output filename (auto-named if omitted)')
    ap.add_argument('--dry-run', action='store_true',
                    help='Show what would change without writing output')
    args = ap.parse_args()

    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = SCRIPT_DIR / input_path
    if not input_path.exists():
        print(f'[ERROR] Input file not found: {input_path}')
        raise SystemExit(1)

    target_asins = set()
    if args.asins:
        target_asins = {a.strip().upper() for a in args.asins.split(',') if a.strip()}
        print(f'  ASIN filter: {len(target_asins)} ASINs')

    output_path = Path(args.output) if args.output else \
        SCRIPT_DIR / f'pc_stripped_{RUN_ID}.csv'

    rows_in  = []
    fieldnames = []

    with open(input_path, newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        for row in reader:
            rows_in.append(row)

    print(f'\n{"═" * 60}')
    print(f'  PC Strip Listing')
    print(f'  Input  : {input_path.name}  ({len(rows_in)} rows)')
    print(f'  Mode   : {"DRY RUN" if args.dry_run else "LIVE"}')
    print(f'{"═" * 60}\n')

    rows_out   = []
    strip_count = 0
    skip_count  = 0

    for row in rows_in:
        asin = (row.get('asin') or '').strip().upper()

        if target_asins and asin not in target_asins:
            rows_out.append(row)
            skip_count += 1
            continue

        stripped, changed = strip_row(row)
        rows_out.append(stripped)
        strip_count += 1

        if args.dry_run and changed:
            print(f'  {asin}  stripped: {", ".join(changed)}')

    if args.dry_run:
        print(f'\n  Would strip {strip_count} rows, pass through {skip_count} unchanged.')
        return

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows_out:
            writer.writerow(row)

    print(f'  Stripped       : {strip_count} rows')
    if skip_count:
        print(f'  Passed through : {skip_count} rows (not in --asins filter)')
    print(f'  Output file    : {output_path.name}')
    print(f'\n{"═" * 60}')
    print(f'\n  Next — push stripped listings:')
    print(f'      python pc_sp_api_push_v2.py --input "{output_path.name}" --resume')
    print(f'      python pc_sp_api_push.py   --input "{output_path.name}" --resume')
    print()


if __name__ == '__main__':
    main()
