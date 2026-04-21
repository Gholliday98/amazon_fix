#!/usr/bin/env python3
"""
pc_stage2_optimizer.py — Fast secondary attributes + description optimizer.

Generates the CSV needed by pc_sp_api_push_v2.py.
Runs independently of the stage 1 optimizer — safe to run in parallel.

Claude API is called ONCE per SKU for: description (with Q&A embedded).
Everything else — color, material_type, style, intended_use, etc. — is
computed deterministically from the product data. No retries, no self-healing,
no image review server. Just fast, focused description generation.

Output fields:
    sku, asin
    description                (with Q&A block embedded)
    subject_matter, intended_use, target_audience
    material_type, color, finish_type, style, size_description, item_form
    material_composition, recommended_uses
    manufacturer, country_of_origin, brand, part_number, model_number

Usage
-----
    python pc_stage2_optimizer.py                   # full run
    python pc_stage2_optimizer.py --limit 10        # test first 10
    python pc_stage2_optimizer.py --dry-run         # no API calls, compute only
    python pc_stage2_optimizer.py --input file.txt  # specify report file

Environment variables
---------------------
    ANTHROPIC_API_KEY
"""

import argparse
import csv
import glob
import importlib.util
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import anthropic
except ImportError:
    print('[ERROR] anthropic not installed.  Run: pip install anthropic')
    sys.exit(1)

# ─── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()
RUN_ID     = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_FILE = SCRIPT_DIR / f'pc_stage2_feed_{RUN_ID}.csv'
CHECKPOINT  = SCRIPT_DIR / 'pc_stage2_checkpoint.json'

sys.path.insert(0, str(SCRIPT_DIR))

# ─── Load synonym library ─────────────────────────────────────────────────────
try:
    from pc_synonym_library import get_subject_matter, get_intended_use, get_target_audience
except ImportError:
    print('[ERROR] pc_synonym_library.py not found in script directory')
    sys.exit(1)

# ─── Load helper functions from the main optimizer ───────────────────────────
def _load_optimizer():
    """
    Dynamically load pc_listing_optimizer_v4*.py to reuse its data and
    detection functions without duplicating code.
    """
    candidates = sorted(SCRIPT_DIR.glob('pc_listing_optimizer_v4*.py'))
    if not candidates:
        print('[ERROR] pc_listing_optimizer_v4*.py not found in script directory')
        sys.exit(1)
    path = candidates[0]
    spec = importlib.util.spec_from_file_location('pc_listing_optimizer_v4', path)
    mod  = importlib.util.module_from_spec(spec)
    sys.modules['pc_listing_optimizer_v4'] = mod  # register so self-import works
    try:
        spec.loader.exec_module(mod)
    except Exception as e:
        print(f'[ERROR] Failed to load optimizer module: {e}')
        sys.exit(1)
    return mod

_opt = _load_optimizer()

detect_material            = _opt.detect_material
detect_color_and_modifiers = _opt.detect_color_and_modifiers
detect_brand               = _opt.detect_brand
detect_pack                = _opt.detect_pack
extract_dimensions         = _opt.extract_dimensions
calculate_weight           = _opt.calculate_weight
calculate_box_dims         = _opt.calculate_box_dims
get_recommended_uses       = _opt.get_recommended_uses
get_rufus_qa_block         = _opt.get_rufus_qa_block
get_style                  = _opt.get_style
get_item_form              = _opt.get_item_form
get_brand                  = _opt.get_brand
needs_freight_notice       = _opt.needs_freight_notice
MATERIAL_COMPOSITION       = _opt.MATERIAL_COMPOSITION
MATERIAL_DATA              = _opt.MATERIAL_DATA
FREIGHT_BLURB              = _opt.FREIGHT_BLURB
THIRD_PARTY_BRANDS         = _opt.THIRD_PARTY_BRANDS

# ─── Constants ────────────────────────────────────────────────────────────────
MAX_DESC_CHARS = 2000
API_DELAY      = 0.5   # seconds between Claude calls
MAX_RETRIES    = 3

FOOTER = (
    'Plastic-Craft Products has been a trusted supplier of quality plastic '
    'materials since 1934. We are ISO 9001:2015 and AS9100D certified, ensuring '
    'rigorous quality management standards across our entire operation. All '
    'dimensions are held to +/- 0.010″ tolerances. Whether you’re a '
    'hobbyist, fabricator, engineer, or procurement professional — we have '
    'the materials and expertise to support your project.'
)

FIELDNAMES = [
    'sku', 'asin',
    'description',
    'subject_matter', 'intended_use', 'target_audience',
    'material_type', 'color', 'finish_type', 'style',
    'size_description', 'item_form',
    'material_composition', 'recommended_uses',
    'manufacturer', 'country_of_origin',
    'brand', 'part_number', 'model_number',
    'status', 'generated_at',
]


# ═══════════════════════════════════════════════════════════════════════════════
# Size description (deterministic)
# ═══════════════════════════════════════════════════════════════════════════════

def build_size_description(dims: dict) -> str:
    ptype = dims.get('product_type', '')
    def fmt(v):
        return f'{v}"' if v else ''

    if ptype == 'sheet':
        l, w, t = dims.get('length'), dims.get('width'), dims.get('thickness')
        if l and w and t:
            return f'{fmt(l)} L x {fmt(w)} W x {fmt(t)} T'
    elif ptype in ('rod', 'square_rod', 'triangular_rod'):
        l  = dims.get('length')
        od = dims.get('od') or dims.get('size') or dims.get('diameter')
        if l and od:
            return f'{fmt(l)} L x {fmt(od)} OD'
    elif ptype in ('tube', 'square_tube', 'triangular_tube'):
        l  = dims.get('length')
        od = dims.get('od')
        if l and od:
            return f'{fmt(l)} L x {fmt(od)} OD'
    elif ptype in ('sphere', 'half_sphere'):
        d = dims.get('diameter') or dims.get('od')
        if d:
            return f'{fmt(d)} Diameter'
    elif ptype == 'cube':
        s = dims.get('size')
        if s:
            return f'{fmt(s)} x {fmt(s)} x {fmt(s)}'
    # Generic fallback
    parts = [fmt(dims[k]) for k in ('length', 'width', 'thickness', 'od', 'size') if dims.get(k)]
    return ' x '.join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# Finish type (deterministic)
# ═══════════════════════════════════════════════════════════════════════════════

def get_finish_type(material_key: str, modifiers: list) -> str:
    if 'Transparent' in modifiers or 'Clear' in modifiers:
        return 'Clear'
    if 'Translucent' in modifiers:
        return 'Translucent'
    if material_key in ('acrylic', 'polycarbonate', 'petg'):
        return 'Polished'
    if material_key in ('hdpe', 'uhmw', 'nylon', 'delrin', 'ptfe'):
        return 'Standard'
    if material_key == 'abs':
        return 'Matte'
    return 'Standard'


# ═══════════════════════════════════════════════════════════════════════════════
# Description generation (Claude API)
# ═══════════════════════════════════════════════════════════════════════════════

def build_description_prompt(title: str, material_key: str, material_name: str,
                              dims: dict, color: str, weight: float,
                              box_dims: dict, freight_needed: bool) -> str:
    mat_data  = MATERIAL_DATA.get(material_key, {})
    rufus_qa  = get_rufus_qa_block(material_key, dims.get('product_type', ''))
    rec_uses  = get_recommended_uses(material_key, dims.get('product_type', ''))
    mat_comp  = MATERIAL_COMPOSITION.get(material_key, material_name)
    ptype     = dims.get('product_type', 'product')
    compliance = ', '.join(mat_data.get('compliance', []))
    key_stat   = mat_data.get('key_stat', '')
    tensile    = mat_data.get('tensile_psi', '')
    temp       = mat_data.get('service_temp', '')
    pain_pts   = ' | '.join(mat_data.get('pain_points', []))

    freight_line = (
        f'\nIMPORTANT: End description with this exact freight notice:\n{FREIGHT_BLURB}'
        if freight_needed else ''
    )

    return f"""Write an Amazon product description for this listing.

Product: {title}
Material: {material_name} | Composition: {mat_comp}
Product type: {ptype} | Color: {color or 'Natural'}
Key stat: {key_stat}
Tensile strength: {tensile} PSI
Service temp: {temp}
Compliance: {compliance}
Recommended uses: {rec_uses}
Pain points: {pain_pts}
{freight_line}

INSTRUCTIONS:
1. START with this exact Q&A block:
{rufus_qa}

2. Write 2-3 paragraphs of conversational copy covering:
   - What this material is and why buyers choose it for {ptype} applications
   - Specific industries, projects, and use cases
   - Fabrication notes (cutting, bonding, machining, forming)
   - Include: "Stocked and ships from West Nyack, NY"

3. END with EXACTLY this text (do not alter a single word):
{FOOTER}

RULES:
- Total description MUST be under {MAX_DESC_CHARS} characters
- No bullet points — flowing paragraphs only
- Return ONLY the description text, no JSON, no markdown, no labels"""


def generate_description(client: anthropic.Anthropic, prompt: str) -> str | None:
    for attempt in range(MAX_RETRIES):
        try:
            r = client.messages.create(
                model='claude-sonnet-4-6',
                max_tokens=800,
                messages=[{'role': 'user', 'content': prompt}]
            )
            text = r.content[0].text.strip()
            # Policy compliance filter
            try:
                from pc_policy_validator import validate_and_fix
                text, violations = validate_and_fix(text, 'description')
                for v in violations:
                    print(f'    [POLICY] {v}')
            except ImportError:
                pass
            if len(text) > MAX_DESC_CHARS:
                text = text[:MAX_DESC_CHARS].rsplit(' ', 1)[0]
            return text
        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            print(f'    [THROTTLE] Rate limit — waiting {wait}s')
            time.sleep(wait)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f'    [ERROR] Claude API: {e}')
                return None
            time.sleep(3 * (attempt + 1))
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Input file discovery
# ═══════════════════════════════════════════════════════════════════════════════

def find_input_file() -> Path:
    patterns = [
        '*All_Listings_Report*.txt',
        '*All+Listings+Report*.txt',
        '*all_listings_report*.txt',
    ]
    matches = []
    for pat in patterns:
        matches.extend(SCRIPT_DIR.glob(pat))
    if not matches:
        print(f'[ERROR] No All Listings Report .txt found in {SCRIPT_DIR}')
        print('        Download one from Seller Central → Reports → Inventory Reports')
        sys.exit(1)
    return max(matches, key=os.path.getmtime)


def load_listings(path: Path, limit: int = 0) -> list[dict]:
    rows = []
    with open(path, newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            status = row.get('status', '').strip()
            if status in ('Active', 'Inactive', 'Incomplete'):
                rows.append(row)
                if limit and len(rows) >= limit:
                    break
    return rows


# ═══════════════════════════════════════════════════════════════════════════════
# Checkpoint
# ═══════════════════════════════════════════════════════════════════════════════

def load_checkpoint() -> set:
    if CHECKPOINT.exists():
        try:
            data = json.loads(CHECKPOINT.read_text())
            done = set(data.get('completed', []))
            print(f'  Checkpoint: {len(done)} SKUs already done — skipping')
            return done
        except Exception:
            pass
    return set()


def save_checkpoint(completed: set):
    CHECKPOINT.write_text(json.dumps({'completed': list(completed)}, indent=2))


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args():
    ap = argparse.ArgumentParser(
        description='Generate description + secondary attributes CSV for stage 2 push.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--dry-run', action='store_true',
                    help='Compute all fields but skip Claude API calls')
    ap.add_argument('--limit', type=int, default=0, metavar='N',
                    help='Process only first N listings')
    ap.add_argument('--input', metavar='FILE',
                    help='All Listings Report .txt file (default: auto-detect)')
    ap.add_argument('--no-checkpoint', action='store_true',
                    help='Ignore existing checkpoint and restart from scratch')
    return ap.parse_args()


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    args = parse_args()

    # Locate input file
    if args.input:
        input_path = Path(args.input)
        if not input_path.is_absolute():
            input_path = SCRIPT_DIR / input_path
        if not input_path.exists():
            print(f'[ERROR] File not found: {input_path}')
            sys.exit(1)
    else:
        input_path = find_input_file()

    # Claude client
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key and not args.dry_run:
        print('[ERROR] ANTHROPIC_API_KEY not set')
        sys.exit(1)
    client = anthropic.Anthropic(api_key=api_key) if api_key else None

    print('\n' + '═' * 60)
    print(f'  PC Stage 2 Optimizer  ({"DRY RUN" if args.dry_run else "LIVE"})')
    print(f'  Run ID : {RUN_ID}')
    print(f'  Input  : {input_path.name}')
    print(f'  Output : {OUTPUT_FILE.name}')
    print('═' * 60)

    # Load listings
    rows = load_listings(input_path, args.limit)
    print(f'\n  Loaded {len(rows):,} listings from report')

    # Checkpoint
    completed = set() if args.no_checkpoint else load_checkpoint()
    remaining = [r for r in rows if r.get('seller-sku', '').strip() not in completed]
    print(f'  Remaining: {len(remaining):,}\n')

    if not remaining:
        print('  Nothing to do.')
        return

    # Open output CSV (append so checkpoint resumes work)
    out_exists = OUTPUT_FILE.exists()
    fh = open(OUTPUT_FILE, 'a', newline='', encoding='utf-8')
    writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
    if not out_exists:
        writer.writeheader()
    fh.flush()

    stats = {'success': 0, 'error': 0, 'skipped': 0}

    try:
        for n, row in enumerate(remaining, 1):
            sku   = row.get('seller-sku', '').strip()
            asin  = row.get('asin1', '').strip()
            title = row.get('item-name', '').strip()

            print(f'  [{n}/{len(remaining)}] {sku}')

            # ── Detect product properties ─────────────────────────────────────
            material_key, material_name = detect_material(title)
            modifiers, cast_ext, solid_hollow, color = detect_color_and_modifiers(title)
            is_third_party, brand_name, _ = detect_brand(title)
            dims     = extract_dimensions(title)
            weight   = calculate_weight(material_key, dims)
            box_dims = calculate_box_dims(dims)
            ptype    = dims.get('product_type', 'accessory')

            freight_needed = needs_freight_notice(dims, weight, box_dims, weight)

            # ── Compute deterministic secondary fields ────────────────────────
            subject_matter    = get_subject_matter(material_key, ptype)
            intended_use      = get_intended_use(material_key, ptype)
            target_audience   = get_target_audience(material_key, ptype)
            material_type_val = material_name
            color_val         = color or ''
            finish_type       = get_finish_type(material_key, modifiers)
            style_val         = get_style(ptype, cast_ext, material_name)
            size_desc         = build_size_description(dims)
            item_form_val     = get_item_form(ptype, solid_hollow)
            mat_comp          = MATERIAL_COMPOSITION.get(material_key, '')
            rec_uses          = get_recommended_uses(material_key, ptype)
            brand_val         = get_brand(is_third_party, brand_name)

            # ── DRY RUN — skip Claude call ────────────────────────────────────
            if args.dry_run:
                print(f'    [DRY RUN] material={material_key} type={ptype} color={color_val}')
                writer.writerow({
                    'sku': sku, 'asin': asin,
                    'description': '[DRY RUN — not generated]',
                    'subject_matter': subject_matter,
                    'intended_use': intended_use,
                    'target_audience': target_audience,
                    'material_type': material_type_val,
                    'color': color_val,
                    'finish_type': finish_type,
                    'style': style_val,
                    'size_description': size_desc,
                    'item_form': item_form_val,
                    'material_composition': mat_comp,
                    'recommended_uses': rec_uses,
                    'manufacturer': 'Plastic-Craft Products',
                    'country_of_origin': 'United States',
                    'brand': brand_val,
                    'part_number': sku,
                    'model_number': sku,
                    'status': 'dry_run',
                    'generated_at': datetime.now().isoformat(timespec='seconds'),
                })
                fh.flush()
                completed.add(sku)
                stats['success'] += 1
                continue

            # ── Generate description via Claude ───────────────────────────────
            prompt = build_description_prompt(
                title, material_key, material_name,
                dims, color_val, weight, box_dims, freight_needed
            )

            desc = generate_description(client, prompt)
            time.sleep(API_DELAY)

            if not desc:
                print(f'    [ERROR] Description generation failed')
                writer.writerow({
                    'sku': sku, 'asin': asin,
                    'description': '',
                    'subject_matter': subject_matter,
                    'intended_use': intended_use,
                    'target_audience': target_audience,
                    'material_type': material_type_val,
                    'color': color_val,
                    'finish_type': finish_type,
                    'style': style_val,
                    'size_description': size_desc,
                    'item_form': item_form_val,
                    'material_composition': mat_comp,
                    'recommended_uses': rec_uses,
                    'manufacturer': 'Plastic-Craft Products',
                    'country_of_origin': 'United States',
                    'brand': brand_val,
                    'part_number': sku,
                    'model_number': sku,
                    'status': 'error',
                    'generated_at': datetime.now().isoformat(timespec='seconds'),
                })
                fh.flush()
                stats['error'] += 1
                continue

            print(f'    [OK] {len(desc)} chars | freight={freight_needed}')
            writer.writerow({
                'sku': sku, 'asin': asin,
                'description': desc,
                'subject_matter': subject_matter,
                'intended_use': intended_use,
                'target_audience': target_audience,
                'material_type': material_type_val,
                'color': color_val,
                'finish_type': finish_type,
                'style': style_val,
                'size_description': size_desc,
                'item_form': item_form_val,
                'material_composition': mat_comp,
                'recommended_uses': rec_uses,
                'manufacturer': 'Plastic-Craft Products',
                'country_of_origin': 'United States',
                'brand': brand_val,
                'part_number': sku,
                'model_number': sku,
                'status': 'success',
                'generated_at': datetime.now().isoformat(timespec='seconds'),
            })
            fh.flush()
            completed.add(sku)
            stats['success'] += 1

            # Save checkpoint every 25 SKUs
            if len(completed) % 25 == 0:
                save_checkpoint(completed)

    finally:
        fh.close()
        save_checkpoint(completed)

    print('\n' + '═' * 60)
    print('  DONE')
    print(f'  Success : {stats["success"]}')
    print(f'  Errors  : {stats["error"]}')
    print(f'\n  Output  → {OUTPUT_FILE.name}')
    print(f'  Push with: python pc_sp_api_push_v2.py --input {OUTPUT_FILE.name}')
    print('═' * 60 + '\n')


if __name__ == '__main__':
    main()
