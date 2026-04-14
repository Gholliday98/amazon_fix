"""
Plastic-Craft Amazon Listing Optimizer v3.0
Full self-correcting, resumable, production-ready catalog optimizer

Self-correction layers:
  L1 - Pre-API input validation and secondary parse attempts
  L2 - Post-API output validation on every field
  L3 - Auto-correction without retry (trim, append, reformat)
  L4 - Targeted single-field retry with tighter prompt
  L5 - Escalation logging with partial data preservation

Additional features:
  - Checkpoint save every 50 listings (resume from interruption)
  - Freight notice auto-detection and injection
  - Material data points (tensile strength, temp ratings, etc.)
  - Feature + Benefit + Proof bullet structure
  - Pain point resolution in descriptions
  - Compliance flagging (FDA, NSF, RoHS, ASTM)
  - Video recommendation flagging
  - USA stocked callout in backend terms
"""

import csv
import json
import re
import time
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, '/home/claude')
from pc_synonym_library import (
    get_base_backend_terms,
    clean_and_trim_backend_terms,
    get_subject_matter,
    get_intended_use,
    get_target_audience,
)

import anthropic
import pc_listing_optimizer_v3 as _self_module
from pc_logger import setup_logger, write_run_summary
from pc_self_heal import SelfHealingEngine, DimensionSanityChecker

# ============================================================
# CONFIGURATION
# ============================================================

INPUT_FILE = '/mnt/user-data/uploads/1775844707902_All_Listings_Report_02-02-2026.txt'
LOG_DIR = '/home/claude'
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = f'/home/claude/pc_amazon_feed_v3_{RUN_ID}.csv'
ERROR_LOG = f'/home/claude/pc_errors_v3_{RUN_ID}.csv'
CHECKPOINT_FILE = '/home/claude/pc_checkpoint.json'

TEST_MODE = True
TEST_LIMIT = 8

# Initialize logger (done early so all functions can use it)
logger, jlog, RUN_ID = setup_logger('pc_optimizer', LOG_DIR)

WEIGHT_BUFFER = 1.10
BOX_BUFFER = 2.0
CHECKPOINT_INTERVAL = 50
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds between retries

# ============================================================
# LIMITS
# ============================================================

LIMITS = {
    'title': 200,
    'bullet': 500,
    'description': 2000,
    'backend_search_terms': 249,
}

# ============================================================
# FREIGHT DETECTION
# ============================================================

FREIGHT_BLURB = (
    "FREIGHT SHIPPING NOTICE: This item ships via freight carrier. "
    "Orders delivered to a residential address are subject to an additional "
    "residential delivery surcharge. If a liftgate is required for unloading, "
    "an additional liftgate fee will apply. Please review freight delivery "
    "requirements before placing your order."
)

FREIGHT_TRIGGERS = {
    'sheet_length': 48,
    'sheet_width': 48,
    'rod_tube_length': 96,
    'weight_lbs': 70,
    'box_dimension': 96,
}

def needs_freight_notice(dims, weight, box_dims):
    ptype = dims.get('product_type', '')
    if weight and weight > FREIGHT_TRIGGERS['weight_lbs']:
        return True
    if box_dims:
        for val in box_dims.values():
            if val and val > FREIGHT_TRIGGERS['box_dimension']:
                return True
    if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
        if dims.get('length', 0) > FREIGHT_TRIGGERS['sheet_length']:
            return True
        if dims.get('width', 0) > FREIGHT_TRIGGERS['sheet_width']:
            return True
    elif ptype in ('rod', 'tube'):
        if dims.get('length', 0) > FREIGHT_TRIGGERS['rod_tube_length']:
            return True
    return False

# ============================================================
# MATERIAL DATA POINTS
# ============================================================

MATERIAL_DATA = {
    'acrylic': {
        'tensile_psi': 8000,
        'impact_strength': '0.4 ft-lb/in notched',
        'heat_deflection': '195°F (91°C)',
        'service_temp': '-40°F to 180°F',
        'light_transmission': '92%',
        'key_stat': '92% light transmission — the clearest plastic available',
        'compliance': ['ASTM D4802'],
        'pain_points': [
            'Will this be clear enough for my display?',
            'Can I cut this with my tools?',
            'Will this yellow outdoors?',
        ],
    },
    'abs': {
        'tensile_psi': 5200,
        'impact_strength': '3.0 ft-lb/in notched',
        'heat_deflection': '180°F (82°C)',
        'service_temp': '-40°F to 180°F',
        'key_stat': '3.0 ft-lb/in impact strength — tough enough for demanding applications',
        'compliance': ['ASTM D4673', 'RoHS Compliant'],
        'pain_points': [
            'Is this rigid enough for my enclosure?',
            'Can I paint or bond this?',
            'Will this hold up in heat?',
        ],
    },
    'hdpe': {
        'tensile_psi': 4500,
        'impact_strength': 'No break (notched)',
        'heat_deflection': '180°F (82°C)',
        'service_temp': '-220°F to 180°F',
        'key_stat': 'FDA and NSF compliant — safe for direct food contact',
        'compliance': ['FDA 21 CFR', 'NSF Compliant', 'ASTM D4976', 'USDA Approved'],
        'pain_points': [
            'Is this actually food safe?',
            'Will this warp or absorb odors?',
            'Can I use this outside?',
        ],
    },
    'polycarbonate': {
        'tensile_psi': 9500,
        'impact_strength': '16.0 ft-lb/in — 250x stronger than glass',
        'heat_deflection': '270°F (132°C)',
        'service_temp': '-40°F to 280°F',
        'key_stat': '250x stronger than glass — virtually unbreakable',
        'compliance': ['ASTM D3935', 'UL 94 V-2 Flame Rated'],
        'pain_points': [
            'Is this actually shatterproof?',
            'Will this hold up in extreme temperatures?',
            'Is this UV stable for outdoor use?',
        ],
    },
    'nylon': {
        'tensile_psi': 12000,
        'impact_strength': '1.2 ft-lb/in notched',
        'heat_deflection': '400°F (204°C)',
        'service_temp': '-40°F to 250°F continuous',
        'key_stat': '12,000 PSI tensile strength with self-lubricating properties',
        'compliance': ['ASTM D4066', 'FDA Compliant grades available'],
        'pain_points': [
            'Will this machine cleanly on my lathe?',
            'Does this absorb moisture?',
            'Is this strong enough for my gear application?',
        ],
    },
    'uhmw': {
        'tensile_psi': 6500,
        'impact_strength': 'No break — highest impact of any thermoplastic',
        'heat_deflection': '180°F (82°C)',
        'service_temp': '-300°F to 180°F',
        'key_stat': 'Lowest friction coefficient of any plastic — superior wear resistance',
        'compliance': ['FDA 21 CFR', 'NSF Compliant', 'USDA Approved', 'ASTM D4020'],
        'pain_points': [
            'How long will this last in my wear application?',
            'Is this food safe for my conveyor?',
            'Can I weld this?',
        ],
    },
    'pvc': {
        'tensile_psi': 7500,
        'impact_strength': '0.9 ft-lb/in notched',
        'heat_deflection': '158°F (70°C)',
        'service_temp': '32°F to 140°F',
        'key_stat': 'Resistant to 90%+ of acids, bases, and salts',
        'compliance': ['ASTM D1784', 'UL 94 V-0 Flame Rated'],
        'pain_points': [
            'Will this hold up to my chemicals?',
            'Is this flame retardant?',
            'Can I cement/weld this?',
        ],
    },
    'polypropylene': {
        'tensile_psi': 4800,
        'impact_strength': '1.2 ft-lb/in notched',
        'heat_deflection': '200°F (93°C)',
        'service_temp': '-20°F to 200°F',
        'key_stat': 'Unique living hinge capability — flexes millions of times without fatigue',
        'compliance': ['FDA 21 CFR', 'NSF Compliant', 'ASTM D4101'],
        'pain_points': [
            'Is this food safe?',
            'Can this be used for living hinges?',
            'How chemical resistant is this?',
        ],
    },
    'peek': {
        'tensile_psi': 14500,
        'impact_strength': '1.6 ft-lb/in notched',
        'heat_deflection': '600°F (316°C)',
        'service_temp': 'Continuous 480°F (250°C)',
        'key_stat': 'Continuous service at 480°F — one of the highest performance thermoplastics available',
        'compliance': ['ASTM D6262', 'USP Class VI', 'FDA Compliant', 'RoHS Compliant'],
        'pain_points': [
            'Will this hold up at high temperature?',
            'Is this biocompatible for my medical application?',
            'Can this replace metal in my application?',
        ],
    },
    'delrin': {
        'tensile_psi': 10000,
        'impact_strength': '1.4 ft-lb/in notched',
        'heat_deflection': '257°F (125°C)',
        'service_temp': '-40°F to 180°F',
        'key_stat': '< 0.2% moisture absorption — exceptional dimensional stability',
        'compliance': ['ASTM D4181', 'FDA Compliant grades available'],
        'pain_points': [
            'Will this hold tight tolerances after machining?',
            'How does this compare to nylon for gears?',
            'Is this self-lubricating?',
        ],
    },
    'ptfe': {
        'tensile_psi': 3500,
        'impact_strength': '3.0 ft-lb/in notched',
        'heat_deflection': 'Continuous 500°F (260°C)',
        'service_temp': '-328°F to 500°F',
        'key_stat': 'Lowest friction coefficient of any solid material — near-zero stick',
        'compliance': ['FDA 21 CFR', 'USP Class VI', 'RoHS Compliant', 'ASTM D3294'],
        'pain_points': [
            'Is this truly non-stick?',
            'Will this hold up to my aggressive chemicals?',
            'Can this be used in high temperature applications?',
        ],
    },
}

# ============================================================
# MATERIAL DENSITIES
# ============================================================

DENSITIES = {
    'acrylic': 0.0433, 'pmma': 0.0433,
    'abs': 0.0385,
    'hdpe': 0.0335,
    'pvc': 0.0514,
    'polycarbonate': 0.0433, 'pc': 0.0433,
    'nylon': 0.0411,
    'uhmw': 0.0338,
    'polypropylene': 0.0324, 'pp': 0.0324,
    'peek': 0.0481,
    'delrin': 0.0520, 'acetal': 0.0520, 'pom': 0.0520,
    'ptfe': 0.0781, 'teflon': 0.0781,
}

# ============================================================
# FRACTION CONVERSION
# ============================================================

def decimal_to_fraction(decimal):
    if decimal is None:
        return None
    if decimal == int(decimal):
        return str(int(decimal))
    sixteenths = round(decimal * 16)
    def gcd(a, b):
        while b: a, b = b, a % b
        return a
    whole = sixteenths // 16
    remainder = sixteenths % 16
    if remainder == 0:
        return str(whole)
    g = gcd(remainder, 16)
    num = remainder // g
    den = 16 // g
    if whole > 0:
        return f'{whole}-{num}/{den}'
    return f'{num}/{den}'

def format_dim(decimal):
    if decimal is None:
        return None
    if decimal == int(decimal):
        return f'{int(decimal)}"'
    frac = decimal_to_fraction(decimal)
    dec_str = f'{decimal:.4g}'
    return f'{frac}" ({dec_str}")'

# ============================================================
# DIMENSION PARSING
# ============================================================

def parse_fraction(text):
    text = text.strip()
    mixed = re.match(r'(\d+)-(\d+)/(\d+)', text)
    if mixed:
        w, n, d = mixed.groups()
        return int(w) + int(n) / int(d)
    simple = re.match(r'(\d+)/(\d+)', text)
    if simple:
        n, d = simple.groups()
        return int(n) / int(d)
    try:
        return float(text)
    except:
        return None

def extract_dimensions(title):
    title_lower = title.lower()
    product_type = 'unknown'

    if 'cutting board' in title_lower:
        product_type = 'cutting_board_oem' if (
            'oem' in title_lower or 'replacement' in title_lower
        ) else 'cutting_board'
    elif 'tube' in title_lower:
        product_type = 'tube'
    elif 'rod' in title_lower:
        product_type = 'rod'
    elif 'sheet' in title_lower:
        product_type = 'sheet'
    elif any(x in title_lower for x in ['weld-on', 'cement', 'adhesive', 'solvent']):
        product_type = 'adhesive'
    else:
        product_type = 'accessory'

    dims = {'product_type': product_type}
    num = r'(\d+(?:-\d+/\d+|\.\d+|/\d+)?)'

    try:
        if product_type in ('sheet', 'cutting_board', 'cutting_board_oem'):
            thick = re.search(rf'{num}["\s]*(?:thick|t\b)', title, re.IGNORECASE)
            wl = re.search(
                rf'{num}["\s]*w?\s*x\s*{num}["\s]*l?',
                title, re.IGNORECASE
            )
            if thick:
                val = parse_fraction(thick.group(1))
                if val and 0 < val < 20:
                    dims['thickness'] = val
            if wl:
                w = parse_fraction(wl.group(1))
                l = parse_fraction(wl.group(2))
                if w and l and 0 < w < 200 and 0 < l < 200:
                    dims['width'] = w
                    dims['length'] = l

        elif product_type == 'rod':
            len_m = re.search(rf'{num}["\s]*(?:length|l\b)', title, re.IGNORECASE)
            dia_m = re.search(rf'{num}["\s]*(?:diameter|od\b)', title, re.IGNORECASE)
            if len_m:
                val = parse_fraction(len_m.group(1))
                if val and 0 < val < 300:
                    dims['length'] = val
            if dia_m:
                val = parse_fraction(dia_m.group(1))
                if val and 0 < val < 24:
                    dims['od'] = val
            if not len_m or not dia_m:
                pat = re.search(
                    rf'{num}["\s]*(?:length|l)?\s*x\s*{num}["\s]*(?:diameter|od)?',
                    title, re.IGNORECASE
                )
                if pat:
                    if 'length' not in dims:
                        v = parse_fraction(pat.group(1))
                        if v and 0 < v < 300:
                            dims['length'] = v
                    if 'od' not in dims:
                        v = parse_fraction(pat.group(2))
                        if v and 0 < v < 24:
                            dims['od'] = v

        elif product_type == 'tube':
            id_m = re.search(rf'{num}["\s]*id', title, re.IGNORECASE)
            od_m = re.search(rf'{num}["\s]*od', title, re.IGNORECASE)
            len_m = re.search(rf'{num}["\s]*(?:l\b|length)', title, re.IGNORECASE)
            if id_m:
                val = parse_fraction(id_m.group(1))
                if val and 0 < val < 24:
                    dims['id'] = val
            if od_m:
                val = parse_fraction(od_m.group(1))
                if val and 0 < val < 24:
                    dims['od'] = val
            if len_m:
                val = parse_fraction(len_m.group(1))
                if val and 0 < val < 300:
                    dims['length'] = val
    except Exception as e:
        pass

    return dims

# ============================================================
# MATERIAL + COLOR DETECTION
# ============================================================

def detect_material(title):
    title_lower = title.lower()
    materials = [
        ('peek', 'PEEK'), ('polycarbonate', 'Polycarbonate'),
        ('polypropylene', 'Polypropylene'), ('uhmw', 'UHMW'),
        ('hdpe', 'HDPE'), ('acrylic', 'Acrylic'), ('abs', 'ABS'),
        ('nylon', 'Nylon'), ('pvc', 'PVC'), ('delrin', 'Delrin'),
        ('acetal', 'Acetal'), ('ptfe', 'PTFE'), ('teflon', 'PTFE'),
    ]
    for key, name in materials:
        if key in title_lower:
            return key, name
    if 'cutting board' in title_lower:
        return 'hdpe', 'HDPE'
    return 'unknown', 'Unknown'

def detect_color(title):
    title_lower = title.lower()
    modifiers = []
    for mod in ['transparent', 'translucent', 'opaque']:
        if mod in title_lower:
            modifiers.append(mod.capitalize())
    cast_ext = None
    if 'cast' in title_lower:
        cast_ext = 'Cast'
    elif 'extruded' in title_lower:
        cast_ext = 'Extruded'
    color_map = {
        'red': 'Red', 'orange': 'Orange', 'yellow': 'Yellow',
        'green': 'Green', 'blue': 'Blue', 'indigo': 'Indigo',
        'violet': 'Violet', 'purple': 'Purple', 'black': 'Black',
        'white': 'White', 'clear': 'Clear', 'natural': 'Natural',
        'brown': 'Brown', 'gray': 'Gray', 'grey': 'Gray',
        'pink': 'Pink', 'bronze': 'Bronze', 'woodgrain': 'Woodgrain',
    }
    color = None
    for key, val in color_map.items():
        if re.search(rf'\b{key}\b', title_lower):
            color = val
            break
    return modifiers, cast_ext, color

def detect_pack(title):
    m = re.search(r'pack\s+of\s+(\d+)', title, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'\((\d+)\)\s*$', title)
    if m: return int(m.group(1))
    return None

# ============================================================
# WEIGHT + BOX
# ============================================================

def calculate_weight(material_key, dims):
    density = DENSITIES.get(material_key)
    if not density: return None
    ptype = dims.get('product_type')
    volume = None
    try:
        if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
            if all(k in dims for k in ('thickness', 'width', 'length')):
                volume = dims['thickness'] * dims['width'] * dims['length']
        elif ptype == 'rod':
            if all(k in dims for k in ('od', 'length')):
                r = dims['od'] / 2
                volume = 3.14159 * r**2 * dims['length']
        elif ptype == 'tube':
            if all(k in dims for k in ('id', 'od', 'length')):
                volume = 3.14159 * (
                    (dims['od']/2)**2 - (dims['id']/2)**2
                ) * dims['length']
    except:
        return None
    if volume and volume > 0:
        weight = round(volume * density * WEIGHT_BUFFER, 2)
        # Sanity check — flag impossibly heavy or light
        if weight > 500 or weight < 0.001:
            return None
        return weight
    return None

def calculate_box_dims(dims):
    ptype = dims.get('product_type')
    try:
        if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
            if all(k in dims for k in ('thickness', 'width', 'length')):
                return {
                    'box_length': round(dims['length'] + BOX_BUFFER, 1),
                    'box_width': round(dims['width'] + BOX_BUFFER, 1),
                    'box_height': round(dims['thickness'] + BOX_BUFFER, 1),
                }
        elif ptype == 'rod':
            if all(k in dims for k in ('od', 'length')):
                return {
                    'box_length': round(dims['length'] + BOX_BUFFER, 1),
                    'box_width': round(dims['od'] + BOX_BUFFER, 1),
                    'box_height': round(dims['od'] + BOX_BUFFER, 1),
                }
        elif ptype == 'tube':
            if all(k in dims for k in ('od', 'length')):
                return {
                    'box_length': round(dims['length'] + BOX_BUFFER, 1),
                    'box_width': round(dims['od'] + BOX_BUFFER, 1),
                    'box_height': round(dims['od'] + BOX_BUFFER, 1),
                }
    except:
        pass
    return {}

# ============================================================
# L1 — PRE-API INPUT VALIDATION
# ============================================================

def validate_input(dims, material_key, title):
    issues = []
    ptype = dims.get('product_type', 'unknown')

    if ptype == 'unknown':
        issues.append('PRODUCT_TYPE_UNKNOWN')
    if material_key == 'unknown':
        issues.append('MATERIAL_UNKNOWN')

    if ptype == 'sheet':
        if 'thickness' not in dims:
            issues.append('MISSING_THICKNESS')
        if 'width' not in dims or 'length' not in dims:
            issues.append('MISSING_WIDTH_LENGTH')
    elif ptype == 'rod':
        if 'od' not in dims:
            issues.append('MISSING_OD')
        if 'length' not in dims:
            issues.append('MISSING_LENGTH')
    elif ptype == 'tube':
        if 'od' not in dims or 'id' not in dims:
            issues.append('MISSING_ID_OD')
        if 'length' not in dims:
            issues.append('MISSING_LENGTH')

    return issues

# ============================================================
# L2 — POST-API OUTPUT VALIDATION
# ============================================================

CERTIFICATION_CLOSING = "Plastic-Craft Products has been a trusted supplier"
CAPS_PATTERN = re.compile(r'^[A-Z][A-Z\s&/+\-,]{2,}—')

def validate_output(content, freight_needed):
    issues = []

    # Title checks
    title = content.get('title', '')
    if not title:
        issues.append('TITLE_MISSING')
    elif len(title) > LIMITS['title']:
        issues.append(f'TITLE_TOO_LONG:{len(title)}')
    if title and 'Plastic-Craft' not in title[:100]:
        issues.append('TITLE_MISSING_BRAND')

    # Bullet checks
    for i in range(1, 6):
        bullet = content.get(f'bullet{i}', '')
        if not bullet:
            issues.append(f'BULLET{i}_MISSING')
        elif len(bullet) > LIMITS['bullet']:
            issues.append(f'BULLET{i}_TOO_LONG:{len(bullet)}')
        if bullet and not CAPS_PATTERN.match(bullet):
            issues.append(f'BULLET{i}_NO_CAPS_LEAD')

    # Description checks
    desc = content.get('description', '')
    if not desc:
        issues.append('DESCRIPTION_MISSING')
    elif len(desc) > LIMITS['description']:
        issues.append(f'DESCRIPTION_TOO_LONG:{len(desc)}')
    if desc and CERTIFICATION_CLOSING not in desc:
        issues.append('DESCRIPTION_MISSING_CERTIFICATION')
    if freight_needed and FREIGHT_BLURB[:30] not in desc:
        issues.append('FREIGHT_NOTICE_MISSING')

    # Backend terms check
    backend = content.get('backend_search_terms', '')
    if not backend:
        issues.append('BACKEND_MISSING')
    elif len(backend.encode('utf-8')) > LIMITS['backend_search_terms']:
        issues.append(f'BACKEND_TOO_LONG:{len(backend.encode())}bytes')

    # Required fields
    for field in ['material_type', 'color', 'size_description']:
        if not content.get(field):
            issues.append(f'{field.upper()}_MISSING')

    return issues

# ============================================================
# L3 — AUTO-CORRECTION (no retry needed)
# ============================================================

CERTIFICATION_PARAGRAPH = (
    "Plastic-Craft Products has been a trusted supplier of quality plastic "
    "materials since 1934. We are ISO 9001:2015 and AS9100D certified, ensuring "
    "rigorous quality management standards across our entire operation. All "
    "dimensions are held to +/- 0.010\" tolerances. Whether you're a hobbyist, "
    "fabricator, engineer, or procurement professional — we have the materials "
    "and expertise to support your project."
)

def auto_correct(content, issues, freight_needed):
    corrected = content.copy()
    fixes_applied = []

    for issue in issues:

        # Fix title too long — trim at last comma before limit
        if issue.startswith('TITLE_TOO_LONG'):
            title = corrected.get('title', '')
            while len(title) > LIMITS['title']:
                last_comma = title[:LIMITS['title']].rfind(',')
                if last_comma > 100:
                    title = title[:last_comma].strip()
                else:
                    title = title[:LIMITS['title']].strip()
                    break
            corrected['title'] = title
            fixes_applied.append('TITLE_TRIMMED')

        # Fix backend too long — trim last term
        elif issue.startswith('BACKEND_TOO_LONG'):
            backend = corrected.get('backend_search_terms', '')
            while len(backend.encode('utf-8')) > LIMITS['backend_search_terms']:
                parts = backend.rsplit(' ', 1)
                if len(parts) > 1:
                    backend = parts[0]
                else:
                    break
            corrected['backend_search_terms'] = backend
            fixes_applied.append('BACKEND_TRIMMED')

        # Fix missing certification closing
        elif issue == 'DESCRIPTION_MISSING_CERTIFICATION':
            desc = corrected.get('description', '')
            # Try to append if space allows
            combined = desc + '\n\n' + CERTIFICATION_PARAGRAPH
            if len(combined) <= LIMITS['description']:
                corrected['description'] = combined
                fixes_applied.append('CERTIFICATION_APPENDED')
            else:
                # Trim description to make room
                available = LIMITS['description'] - len(CERTIFICATION_PARAGRAPH) - 4
                corrected['description'] = desc[:available].strip() + '\n\n' + CERTIFICATION_PARAGRAPH
                fixes_applied.append('DESCRIPTION_TRIMMED_AND_CERTIFICATION_APPENDED')

        # Fix missing freight notice
        elif issue == 'FREIGHT_NOTICE_MISSING' and freight_needed:
            desc = corrected.get('description', '')
            combined = desc + '\n\n' + FREIGHT_BLURB
            if len(combined) <= LIMITS['description']:
                corrected['description'] = combined
            else:
                available = LIMITS['description'] - len(FREIGHT_BLURB) - 4
                corrected['description'] = desc[:available].strip() + '\n\n' + FREIGHT_BLURB
            fixes_applied.append('FREIGHT_NOTICE_APPENDED')

        # Fix description too long
        elif issue.startswith('DESCRIPTION_TOO_LONG'):
            desc = corrected.get('description', '')
            if CERTIFICATION_CLOSING in desc:
                cert_idx = desc.index(CERTIFICATION_CLOSING)
                cert_text = desc[cert_idx:]
                body = desc[:cert_idx].strip()
                available = LIMITS['description'] - len(cert_text) - 4
                corrected['description'] = body[:available].strip() + '\n\n' + cert_text
            else:
                corrected['description'] = desc[:LIMITS['description']].strip()
            fixes_applied.append('DESCRIPTION_TRIMMED')

        # Fix bullet too long
        elif re.match(r'BULLET(\d)_TOO_LONG', issue):
            num = re.match(r'BULLET(\d)_TOO_LONG', issue).group(1)
            key = f'bullet{num}'
            bullet = corrected.get(key, '')
            corrected[key] = bullet[:LIMITS['bullet'] - 3].strip() + '...'
            fixes_applied.append(f'BULLET{num}_TRIMMED')

        # Fix bullet missing CAPS lead — reformat
        elif re.match(r'BULLET(\d)_NO_CAPS_LEAD', issue):
            num = re.match(r'BULLET(\d)_NO_CAPS_LEAD', issue).group(1)
            key = f'bullet{num}'
            bullet = corrected.get(key, '')
            if bullet and '—' not in bullet and '-' in bullet:
                # Try to convert dash to em dash
                corrected[key] = bullet.replace(' - ', ' — ', 1)
                fixes_applied.append(f'BULLET{num}_DASH_FIXED')

    return corrected, fixes_applied

# ============================================================
# L4 — TARGETED SINGLE-FIELD RETRY
# ============================================================

client = anthropic.Anthropic()

def retry_single_field(field_name, original_content, row, dims, material_key, material_name):
    title = original_content.get('title', row.get('item-name', ''))

    prompts = {
        'title': f"""Generate ONLY a corrected Amazon product title for this plastic product.
Original: {row.get('item-name', '')}
Material: {material_name}
Product type: {dims.get('product_type')}
Rules: Start with "Plastic-Craft | ", under 200 characters, include material, product type, color, dimensions.
Return ONLY the title string, nothing else.""",

        'description': f"""Generate ONLY the product description for this Amazon listing.
Product: {title}
Material: {material_name}
Rules: Under 2000 characters, conversational tone answering buyer questions, MUST end with exactly this paragraph:
"{CERTIFICATION_PARAGRAPH}"
Return ONLY the description text, nothing else.""",

        'backend_search_terms': f"""Generate ONLY Amazon backend search terms for this product.
Product: {title}
Material: {material_name}
Rules: Under 249 bytes total, space-separated, no commas, no words from title, no prohibited terms (best/top/guaranteed/free/sale/discount).
Return ONLY the space-separated terms string, nothing else.""",
    }

    if field_name.startswith('bullet'):
        num = field_name[-1]
        bullet_focus = {
            '1': 'material properties — what it is and what makes it great',
            '2': 'exact specifications — dimensions and pack quantity',
            '3': 'applications and use cases — specific industries and projects',
            '4': 'fabrication and workability — tools, bonding, forming',
            '5': f'tolerances and quality — use exactly: "PRECISION TOLERANCES & QUALITY — Dimensions held to +/- 0.010" ensuring consistent, precise fit for fabrication and engineering applications. Manufactured under ISO 9001:2015 certified quality management standards for reliable material quality on every order."',
        }
        prompts[field_name] = f"""Generate ONLY bullet point {num} for this Amazon listing.
Product: {title}
Material: {material_name}
Focus: {bullet_focus.get(num, 'product benefit')}
Rules: MUST start with ALL CAPS benefit phrase followed by em dash (—), under 500 characters, no Plastic-Craft branding.
Example format: "IMPACT RESISTANT — ABS delivers exceptional toughness..."
Return ONLY the bullet text, nothing else."""

    prompt = prompts.get(field_name)
    if not prompt:
        return None

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
    except:
        return None

# ============================================================
# MAIN API CALL
# ============================================================

SYSTEM_PROMPT = """You are an expert Amazon listing copywriter for Plastic-Craft Products, founded 1934, ISO 9001:2015 and AS9100D certified.

TITLE: "Plastic-Craft | [Cast/Extruded if Acrylic/Nylon only] [Material] [Type], [Transparent/Translucent/Opaque if present] [Color], [Dimensions]"
- Cast/Extruded ONLY for Acrylic and Nylon
- Rods: use "OD" not "Diameter"
- HDPE Natural = "White" | Nylon Natural = "Natural (White)"
- Dimensions: Fraction" (Decimal") to nearest 1/16th, whole numbers need no decimal
- Pack at end: (Pack of 4)
- Strip color codes, keep Woodgrain, drop other patterns
- Food Grade HDPE ONLY on cutting boards
- Weld-On DCM & TCE Free ONLY for #3, #4, #16
- Third party brands: NO Plastic-Craft prefix
- Move Utility Grade/Nominal to description
- Under 200 characters, brand in first 80 characters

BULLETS — EVERY bullet MUST start ALL CAPS benefit + em dash (—):
- B1: Material properties with specific data (PSI, temp ratings, etc.)
- B2: Exact specs — dimensions, tolerances +/-0.010", pack quantity
- B3: Applications — specific industries, projects, use cases with keywords
- B4: Fabrication — tools needed, bonding methods, finishing techniques
- B5: EXACTLY: "PRECISION TOLERANCES & QUALITY — Dimensions held to +/- 0.010" ensuring consistent, precise fit for fabrication and engineering applications. Manufactured under ISO 9001:2015 certified quality management standards for reliable material quality on every order."
- Each bullet under 500 characters
- Feature + Benefit + Proof structure with numbers/data
- Answer real buyer pain points naturally

DESCRIPTION:
- Conversational — answer real buyer questions
- Include material data points (tensile strength, temp ratings)
- Address top buyer pain points for this material
- Include compliance/certifications (FDA, NSF, ASTM, RoHS where applicable)
- Include "Stocked and ships from West Nyack, NY" 
- MUST end with EXACTLY: "Plastic-Craft Products has been a trusted supplier of quality plastic materials since 1934. We are ISO 9001:2015 and AS9100D certified, ensuring rigorous quality management standards across our entire operation. All dimensions are held to +/- 0.010" tolerances. Whether you're a hobbyist, fabricator, engineer, or procurement professional — we have the materials and expertise to support your project."
- Under 2000 characters total
- Include Utility Grade/Nominal if in original title

BACKEND TERMS:
- Under 249 bytes, space-separated, NO commas
- NO words already in title
- NO prohibited terms: best top guaranteed free sale discount cheap lowest price amazing
- Include alternate names, trade names, Spanish terms, misspellings, applications
- Include "west nyack ny" "plastic supplier" "usa stocked"

ATTRIBUTES: material_type, color (clean name), size_description (human readable), finish_type

Return ONLY valid JSON no markdown:
{"title":"...","bullet1":"...","bullet2":"...","bullet3":"...","bullet4":"...","bullet5":"...","description":"...","backend_search_terms":"...","material_type":"...","color":"...","size_description":"...","finish_type":"..."}"""

def generate_content(row, dims, material_key, material_name, weight, box_dims, base_backend, freight_needed):
    original_title = row.get('item-name', '').strip()
    original_desc = row.get('item-description', '').strip()
    modifiers, cast_ext, color = detect_color(original_title)
    pack = detect_pack(original_title)
    mat_data = MATERIAL_DATA.get(material_key, {})

    context = f"""Original title: {original_title}
Original description: {original_desc[:300] if original_desc else 'None'}
SKU: {row.get('seller-sku', '')} | ASIN: {row.get('asin1', '')}
Material: {material_name} | Key: {material_key}
Product type: {dims.get('product_type', 'unknown')}
Dimensions parsed: {json.dumps({k:v for k,v in dims.items() if k != 'product_type'})}
Color: {color} | Cast/Extruded: {cast_ext} | Modifiers: {modifiers} | Pack: {pack}
Weight calculated: {weight} lbs
Box dimensions: {json.dumps(box_dims)}
Material key stats: {mat_data.get('key_stat', 'N/A')}
Tensile strength: {mat_data.get('tensile_psi', 'N/A')} PSI
Service temp: {mat_data.get('service_temp', 'N/A')}
Compliance: {', '.join(mat_data.get('compliance', []))}
Buyer pain points to address: {' | '.join(mat_data.get('pain_points', []))}
Freight shipping required: {freight_needed}
Base backend terms to incorporate: {' '.join(base_backend[:12])}
{'IMPORTANT: Append freight notice at end of description: ' + FREIGHT_BLURB[:100] + '...' if freight_needed else ''}"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1800,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": context}]
        )
        raw = response.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        return json.loads(raw)
    except json.JSONDecodeError:
        # L3 fallback: try to extract JSON with regex
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except:
                pass
        return None
    except Exception:
        return None

# ============================================================
# CHECKPOINT SYSTEM
# ============================================================

def load_checkpoint():
    if Path(CHECKPOINT_FILE).exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                print(f"  Resuming from checkpoint: {len(data.get('completed', []))} listings already done")
                return data
        except:
            pass
    return {'completed': [], 'run_id': RUN_ID}

def save_checkpoint(checkpoint_data):
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f)
    except Exception as e:
        print(f"  Warning: Could not save checkpoint: {e}")

def clear_checkpoint():
    if Path(CHECKPOINT_FILE).exists():
        os.remove(CHECKPOINT_FILE)

# ============================================================
# VIDEO RECOMMENDATION FLAG
# ============================================================

def should_flag_video(material_key, product_type, weight):
    high_value = material_key in ('peek', 'ptfe', 'polycarbonate', 'delrin')
    large_format = weight and weight > 10
    complex_product = product_type in ('cutting_board_oem', 'tube')
    return high_value or large_format or complex_product

# ============================================================
# OUTPUT FIELDS
# ============================================================

FIELDNAMES = [
    'sku', 'asin', 'original_title', 'new_title',
    'title_char_count', 'title_mobile_ok',
    'bullet1', 'bullet2', 'bullet3', 'bullet4', 'bullet5',
    'description', 'description_char_count',
    'backend_search_terms', 'backend_byte_count',
    'subject_matter', 'intended_use', 'target_audience',
    'material_type', 'color', 'size_description', 'finish_type',
    'item_weight_lbs', 'shipping_weight_lbs',
    'box_length_in', 'box_width_in', 'box_height_in',
    'item_length_in', 'item_width_in', 'item_height_in',
    'product_type', 'material_key',
    'freight_notice_applied', 'video_recommended',
    'compliance_flags',
    'l1_input_issues', 'l2_output_issues',
    'l3_fixes_applied', 'l4_field_retries',
    'validation_final', 'status'
]

# ============================================================
# MAIN PROCESSING
# ============================================================

def process_listings():
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        logger.critical("ANTHROPIC_API_KEY environment variable not set — cannot run")
        return None

    start_time = datetime.now()

    logger.info("=" * 65)
    logger.info("Plastic-Craft Amazon Listing Optimizer v3.0")
    logger.info(f"Mode: {'TEST (' + str(TEST_LIMIT) + ' listings)' if TEST_MODE else 'FULL CATALOG RUN'}")
    logger.info(f"Output file: {OUTPUT_FILE}")
    logger.info(f"Run ID: {RUN_ID}")
    logger.info("=" * 65)

    jlog.log('run_start', {
        'mode': 'test' if TEST_MODE else 'full',
        'test_limit': TEST_LIMIT if TEST_MODE else None,
        'output_file': OUTPUT_FILE,
    })

    # Initialize self-healing engine
    healer = SelfHealingEngine(
        module=_self_module,
        logger=logger,
        jlog=jlog,
        consecutive_threshold=5,
        percentage_threshold=0.10,
        auto_apply=False,
    )
    sanity = DimensionSanityChecker(logger)
    logger.info("✓ Self-healing engine initialized (threshold: 5 consecutive OR 10%)")
    logger.info("✓ Dimension sanity checker initialized")

    # Load checkpoint
    checkpoint = load_checkpoint()
    completed_skus = set(checkpoint.get('completed', []))
    if completed_skus:
        logger.info(f"↻ Resuming run — {len(completed_skus):,} SKUs already completed")

    # Read listings
    all_listings = []
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                if row.get('status', '').strip() == 'Active':
                    all_listings.append(row)
        logger.info(f"✓ Loaded {len(all_listings):,} active listings from input file")
    except Exception as e:
        logger.critical(f"✗ Failed to read input file: {e}")
        return None

    listings = [r for r in all_listings if r.get('seller-sku', '') not in completed_skus]
    logger.info(f"📋 Remaining to process: {len(listings):,}")

    if TEST_MODE:
        samples = []
        seen = set()
        targets = ['tube', 'rod', 'sheet', 'cutting board', 'acrylic', 'nylon', 'polycarbonate', 'hdpe']
        for t in targets:
            for row in listings:
                title = row.get('item-name', '').lower()
                if t in title and t not in seen:
                    samples.append(row)
                    seen.add(t)
                    break
        for row in listings:
            if len(samples) >= TEST_LIMIT: break
            if row not in samples:
                samples.append(row)
        listings = samples[:TEST_LIMIT]
        logger.info(f"📋 Test mode: selected {len(listings)} diverse listings")

    logger.info("=" * 65)

    # Stats tracking
    stats = {
        'total': 0, 'successful': 0, 'warnings': 0, 'errors': 0,
        'freight_flagged': 0, 'video_flagged': 0, 'compliance_flagged': 0,
        'l1_flagged': 0, 'l2_caught': 0, 'l3_fixes': 0, 'l4_retries': 0,
        'top_errors': {},
    }

    errors = []
    processed_count = 0

    # Open output files for streaming writes
    out_file_exists = Path(OUTPUT_FILE).exists()
    out_f = open(OUTPUT_FILE, 'a', newline='', encoding='utf-8')
    out_writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES)
    if not out_file_exists:
        out_writer.writeheader()

    err_file_exists = Path(ERROR_LOG).exists()
    err_f = open(ERROR_LOG, 'a', newline='', encoding='utf-8')
    err_writer = csv.DictWriter(err_f, fieldnames=['sku', 'asin', 'title', 'issues', 'status'])
    if not err_file_exists:
        err_writer.writeheader()

    for i, row in enumerate(listings):
        title = row.get('item-name', '').strip()
        sku = row.get('seller-sku', '').strip()
        asin = row.get('asin1', '').strip()

        logger.info(f"\n[{i+1}/{len(listings)}] SKU: {sku}")
        logger.debug(f"  Title: {title[:80]}...")

        jlog.log('listing_start', {'sku': sku, 'asin': asin, 'title': title[:100]})

        # ---- L1: PRE-API INPUT VALIDATION ----
        logger.debug("  L1: Parsing dimensions and validating input...")
        dims = extract_dimensions(title)
        material_key, material_name = detect_material(title)
        l1_issues = validate_input(dims, material_key, title)

        if l1_issues:
            logger.warning(f"  ⚠ L1 input issues: {', '.join(l1_issues)}")
            stats['l1_flagged'] += 1
            jlog.log('l1_issues', {'sku': sku, 'issues': l1_issues})
        else:
            logger.debug(f"  ✓ L1 passed — type: {dims.get('product_type')} | material: {material_name}")

        weight = calculate_weight(material_key, dims)
        box_dims = calculate_box_dims(dims)

        # ---- DIMENSION + WEIGHT SANITY CHECK ----
        logger.debug("  Sanity checking dimensions and weight...")
        san_valid, san_issues, dims, weight, box_dims = sanity.check_all(
            dims, weight, box_dims, material_key, title
        )

        if san_issues:
            errors_only = [i for i in san_issues if i['severity'] == 'ERROR']
            warnings_only = [i for i in san_issues if i['severity'] == 'WARNING']

            for issue in errors_only:
                logger.error(f"  ✗ SANITY ERROR [{issue['field']}]: {issue['msg']}")
                if 'corrected_to' in issue:
                    logger.info(f"    Auto-corrected to: {issue['corrected_to']}")
                jlog.log('sanity_error', {
                    'sku': sku, 'field': issue['field'],
                    'msg': issue['msg'], 'value': str(issue['value'])
                })

            for issue in warnings_only:
                logger.warning(f"  ⚠ SANITY WARNING [{issue['field']}]: {issue['msg']}")
                if issue.get('suggested_weight'):
                    logger.info(f"    Suggested weight: {issue['suggested_weight']} lbs")
                    weight = issue['suggested_weight']
                jlog.log('sanity_warning', {
                    'sku': sku, 'field': issue['field'],
                    'msg': issue['msg'], 'value': str(issue['value'])
                })

            if not san_valid:
                logger.warning(f"  ⚠ Sanity check failed — dimensions may be incorrect for {sku}")
                healer.record_failing_title(title)
        else:
            logger.debug("  ✓ Sanity check passed — dimensions and weight look correct")

        freight_needed = needs_freight_notice(dims, weight, box_dims)
        base_backend = get_base_backend_terms(material_key, dims.get('product_type', 'unknown'))
        subject_matter = get_subject_matter(material_key, dims.get('product_type', 'unknown'))
        intended_use = get_intended_use(material_key, dims.get('product_type', 'unknown'))
        target_audience = get_target_audience(material_key, dims.get('product_type', 'unknown'))
        compliance = ', '.join(MATERIAL_DATA.get(material_key, {}).get('compliance', []))
        video_flag = should_flag_video(material_key, dims.get('product_type', ''), weight)

        logger.debug(f"  Weight: {weight} lbs | Box: {box_dims} | Freight: {freight_needed}")

        if freight_needed:
            logger.info(f"  🚛 Freight notice will be applied (weight: {weight} lbs, dims: {box_dims})")
            stats['freight_flagged'] += 1
            jlog.log('freight_flagged', {'sku': sku, 'weight': weight, 'box_dims': box_dims})

        if video_flag:
            logger.debug(f"  📹 Video recommended for this SKU")
            stats['video_flagged'] += 1

        if compliance:
            stats['compliance_flagged'] += 1

        ptype = dims.get('product_type', '')
        item_l = dims.get('length', '')
        item_w = dims.get('width', dims.get('od', ''))
        item_h = dims.get('thickness', dims.get('od', ''))

        # ---- API CALL WITH RETRY ----
        content = None
        attempt = 0
        while attempt < MAX_RETRIES and content is None:
            attempt += 1
            if attempt > 1:
                logger.warning(f"  ↻ API retry attempt {attempt}/{MAX_RETRIES} (waiting {RETRY_DELAY * attempt}s)...")
                jlog.log('api_retry', {'sku': sku, 'attempt': attempt})
                time.sleep(RETRY_DELAY * attempt)

            logger.debug(f"  Calling Claude API (attempt {attempt})...")
            content = generate_content(
                row, dims, material_key, material_name,
                weight, box_dims, base_backend, freight_needed
            )

        if content is None:
            logger.error(f"  ✗ All {MAX_RETRIES} API attempts failed — saving partial data and continuing")
            jlog.log('api_failed', {'sku': sku, 'attempts': MAX_RETRIES})
            stats['errors'] += 1
            stats['top_errors']['API_FAILED'] = stats['top_errors'].get('API_FAILED', 0) + 1

            result = build_error_result(
                sku, asin, title, dims, material_key, material_name,
                weight, box_dims, subject_matter, intended_use,
                target_audience, compliance, freight_needed, video_flag,
                item_l, item_w, item_h, l1_issues
            )
            out_writer.writerow(result)
            out_f.flush()
            err_writer.writerow({
                'sku': sku, 'asin': asin, 'title': title,
                'issues': 'API_FAILED_ALL_RETRIES', 'status': 'error'
            })
            err_f.flush()
            errors.append(sku)
            checkpoint['completed'].append(sku)
            processed_count += 1
            stats['total'] += 1
            if processed_count % CHECKPOINT_INTERVAL == 0:
                save_checkpoint(checkpoint)
            continue

        logger.debug(f"  ✓ API returned content successfully")

        # ---- L2: OUTPUT VALIDATION ----
        logger.debug("  L2: Validating output fields...")
        l2_issues = validate_output(content, freight_needed)

        if l2_issues:
            logger.warning(f"  ⚠ L2 output issues: {', '.join(l2_issues)}")
            stats['l2_caught'] += len(l2_issues)
            for issue in l2_issues:
                issue_type = issue.split(':')[0]
                stats['top_errors'][issue_type] = stats['top_errors'].get(issue_type, 0) + 1
            jlog.log('l2_issues', {'sku': sku, 'issues': l2_issues})
        else:
            logger.debug("  ✓ L2 validation passed — all fields clean")

        # ---- L3: AUTO-CORRECTION ----
        l3_fixes = []
        if l2_issues:
            logger.debug("  L3: Attempting auto-corrections...")
            content, l3_fixes = auto_correct(content, l2_issues, freight_needed)
            if l3_fixes:
                logger.info(f"  ✓ L3 auto-fixed: {', '.join(l3_fixes)}")
                stats['l3_fixes'] += len(l3_fixes)
                jlog.log('l3_fixes', {'sku': sku, 'fixes': l3_fixes})
            else:
                logger.debug("  L3: No auto-corrections applied")

        # ---- L4: TARGETED FIELD RETRY ----
        remaining_issues = validate_output(content, freight_needed)
        l4_retries = []

        if remaining_issues:
            logger.debug(f"  L4: {len(remaining_issues)} issues remain after L3 — attempting targeted retries...")

        for issue in remaining_issues:
            field = None
            if 'TITLE' in issue and 'TRIMMED' not in issue:
                field = 'title'
            elif re.match(r'BULLET(\d)_(MISSING|NO_CAPS)', issue):
                num = re.match(r'BULLET(\d)', issue).group(1)
                field = f'bullet{num}'
            elif 'DESCRIPTION_MISSING' in issue and 'CERTIFICATION' not in issue:
                field = 'description'
            elif 'BACKEND' in issue and 'TRIMMED' not in issue:
                field = 'backend_search_terms'

            if field:
                logger.info(f"  ↻ L4 targeted retry: {field}...")
                new_val = retry_single_field(
                    field, content, row, dims, material_key, material_name
                )
                if new_val:
                    content[field] = new_val
                    l4_retries.append(field)
                    stats['l4_retries'] += 1
                    logger.info(f"  ✓ L4 fixed: {field}")
                    jlog.log('l4_retry', {'sku': sku, 'field': field, 'success': True})
                else:
                    logger.warning(f"  ⚠ L4 retry failed for: {field}")
                    jlog.log('l4_retry', {'sku': sku, 'field': field, 'success': False})

        # ---- FINAL BACKEND CLEANUP ----
        raw_backend = content.get('backend_search_terms', '')
        final_backend = clean_and_trim_backend_terms(
            raw_backend.split() + base_backend,
            content.get('title', title)
        )
        content['backend_search_terms'] = final_backend
        logger.debug(f"  Backend terms: {len(final_backend.encode())} bytes")

        # ---- FINAL VALIDATION ----
        final_issues = validate_output(content, freight_needed)
        final_status = 'ready' if not final_issues else 'ready_with_warnings'

        if final_issues:
            logger.warning(f"  ⚠ Final remaining issues: {', '.join(final_issues)}")
            stats['warnings'] += 1
        else:
            stats['successful'] += 1

        # ---- BUILD AND WRITE RESULT ----
        result = {
            'sku': sku,
            'asin': asin,
            'original_title': title,
            'new_title': content.get('title', ''),
            'title_char_count': len(content.get('title', '')),
            'title_mobile_ok': 'YES' if 'Plastic-Craft' in content.get('title', '')[:80] else 'CHECK',
            'bullet1': content.get('bullet1', ''),
            'bullet2': content.get('bullet2', ''),
            'bullet3': content.get('bullet3', ''),
            'bullet4': content.get('bullet4', ''),
            'bullet5': content.get('bullet5', ''),
            'description': content.get('description', ''),
            'description_char_count': len(content.get('description', '')),
            'backend_search_terms': final_backend,
            'backend_byte_count': len(final_backend.encode('utf-8')),
            'subject_matter': subject_matter,
            'intended_use': intended_use,
            'target_audience': target_audience,
            'material_type': content.get('material_type', material_name),
            'color': content.get('color', ''),
            'size_description': content.get('size_description', ''),
            'finish_type': content.get('finish_type', 'Standard'),
            'item_weight_lbs': weight or '',
            'shipping_weight_lbs': weight or '',
            'box_length_in': box_dims.get('box_length', ''),
            'box_width_in': box_dims.get('box_width', ''),
            'box_height_in': box_dims.get('box_height', ''),
            'item_length_in': item_l,
            'item_width_in': item_w,
            'item_height_in': item_h,
            'product_type': ptype,
            'material_key': material_key,
            'freight_notice_applied': 'YES' if freight_needed else 'NO',
            'video_recommended': 'YES' if video_flag else 'NO',
            'compliance_flags': compliance,
            'l1_input_issues': ' | '.join(l1_issues) if l1_issues else 'PASS',
            'l2_output_issues': ' | '.join(l2_issues) if l2_issues else 'PASS',
            'l3_fixes_applied': ' | '.join(l3_fixes) if l3_fixes else 'NONE',
            'l4_field_retries': ' | '.join(l4_retries) if l4_retries else 'NONE',
            'validation_final': ' | '.join(final_issues) if final_issues else 'PASS',
            'status': final_status,
        }

        out_writer.writerow(result)
        out_f.flush()

        logger.info(f"  ✓ {final_status.upper()} | Title: {content.get('title','')[:60]}...")
        logger.debug(f"    L1:{len(l1_issues)} issues | L2:{len(l2_issues)} | L3:{len(l3_fixes)} fixes | L4:{len(l4_retries)} retries")

        jlog.log('listing_complete', {
            'sku': sku,
            'status': final_status,
            'freight': freight_needed,
            'weight': weight,
            'l1_issues': len(l1_issues),
            'l2_issues': len(l2_issues),
            'l3_fixes': len(l3_fixes),
            'l4_retries': len(l4_retries),
            'title_chars': len(content.get('title', '')),
            'desc_chars': len(content.get('description', '')),
            'backend_bytes': len(final_backend.encode()),
        })

        # Update checkpoint and healer
        checkpoint['completed'].append(sku)
        processed_count += 1
        stats['total'] += 1

        # Record result in self-healing engine
        healer.record_result(
            sku,
            l1_issues,
            l2_issues,
            final_issues if final_issues else []
        )
        if l1_issues:
            healer.record_failing_title(title)

        if processed_count % CHECKPOINT_INTERVAL == 0:
            save_checkpoint(checkpoint)
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = processed_count / elapsed * 60 if elapsed > 0 else 0
            logger.info(f"\n  💾 Checkpoint saved — {processed_count} processed ({rate:.1f}/min)")
            jlog.log('checkpoint', {'processed': processed_count, 'rate_per_min': round(rate, 1)})

        time.sleep(0.5)

    out_f.close()
    err_f.close()

    # Clear checkpoint on successful full run
    if not TEST_MODE and not errors:
        clear_checkpoint()
        logger.info("✓ Checkpoint cleared — full run completed successfully")

    # Self-healing summary
    heal_summary = healer.get_summary()
    stats['self_heal_rewrites_attempted'] = sum(heal_summary['rewrites_attempted'].values())
    stats['self_heal_rewrites_applied'] = len(heal_summary['rewrites_applied'])

    if heal_summary['rewrites_applied']:
        logger.info(f"  Self-heal rewrites applied: {', '.join(heal_summary['rewrites_applied'])}")
    if heal_summary['rewrite_history']:
        logger.info(f"  Rewrite history:")
        for rw in heal_summary['rewrite_history']:
            logger.info(f"    {rw['function']} — triggered by {rw['error_type']}")

    jlog.log('self_heal_summary', heal_summary)

    # Write run summary
    duration = (datetime.now() - start_time).total_seconds()
    stats['duration_seconds'] = duration
    stats['output_file'] = OUTPUT_FILE
    stats['error_log'] = ERROR_LOG

    summary_file = write_run_summary(LOG_DIR, RUN_ID, stats)

    logger.info("\n" + "=" * 65)
    logger.info("RUN COMPLETE")
    logger.info(f"  Total processed  : {stats['total']:,}")
    logger.info(f"  Successful       : {stats['successful']:,}")
    logger.info(f"  Warnings         : {stats['warnings']:,}")
    logger.info(f"  Errors           : {stats['errors']:,}")
    logger.info(f"  Freight flagged  : {stats['freight_flagged']:,}")
    logger.info(f"  Video flagged    : {stats['video_flagged']:,}")
    logger.info(f"  L3 auto-fixes    : {stats['l3_fixes']:,}")
    logger.info(f"  L4 retries       : {stats['l4_retries']:,}")
    logger.info(f"  Duration         : {int(duration//60)}m {int(duration%60)}s")
    logger.info(f"  Output file      : {OUTPUT_FILE}")
    if errors:
        logger.warning(f"  Error log        : {ERROR_LOG}")
    if summary_file:
        logger.info(f"  Run summary      : {summary_file}")
    logger.info("=" * 65)

    jlog.log('run_complete', {**stats, 'error_skus': errors[:20]})

    return OUTPUT_FILE

def build_error_result(sku, asin, title, dims, material_key, material_name,
                       weight, box_dims, subject_matter, intended_use,
                       target_audience, compliance, freight_needed,
                       video_flag, item_l, item_w, item_h, l1_issues):
    return {
        'sku': sku, 'asin': asin, 'original_title': title,
        'new_title': '', 'title_char_count': 0, 'title_mobile_ok': 'ERROR',
        'bullet1': '', 'bullet2': '', 'bullet3': '',
        'bullet4': '', 'bullet5': '', 'description': '',
        'description_char_count': 0,
        'backend_search_terms': '', 'backend_byte_count': 0,
        'subject_matter': subject_matter,
        'intended_use': intended_use,
        'target_audience': target_audience,
        'material_type': material_name, 'color': '',
        'size_description': '', 'finish_type': '',
        'item_weight_lbs': weight or '',
        'shipping_weight_lbs': weight or '',
        'box_length_in': box_dims.get('box_length', ''),
        'box_width_in': box_dims.get('box_width', ''),
        'box_height_in': box_dims.get('box_height', ''),
        'item_length_in': item_l, 'item_width_in': item_w,
        'item_height_in': item_h,
        'product_type': dims.get('product_type', ''),
        'material_key': material_key,
        'freight_notice_applied': 'YES' if freight_needed else 'NO',
        'video_recommended': 'YES' if video_flag else 'NO',
        'compliance_flags': compliance,
        'l1_input_issues': ' | '.join(l1_issues) if l1_issues else 'PASS',
        'l2_output_issues': 'API_FAILED',
        'l3_fixes_applied': 'NONE',
        'l4_field_retries': 'NONE',
        'validation_final': 'FAILED',
        'status': 'error'
    }

if __name__ == '__main__':
    process_listings()
