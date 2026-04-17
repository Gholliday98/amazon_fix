"""
Plastic-Craft Amazon Listing Optimizer
Comprehensive Synonym Library + Backend Search Term Generator
Covers all materials, product types, applications, and buyer intent phrases
"""

# ============================================================
# MATERIAL SYNONYM LIBRARY
# Alternate names, trade names, abbreviations, misspellings,
# Spanish equivalents, material codes, ASTM grades
# ============================================================

MATERIAL_SYNONYMS = {
    'acrylic': {
        'alternates': [
            'plexiglass', 'plexiglas', 'perspex', 'pmma',
            'poly methyl methacrylate', 'polymethylmethacrylate',
            'lucite', 'acrylite', 'optix', 'polycast',
            'transparent plastic', 'clear plastic',
        ],
        'misspellings': [
            'acryllic', 'acrylyc', 'acrylc', 'plexiglas',
            'plexiglass sheet', 'plexiglass rod', 'plexiglass tube',
        ],
        'spanish': ['acrilico', 'plastico acrilico', 'lamina acrilica'],
        'astm': ['ASTM D4802', 'ASTM D788'],
        'applications': [
            'laser cutting', 'laser engraving', 'cnc machining',
            'display case', 'display stand', 'sign making', 'signage',
            'window replacement', 'glazing', 'picture frame',
            'aquarium', 'terrarium', 'light diffuser', 'light cover',
            'retail display', 'point of purchase display',
            'protective shield', 'sneeze guard', 'barrier',
            'art projects', 'craft projects', 'diy projects',
            'thermoforming', 'vacuum forming', 'fabrication',
            'trophy', 'award', 'nameplate',
        ],
        'properties': [
            'uv resistant', 'uv stabilized', 'weatherproof',
            'shatter resistant', 'optical clarity', 'high clarity',
            'lightweight', 'glass alternative', 'glass replacement',
            'impact resistant', 'scratch resistant',
        ],
        'industry': ['cast acrylic', 'extruded acrylic', 'cell cast'],
    },

    'abs': {
        'alternates': [
            'acrylonitrile butadiene styrene',
            'abs plastic', 'abs sheet', 'styrene plastic',
            'engineering plastic', 'thermoplastic',
        ],
        'misspellings': ['abs plasic', 'abs plastc'],
        'spanish': ['abs plastico', 'lamina abs'],
        'astm': ['ASTM D4673', 'ASTM D1892'],
        'applications': [
            'enclosure', 'housing', 'electronics housing',
            'automotive trim', 'automotive parts',
            'prototype', 'prototyping', 'model making',
            'vacuum forming', 'thermoforming',
            'machine guard', 'safety guard',
            'panel', 'wall panel', 'cabinet',
            'robotics', 'drone parts', '3d printing alternative',
            'cosplay', 'prop making',
        ],
        'properties': [
            'rigid', 'tough', 'impact resistant',
            'machinable', 'weldable', 'paintable',
            'chemical resistant', 'moisture resistant',
            'dimensional stability', 'easy to fabricate',
        ],
        'industry': ['utility grade', 'general purpose', 'haricell'],
    },

    'hdpe': {
        'alternates': [
            'high density polyethylene',
            'high-density polyethylene',
            'hdpe plastic', 'polyethylene',
            'marine board', 'cutting board plastic',
            'food grade plastic', 'fda plastic',
        ],
        'misspellings': ['hdp', 'high density polyethylene sheet'],
        'spanish': ['polietileno alta densidad', 'hdpe plastico'],
        'astm': ['ASTM D4976', 'ASTM F1048'],
        'applications': [
            'cutting board', 'food prep', 'food contact',
            'food grade', 'fda approved', 'nsf compliant',
            'marine application', 'boat', 'dock',
            'chemical tank', 'chemical resistant',
            'outdoor use', 'weatherproof',
            'playground equipment', 'picnic table',
            'weld fabrication', 'plastic welding',
            'machining', 'wear part',
        ],
        'properties': [
            'food safe', 'fda compliant', 'nsf approved',
            'chemical resistant', 'moisture resistant',
            'uv resistant', 'weatherproof', 'outdoor rated',
            'non-porous', 'easy to clean', 'sanitizable',
            'self lubricating', 'low friction',
            'impact resistant', 'tough',
        ],
        'industry': ['natural', 'white hdpe', 'black hdpe', 'virgin hdpe'],
    },

    'polycarbonate': {
        'alternates': [
            'pc plastic', 'lexan', 'makrolon', 'tuffak',
            'polycarbonate sheet', 'pc sheet',
            'bulletproof plastic', 'bullet resistant plastic',
            'shatterproof plastic', 'impact resistant plastic',
        ],
        'misspellings': [
            'polycarbante', 'polycarbonite', 'policarbonate',
            'lexan sheet',
        ],
        'spanish': ['policarbonato', 'lamina policarbonato'],
        'astm': ['ASTM D3935', 'ASTM D5948'],
        'applications': [
            'machine guard', 'safety guard', 'safety shield',
            'bullet resistant', 'ballistic shield',
            'greenhouse panel', 'greenhouse glazing',
            'skylight', 'roofing panel', 'canopy',
            'window replacement', 'glazing',
            'face shield', 'protective barrier',
            'electronic enclosure', 'instrument panel',
            'riot shield', 'security barrier',
            'uv protection', 'light diffuser',
        ],
        'properties': [
            'virtually unbreakable', 'shatter resistant',
            'impact resistant', '250 times stronger than glass',
            'uv stabilized', 'uv resistant',
            'optical clarity', 'high clarity',
            'lightweight', 'heat resistant',
            'flame retardant',
        ],
        'industry': ['machine grade', 'optical grade', 'uv grade'],
    },

    'nylon': {
        'alternates': [
            'polyamide', 'pa6', 'pa66', 'nylon 6', 'nylon 66',
            'nylon plastic', 'engineering nylon',
        ],
        'misspellings': ['nilon', 'nylon sheet plastic'],
        'spanish': ['nylon plastico', 'poliamida'],
        'astm': ['ASTM D4066', 'ASTM D5989'],
        'applications': [
            'bushing', 'bearing', 'wear part',
            'gear', 'sprocket', 'pulley',
            'machining', 'precision machining',
            'food grade', 'fda compliant',
            'structural part', 'mechanical part',
            'standoff', 'spacer', 'fastener',
            'electrical insulation', 'insulator',
        ],
        'properties': [
            'self lubricating', 'low friction',
            'wear resistant', 'abrasion resistant',
            'impact resistant', 'tough',
            'heat resistant', 'high temperature',
            'chemical resistant', 'oil resistant',
            'machinable', 'easy to machine',
            'dimensional stability',
        ],
        'industry': [
            'cast nylon', 'extruded nylon',
            'natural nylon', 'nylon 6/6',
            'mc nylon', 'oil filled nylon',
        ],
    },

    'uhmw': {
        'alternates': [
            'ultra high molecular weight polyethylene',
            'ultra-high-molecular-weight polyethylene',
            'uhmw pe', 'uhmwpe',
            'tivar', 'tivar 1000',
            'wear plastic', 'liner plastic',
        ],
        'misspellings': ['uhmw plastic', 'ultra high molecular weight'],
        'spanish': ['polietileno ultra alto peso molecular'],
        'astm': ['ASTM D4020'],
        'applications': [
            'wear strip', 'wear liner', 'wear pad',
            'conveyor liner', 'conveyor guide',
            'chute liner', 'hopper liner',
            'truck bed liner', 'dump truck liner',
            'food grade', 'fda compliant', 'food contact',
            'cutting board', 'food processing',
            'star wheel', 'timing screw',
            'sliding surface', 'low friction surface',
            'impact absorption',
        ],
        'properties': [
            'self lubricating', 'lowest friction',
            'wear resistant', 'abrasion resistant',
            'impact resistant', 'noise dampening',
            'chemical resistant', 'food safe',
            'fda compliant', 'usda approved',
            'non stick', 'easy release',
        ],
        'industry': ['natural uhmw', 'black uhmw', 'virgin uhmw'],
    },

    'pvc': {
        'alternates': [
            'polyvinyl chloride', 'poly vinyl chloride',
            'pvc plastic', 'rigid pvc', 'cpvc',
            'vinyl sheet', 'vinyl plastic',
            'type 1 pvc', 'type ii pvc',
        ],
        'misspellings': ['pvc plasic', 'polyvinl chloride'],
        'spanish': ['pvc plastico', 'cloruro de polivinilo'],
        'astm': ['ASTM D1784', 'ASTM D4216'],
        'applications': [
            'chemical tank', 'chemical resistant',
            'plumbing', 'pipe fitting',
            'signage', 'sign board',
            'electrical insulation',
            'food processing equipment',
            'laboratory use', 'lab equipment',
            'marine use', 'boat',
            'industrial fabrication',
        ],
        'properties': [
            'chemical resistant', 'acid resistant',
            'flame retardant', 'self extinguishing',
            'moisture resistant', 'waterproof',
            'rigid', 'stiff', 'dimensional stability',
            'machinable', 'weldable',
        ],
        'industry': ['type 1', 'gray pvc', 'white pvc', 'clear pvc'],
    },

    'polypropylene': {
        'alternates': [
            'pp plastic', 'polypropene',
            'pp sheet', 'copolymer polypropylene',
            'homopolymer polypropylene',
        ],
        'misspellings': ['polypropelene', 'polypropylene sheet'],
        'spanish': ['polipropileno', 'pp plastico'],
        'astm': ['ASTM D4101'],
        'applications': [
            'chemical tank', 'acid resistant',
            'food contact', 'food grade',
            'living hinge', 'flexible hinge',
            'packaging', 'container',
            'laboratory', 'lab equipment',
            'filtration', 'filter housing',
        ],
        'properties': [
            'chemical resistant', 'acid resistant',
            'food safe', 'fda compliant',
            'lightweight', 'fatigue resistant',
            'living hinge capability',
            'moisture resistant', 'waterproof',
        ],
        'industry': ['natural pp', 'white pp', 'homopolymer', 'copolymer'],
    },

    'peek': {
        'alternates': [
            'polyether ether ketone', 'polyetheretherketone',
            'peek plastic', 'ketron peek',
            'high performance plastic', 'engineering plastic',
            'high temperature plastic',
        ],
        'misspellings': ['pek plastic', 'peek sheet'],
        'spanish': ['peek plastico', 'polietereeterketona'],
        'astm': ['ASTM D6262'],
        'applications': [
            'aerospace component', 'aerospace grade',
            'medical device', 'medical grade', 'implantable',
            'semiconductor', 'semiconductor equipment',
            'high temperature application',
            'chemical resistance application',
            'precision machining', 'machined part',
            'bearing', 'bushing', 'seal',
        ],
        'properties': [
            'high temperature resistance', 'continuous use 480f',
            'chemical resistant', 'radiation resistant',
            'flame retardant', 'low smoke emission',
            'biocompatible', 'sterilizable',
            'high strength', 'high stiffness',
            'dimensional stability', 'creep resistant',
        ],
        'industry': ['natural peek', 'unfilled peek', 'glass filled peek'],
    },

    'delrin': {
        'alternates': [
            'acetal', 'pom', 'polyoxymethylene', 'polyacetal',
            'acetal copolymer', 'acetal homopolymer',
            'celcon', 'hostaform',
            'acetal plastic', 'delrin rod',
        ],
        'misspellings': ['delren', 'acetal plastic sheet'],
        'spanish': ['acetal plastico', 'polioximetileno'],
        'astm': ['ASTM D4181'],
        'applications': [
            'gear', 'sprocket', 'pulley',
            'bushing', 'bearing', 'wear part',
            'precision machining', 'cnc machined part',
            'valve', 'pump component',
            'food grade', 'fda compliant',
            'electrical insulation',
            'standoff', 'spacer',
        ],
        'properties': [
            'self lubricating', 'low friction',
            'wear resistant', 'fatigue resistant',
            'stiff', 'hard', 'rigid',
            'dimensional stability', 'tight tolerance',
            'chemical resistant', 'solvent resistant',
            'machinable', 'easy to machine',
            'natural white', 'black',
        ],
        'industry': ['natural acetal', 'black acetal', 'delrin 150'],
    },

    'ptfe': {
        'alternates': [
            'teflon', 'polytetrafluoroethylene',
            'ptfe plastic', 'teflon sheet',
            'fluoropolymer', 'fluoroplastic',
        ],
        'misspellings': ['telfon', 'teflone', 'ptfe plastic sheet'],
        'spanish': ['teflon plastico', 'politetrafluoroetileno'],
        'astm': ['ASTM D3294'],
        'applications': [
            'chemical resistant lining', 'corrosion resistant',
            'non stick surface', 'release surface',
            'electrical insulation', 'high voltage insulation',
            'bearing', 'seal', 'gasket',
            'food grade', 'fda compliant',
            'high temperature application',
            'laboratory equipment',
        ],
        'properties': [
            'lowest friction coefficient',
            'non stick', 'chemical resistant',
            'high temperature resistance',
            'electrical insulation',
            'food safe', 'fda approved',
            'uv resistant', 'weatherproof',
            'biocompatible',
        ],
        'industry': ['virgin ptfe', 'filled ptfe', 'glass filled ptfe'],
    },
}

# ============================================================
# PRODUCT TYPE SYNONYMS
# ============================================================

PRODUCT_TYPE_SYNONYMS = {
    'sheet': [
        'sheet', 'board', 'panel', 'plate', 'flat stock',
        'slab', 'blank', 'laminate', 'cut to size',
    ],
    'rod': [
        'rod', 'bar', 'round bar', 'solid rod', 'dowel',
        'round stock', 'bar stock', 'cylinder',
    ],
    'tube': [
        'tube', 'tubing', 'hollow tube', 'round tube',
        'pipe', 'hollow rod', 'sleeve',
    ],
    'cutting_board': [
        'cutting board', 'chopping board', 'prep board',
        'food prep surface', 'butcher block alternative',
        'commercial cutting board', 'restaurant cutting board',
    ],
    'adhesive': [
        'adhesive', 'cement', 'glue', 'bonding agent',
        'solvent cement', 'plastic cement', 'acrylic cement',
        'weld on', 'weld-on',
    ],
    'accessory': [
        'accessory', 'tool', 'hardware', 'fitting',
        'supply', 'component',
    ],
}

# ============================================================
# APPLICATION KEYWORDS BY USE CASE
# These are high-intent buyer search terms
# ============================================================

APPLICATION_KEYWORDS = {
    'fabrication': [
        'fabrication', 'plastic fabrication', 'custom fabrication',
        'cut to size', 'machining', 'cnc machining',
        'routing', 'drilling', 'sawing',
    ],
    'diy': [
        'diy', 'do it yourself', 'home project',
        'craft project', 'hobby', 'maker',
        'workshop', 'home improvement',
    ],
    'industrial': [
        'industrial', 'manufacturing', 'production',
        'oem', 'replacement part', 'industrial supply',
        'mro', 'maintenance repair operations',
    ],
    'food_service': [
        'food grade', 'food safe', 'fda approved',
        'nsf compliant', 'food contact', 'food processing',
        'restaurant', 'commercial kitchen', 'food service',
        'haccp', 'sanitary',
    ],
    'display': [
        'display', 'retail display', 'product display',
        'point of sale', 'showcase', 'exhibit',
        'trade show', 'store fixture',
    ],
    'protection': [
        'machine guard', 'safety guard', 'protective barrier',
        'splash guard', 'sneeze guard', 'shield',
        'protective cover', 'safety shield',
    ],
    'outdoor': [
        'outdoor', 'exterior', 'weatherproof',
        'uv resistant', 'weather resistant',
        'outdoor sign', 'outdoor display',
    ],
    'laser': [
        'laser cutting', 'laser engraving', 'laser etching',
        'co2 laser', 'laser compatible', 'laser material',
        'glowforge', 'epilog laser', 'trotec',
    ],
    'medical': [
        'medical grade', 'medical device',
        'biocompatible', 'sterilizable',
        'hospital', 'healthcare',
    ],
    'aerospace': [
        'aerospace', 'aerospace grade', 'as9100',
        'mil spec', 'aviation', 'aircraft',
    ],
}

# ============================================================
# TARGET AUDIENCE KEYWORDS
# ============================================================

TARGET_AUDIENCE = {
    'professional': [
        'fabricator', 'machinist', 'engineer',
        'manufacturer', 'oem', 'production',
        'industrial buyer', 'procurement',
    ],
    'diy': [
        'hobbyist', 'maker', 'diy enthusiast',
        'home improvement', 'crafter',
    ],
    'food_service': [
        'chef', 'restaurant owner', 'food service professional',
        'commercial kitchen', 'catering',
    ],
    'technical': [
        'design engineer', 'mechanical engineer',
        'product designer', 'r&d',
    ],
}

# ============================================================
# BACKEND SEARCH TERM BUILDER
# Combines library terms with Claude-generated terms
# Respects 249 byte limit and no-repeat rule
# ============================================================

# Terms that are PROHIBITED by Amazon
PROHIBITED_TERMS = [
    'best', 'top', 'guaranteed', 'free shipping',
    'sale', 'discount', 'cheap', 'cheapest',
    'lowest price', 'best price', 'number one', '#1',
    'amazing', 'awesome', 'excellent',
    # Competitor brand names
    'eplastics', 'cope plastics', 'piedmont plastics',
    'professional plastics', 'interstate plastics',
    'mcmaster', 'grainger', 'amazon',
]

def get_base_backend_terms(material_key, product_type):
    """
    Build base backend search terms from the library.
    Returns a list of terms to combine with Claude-generated terms.
    """
    terms = set()

    mat = MATERIAL_SYNONYMS.get(material_key, {})

    # Add alternate names (most important)
    for t in mat.get('alternates', [])[:6]:
        terms.add(t)

    # Add top misspellings (1-2 only)
    for t in mat.get('misspellings', [])[:2]:
        terms.add(t)

    # Add Spanish terms (1-2)
    for t in mat.get('spanish', [])[:1]:
        terms.add(t)

    # Add top applications (most relevant)
    for t in mat.get('applications', [])[:5]:
        terms.add(t)

    # Add key properties (2-3)
    for t in mat.get('properties', [])[:3]:
        terms.add(t)

    # Add product type synonyms
    for t in PRODUCT_TYPE_SYNONYMS.get(product_type, [])[:3]:
        terms.add(t)

    return list(terms)

def clean_and_trim_backend_terms(terms_list, title_words, max_bytes=249):
    """
    Clean backend terms:
    - Remove words already in title
    - Remove prohibited terms
    - Trim to max_bytes
    - Return as space-separated string
    """
    # Flatten title words for comparison
    title_lower = set(title_words.lower().split())

    # Clean terms
    cleaned = []
    for term in terms_list:
        term_lower = term.lower().strip()

        # Skip if prohibited
        if any(p in term_lower for p in PROHIBITED_TERMS):
            continue

        # Skip if all words already in title
        term_words = set(term_lower.split())
        if term_words.issubset(title_lower):
            continue

        cleaned.append(term_lower)

    # Remove duplicates while preserving order
    seen = set()
    unique = []
    for t in cleaned:
        if t not in seen:
            seen.add(t)
            unique.append(t)

    # Build string within byte limit
    result = []
    current_bytes = 0

    for term in unique:
        term_bytes = len(term.encode('utf-8')) + 1  # +1 for space
        if current_bytes + term_bytes <= max_bytes:
            result.append(term)
            current_bytes += term_bytes
        else:
            break

    return ' '.join(result)

def get_subject_matter(material_key, product_type):
    """Generate subject matter field value."""
    mat_name = material_key.upper() if material_key in ['abs', 'hdpe', 'pvc', 'uhmw', 'peek', 'ptfe'] else material_key.capitalize()

    subjects = {
        'sheet': f'{mat_name} Plastic Sheet, Raw Material, Industrial Plastic',
        'rod': f'{mat_name} Plastic Rod, Raw Material, Round Stock',
        'tube': f'{mat_name} Plastic Tube, Raw Material, Hollow Tube',
        'cutting_board': 'Cutting Board, Food Grade Plastic, Kitchen Equipment',
        'cutting_board_oem': 'OEM Replacement Cutting Board, Food Service Equipment',
        'adhesive': 'Plastic Adhesive, Solvent Cement, Bonding Agent',
        'accessory': 'Plastic Accessory, Fabrication Supply, Plastic Tool',
    }
    return subjects.get(product_type, f'{mat_name} Plastic, Industrial Material')

def get_intended_use(material_key, product_type):
    """Generate intended use field value."""
    uses = {
        'sheet': 'Fabrication, Machining, DIY Projects, Industrial Applications, Prototyping',
        'rod': 'Machining, Turning, Fabrication, Industrial Applications, DIY Projects',
        'tube': 'Fabrication, Fluid Handling, Display, Industrial Applications',
        'cutting_board': 'Food Preparation, Commercial Kitchen, Food Service',
        'cutting_board_oem': 'OEM Replacement, Food Preparation, Commercial Kitchen',
        'adhesive': 'Plastic Bonding, Fabrication, Repair, Assembly',
        'accessory': 'Plastic Fabrication, DIY Projects, Industrial Use',
    }
    return uses.get(product_type, 'Industrial Applications, Fabrication')

def get_target_audience(material_key, product_type):
    """Generate target audience field value."""
    if product_type in ('cutting_board', 'cutting_board_oem'):
        return 'Food Service Professionals, Restaurant Owners, Commercial Kitchens'
    elif material_key in ('peek', 'ptfe'):
        return 'Engineers, Manufacturers, Aerospace Professionals, Medical Device Manufacturers'
    elif material_key in ('uhmw', 'delrin', 'nylon'):
        return 'Machinists, Engineers, Industrial Buyers, Fabricators'
    else:
        return 'Fabricators, Engineers, DIY Enthusiasts, Industrial Buyers, Hobbyists'

# ============================================================
# QUICK TEST
# ============================================================

if __name__ == '__main__':
    import sys
    sys.path.insert(0, '/home/claude')
    from pc_logger import setup_logger

    logger, jlog, run_id = setup_logger('pc_synonym_library')
    logger.info("Testing Synonym Library...")

    test_cases = [
        ('acrylic', 'sheet', "Plastic-Craft | Cast Acrylic Sheet, Transparent Red, 1/4\" Thick, 12\" W x 24\" L"),
        ('hdpe', 'cutting_board', "Plastic-Craft | Plastic Cutting Board Sheet, Food Grade HDPE, White, 1/4\" Thick"),
        ('nylon', 'rod', "Plastic-Craft | Cast Nylon Round Rod, Natural (White), 1\" OD x 12\" L"),
        ('uhmw', 'sheet', "Plastic-Craft | UHMW Sheet, Natural, 1/2\" Thick, 12\" W x 24\" L"),
        ('peek', 'rod', "Plastic-Craft | PEEK Round Rod, Natural, 1\" OD x 12\" L"),
    ]

    all_passed = True
    for mat, ptype, title in test_cases:
        terms = get_base_backend_terms(mat, ptype)
        backend = clean_and_trim_backend_terms(terms, title)
        byte_count = len(backend.encode('utf-8'))
        status = 'PASS' if byte_count <= 249 else 'FAIL'
        if status == 'FAIL':
            all_passed = False
        logger.info(f"  [{status}] {mat} {ptype}: {byte_count} bytes")
        logger.debug(f"    Terms: {backend[:80]}...")
        jlog.log('library_test', {
            'material': mat, 'product_type': ptype,
            'byte_count': byte_count, 'status': status
        })

    logger.info("")
    logger.info("Attribute field tests:")
    logger.info(f"  Subject matter (acrylic/sheet): {get_subject_matter('acrylic', 'sheet')}")
    logger.info(f"  Intended use (hdpe/board):      {get_intended_use('hdpe', 'cutting_board')}")
    logger.info(f"  Target audience (peek/rod):     {get_target_audience('peek', 'rod')}")

    if all_passed:
        logger.info("✓ All synonym library tests passed")
    else:
        logger.warning("⚠ Some tests failed — review byte counts")
