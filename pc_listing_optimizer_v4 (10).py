"""
Plastic-Craft Amazon Listing Optimizer v4.0
Complete production-ready catalog optimizer

NEW IN v4.0:
  - MM to inches conversion (detects mm anywhere in title)
  - 16 product shape formats (sheet, rod, tube, square rod/tube,
    triangular rod/tube, angle bar, sphere, half sphere, cube,
    cutting board, cutting board OEM, adhesive/Weld-On, accessory)
  - Third party brand detection (no Plastic-Craft prefix)
  - Solid/Hollow/Tumbled descriptors for spheres and cubes
  - Intended use points per material AND per shape
  - Image audit flag column (transparency/quantity mismatch detection)
  - HTML review window (auto-refreshing browser page for image decisions)
  - Profile weight calculations (square, triangular, angle bar, sphere, cube)
  - Sanity check fraction/decimal tolerance fix
  - Cast/Extruded for Acrylic and Nylon only
  - Plastic-Craft | prefix on all owned products
  - Weld-On format: Brand #N Type, DCM & TCE Free Formula, Size (Pack of N)

Self-correction layers:
  L1 - Pre-API input validation
  L2 - Post-API output validation on every field
  L3 - Auto-correction without retry
  L4 - Targeted single-field retry
  L5 - Escalation logging with partial data preservation

Additional features:
  - Checkpoint save every 50 listings (resume from interruption)
  - Freight notice auto-detection and injection
  - Self-healing engine (rewrites broken functions via Claude API)
  - Dimension sanity checker (4 layers of physical validation)
  - Comprehensive logging (console + file + JSON events)
"""

import csv
import json
import re
import time
import os
import sys
import math
from datetime import datetime
from pathlib import Path

# ============================================================
# CROSS-PLATFORM PATH SETUP
# Works on both Windows and Linux/Mac
# ============================================================

# Script directory — all helper files must be in the same folder
SCRIPT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SCRIPT_DIR))

from pc_synonym_library import (
    get_base_backend_terms,
    clean_and_trim_backend_terms,
    get_subject_matter,
    get_intended_use,
    get_target_audience,
)

import pc_listing_optimizer_v4 as _self_module
from pc_logger import setup_logger, write_run_summary
from pc_self_heal import SelfHealingEngine, DimensionSanityChecker

# ============================================================
# CONFIGURATION — Edit these paths for your machine
# ============================================================

# Input: your All Listings Report from Amazon Seller Central
INPUT_FILE = str(SCRIPT_DIR / 'All+Listings+Report_04-17-2026.txt')

# Output directory — defaults to same folder as script
LOG_DIR = str(SCRIPT_DIR)

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = str(SCRIPT_DIR / f'pc_amazon_feed_v4_{RUN_ID}.csv')
ERROR_LOG = str(SCRIPT_DIR / f'pc_errors_v4_{RUN_ID}.csv')
CHECKPOINT_FILE = str(SCRIPT_DIR / 'pc_checkpoint_v4.json')

TEST_MODE = True
TEST_LIMIT = 8

logger, jlog, RUN_ID = setup_logger('pc_optimizer_v4', LOG_DIR)

WEIGHT_BUFFER = 1.10
BOX_BUFFER = 2.0
CHECKPOINT_INTERVAL = 50
MAX_RETRIES = 3
RETRY_DELAY = 2

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
# THIRD PARTY BRANDS
# These get NO Plastic-Craft prefix
# ============================================================

THIRD_PARTY_BRANDS = {
    'weld-on': 'Weld-On',
    'weldon': 'Weld-On',
    'craftics': 'Craftics',
    'ips': 'IPS',
    'scigrip': 'SciGrip',
    'novus': 'Novus',
    'brillianize': 'Brillianize',
    'dykem': 'Dykem',
    'loctite': 'Loctite',
}

# Weld-On products that get DCM & TCE Free label
WELD_ON_DCM_FREE = ['3', '4', '16']

# ============================================================
# FREIGHT
# ============================================================

FREIGHT_BLURB = (
    "FREIGHT SHIPPING NOTICE: Due to the size and/or weight of this item, "
    "it MAY ship via freight carrier. "
    "If shipped freight, the following additional charges may apply and are "
    "the responsibility of the buyer: "
    "(1) Residential delivery surcharge — if delivered to a non-commercial address; "
    "(2) Liftgate fee — if a liftgate is required for unloading; "
    "(3) Limited access delivery fee — if delivery location has restricted access "
    "(e.g. correctional facilities, military bases, schools, or remote locations); "
    "(4) Inside delivery fee — if delivery inside the building is required; "
    "(5) Layover/re-delivery fee — if delivery cannot be completed on first attempt; "
    "(6) Weight reclassification fee — if the carrier reclassifies the freight weight "
    "upon pickup; "
    "(7) Freight class adjustment — if shipment is reclassified by the carrier. "
    "If you have any questions regarding freight delivery, additional charges, "
    "or need a shipping estimate prior to purchase, please contact our storefront "
    "BEFORE placing your order. "
    "By completing your purchase, you acknowledge these potential charges. "
    "Any freight surcharges incurred that were not collected at checkout will be "
    "invoiced directly to the buyer."
)

FREIGHT_TRIGGERS = {
    'sheet_length': 44,    # Lowered from 48 — catches items losing money at 44-47"
    'sheet_width': 30,     # Short side 30+ triggers Mid Sheet
    'rod_tube_length': 44, # Lowered from 48 — catches rods/tubes losing money at 44-47"
    'weight_lbs': 70,      # Heavy items always get freight notice
    'box_dimension': 96,   # Oversized box always gets freight notice
    'dim_weight_multiplier': 1.50,  # Flag if DIM weight is 50%+ above actual weight
}

def needs_freight_notice(dims, weight, box_dims, actual_weight=None):
    """
    Returns True if this listing needs a freight notice.
    Triggers:
    - Chargeable weight over 70 lbs
    - Any box dimension over 96"
    - Box length/width over 44" for sheets (lowered from 48 — catches $16/$45 gap)
    - Box length over 44" for rods/tubes
    - DIM weight is 50%+ above actual weight
    """
    ptype = dims.get('product_type', '')

    # Weight trigger — use chargeable weight
    if weight and weight > FREIGHT_TRIGGERS['weight_lbs']:
        return True

    # Box dimension trigger
    if box_dims:
        for val in box_dims.values():
            if val and val > FREIGHT_TRIGGERS['box_dimension']:
                return True

    # Sheet triggers — use box dims
    if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
        bl = box_dims.get('box_length', 0) if box_dims else dims.get('length', 0)
        bw = box_dims.get('box_width', 0) if box_dims else dims.get('width', 0)
        if bl >= FREIGHT_TRIGGERS['sheet_length']: return True
        if bw >= FREIGHT_TRIGGERS['sheet_width']: return True

    # Rod/tube triggers — use box length
    elif ptype in ('rod', 'tube', 'square_rod', 'square_tube',
                   'triangular_rod', 'triangular_tube', 'angle_bar'):
        bl = box_dims.get('box_length', 0) if box_dims else dims.get('length', 0)
        if bl >= FREIGHT_TRIGGERS['rod_tube_length']: return True

    # DIM weight trigger — catches items where carrier charges 50%+ more than actual
    # These are exactly the items losing money at $16 charged vs $45 actual
    if actual_weight and actual_weight > 0 and weight and weight > actual_weight:
        if weight >= actual_weight * FREIGHT_TRIGGERS['dim_weight_multiplier']:
            return True

    return False

# ============================================================
# RUFUS Q&A — All 11 Materials
# These feed Amazon's AI assistant directly.
# Written as natural language Q&A to match buyer intent.
# ============================================================

RUFUS_QA = {
    'acrylic': [
        {
            'q': 'Can this acrylic be cut with standard woodworking tools?',
            'a': 'Yes — cuts cleanly with a table saw, band saw, or router using a fine-tooth blade. Score-and-snap works for straight cuts on thinner sheets. Always use a slow feed rate to prevent chipping.'
        },
        {
            'q': 'Will this acrylic yellow or crack outdoors?',
            'a': 'Cast acrylic resists UV yellowing significantly better than extruded. Both grades maintain clarity for years in typical outdoor applications. For extended direct sun exposure, a UV-protective coating is recommended.'
        },
        {
            'q': 'How does the clarity of this acrylic compare to glass?',
            'a': '92% light transmission — actually clearer than standard glass, with significantly better impact resistance. Ideal for display cases, picture frames, sneeze guards, and anywhere optical clarity matters.'
        },
        {
            'q': 'What is the difference between cast and extruded acrylic?',
            'a': 'Cast acrylic offers better optical clarity, tighter thickness tolerances, and superior chemical resistance — ideal for display and precision fabrication. Extruded acrylic is more consistent in thickness and easier to thermoform, making it the preferred choice for forming and bending applications.'
        },
        {
            'q': 'Can acrylic be bonded or glued?',
            'a': 'Yes — bonds excellently with solvent cement like Weld-On #3 or #4 for near-invisible joints. Also compatible with cyanoacrylate (super glue) and two-part epoxy for structural applications.'
        },
    ],
    'abs': [
        {
            'q': 'Is ABS plastic strong enough for enclosures and machine guards?',
            'a': 'Yes — ABS delivers 5,200 PSI tensile strength and 3.0 ft-lb/in impact resistance, making it one of the toughest rigid thermoplastics for enclosures, panels, and protective housings.'
        },
        {
            'q': 'Can ABS plastic be painted or bonded?',
            'a': 'Yes — ABS accepts most paints and adhesives without priming. Bonds well with ABS solvent cement, cyanoacrylate, and structural epoxies. Excellent for painted enclosures and assemblies.'
        },
        {
            'q': 'Will ABS hold up in heat or outdoor applications?',
            'a': 'ABS handles continuous service up to 180°F — suitable for most indoor industrial and commercial environments. For outdoor UV exposure, a UV-stabilized grade or painted finish is recommended, as standard ABS can degrade under prolonged sunlight.'
        },
        {
            'q': 'Can ABS be vacuum formed or thermoformed?',
            'a': 'Yes — ABS is one of the most commonly thermoformed plastics. It softens predictably at 200-220°F and reproduces fine detail. Widely used for automotive trim, enclosures, and custom packaging.'
        },
        {
            'q': 'How does ABS compare to polycarbonate for machine guards?',
            'a': 'Polycarbonate offers significantly higher impact resistance (250x stronger than glass vs. ABS). ABS is preferred when machinability, paintability, and cost are priorities. PC is the choice when optical clarity and shatter resistance are critical.'
        },
    ],
    'hdpe': [
        {
            'q': 'Is this HDPE food safe and FDA approved?',
            'a': 'Yes — compliant with FDA 21 CFR and NSF standards for direct food contact. USDA approved. Safe for cutting boards, food prep surfaces, food processing equipment, and any direct food contact application.'
        },
        {
            'q': 'Will HDPE warp, absorb odors, or stain?',
            'a': 'No — HDPE has virtually zero moisture absorption, does not retain odors or stains, and resists warping under normal conditions. One of the most hygienic plastics for food service environments.'
        },
        {
            'q': 'Can HDPE be used outdoors or in marine environments?',
            'a': 'Yes — HDPE is UV stable, highly resistant to moisture, and unaffected by saltwater corrosion. Widely used for marine dock components, outdoor furniture, and exterior applications where long-term weather resistance is required.'
        },
        {
            'q': 'Is HDPE chemical resistant?',
            'a': 'Yes — excellent resistance to most acids, bases, alcohols, and cleaning chemicals. Resistant to bleach, detergents, and many industrial solvents. Check a chemical resistance chart for aggressive chemicals or elevated temperatures.'
        },
        {
            'q': 'Can HDPE be welded or fabricated?',
            'a': 'Yes — HDPE welds well with hot gas or extrusion welding. Machines cleanly with standard woodworking tools. Does not bond with solvent cement — use mechanical fasteners or welding for assemblies.'
        },
    ],
    'polycarbonate': [
        {
            'q': 'Is polycarbonate actually shatterproof?',
            'a': 'Yes — polycarbonate delivers 250x the impact resistance of glass with 16 ft-lb/in notched impact strength. It will not shatter under normal impact loads — the standard choice for machine guards, safety shields, and bullet-resistant barriers.'
        },
        {
            'q': 'Will polycarbonate work for a machine guard or safety shield?',
            'a': 'Yes — polycarbonate is the industry standard for machine guards, safety shields, and protective barriers precisely because of its combination of optical clarity and near-unbreakable toughness. Rated UL 94 V-2 flame resistant.'
        },
        {
            'q': 'Is polycarbonate UV stable for greenhouse or outdoor use?',
            'a': 'Standard polycarbonate provides moderate UV resistance but can yellow over time with prolonged direct sun exposure. For greenhouse panels and long-term outdoor use, specify a UV-coated or UV-stabilized grade for maximum longevity.'
        },
        {
            'q': 'Can polycarbonate be cut and fabricated?',
            'a': 'Yes — cuts with standard carbide-tipped saw blades, drills with standard bits, and bends with controlled heat. Keep the protective masking on during fabrication to prevent scratching. Use PC-compatible adhesives or mechanical fasteners for assembly.'
        },
        {
            'q': 'How does polycarbonate compare to acrylic?',
            'a': 'Polycarbonate is significantly tougher (impact resistance 30x higher than acrylic) but slightly less optically clear. Acrylic is preferred for display applications requiring maximum clarity. Polycarbonate is preferred anywhere impact resistance and safety are priorities.'
        },
    ],
    'nylon': [
        {
            'q': 'Will nylon machine cleanly on a lathe or mill?',
            'a': 'Yes — nylon is one of the most machinable engineering plastics. Cuts cleanly on lathes, mills, and CNC machines. Use sharp tooling, moderate speeds, and a light cutting fluid or air blast to manage heat and achieve tight tolerances.'
        },
        {
            'q': 'Does nylon absorb moisture and change dimensions?',
            'a': 'Nylon does absorb some moisture (1-3% depending on grade and humidity), which can cause slight dimensional change. For precision applications, allow nylon to acclimate to its operating environment. Nylon 6/6 absorbs less moisture than Nylon 6.'
        },
        {
            'q': 'Is nylon strong enough for gears and bearing applications?',
            'a': 'Yes — nylon delivers 12,000 PSI tensile strength with self-lubricating properties, making it the most common plastic for gears, bushings, bearings, and wear parts. Operates quietly, reduces noise compared to metal gears, and runs dry without additional lubrication.'
        },
        {
            'q': 'What is the difference between cast and extruded nylon?',
            'a': 'Cast nylon offers better crystallinity, higher tensile strength, and improved chemical resistance — preferred for large structural and wear parts. Extruded nylon provides tighter dimensional tolerances and more consistent properties for precision machined components.'
        },
        {
            'q': 'Is nylon food safe?',
            'a': 'FDA-compliant grades of nylon are available and widely used in food processing equipment, conveyor components, and food contact applications. Confirm the specific grade\'s FDA compliance for your application before use.'
        },
    ],
    'uhmw': [
        {
            'q': 'How long will UHMW last in a high-wear application?',
            'a': 'UHMW has the highest abrasion resistance of any thermoplastic and no break impact strength — designed to outlast steel in many wear applications. Conveyor liners, wear strips, and chute liners made from UHMW regularly last years in demanding industrial environments.'
        },
        {
            'q': 'Is UHMW food safe for conveyor and food processing applications?',
            'a': 'Yes — FDA 21 CFR, NSF, and USDA approved. Widely used in food processing conveyor systems, star wheels, guide rails, and food contact wear components. Non-porous surface resists bacterial growth.'
        },
        {
            'q': 'Can UHMW be bonded or welded?',
            'a': 'UHMW is notoriously difficult to bond with adhesives due to its extremely low surface energy. Hot gas welding works for fabricated assemblies. For most applications, mechanical fasteners are the most reliable joining method.'
        },
        {
            'q': 'How does UHMW compare to nylon for wear applications?',
            'a': 'UHMW has significantly higher abrasion resistance and lower friction than nylon, making it the preferred choice for high-wear sliding and conveyor applications. Nylon offers higher tensile strength and is preferred for structural and load-bearing components.'
        },
        {
            'q': 'Can UHMW be used at low temperatures?',
            'a': 'Yes — UHMW maintains toughness down to -300°F, making it one of the few plastics suitable for cryogenic applications. Widely used in cold storage, freezer equipment, and extreme cold environments.'
        },
    ],
    'pvc': [
        {
            'q': 'Is PVC resistant to the chemicals in my application?',
            'a': 'PVC resists 90%+ of acids, bases, and salts — excellent for chemical tanks, fume hoods, and laboratory equipment. Always verify against a PVC chemical resistance chart for aggressive solvents, concentrated oxidizers, or elevated temperatures, where resistance can vary.'
        },
        {
            'q': 'Is PVC flame retardant?',
            'a': 'Yes — PVC is rated UL 94 V-0 flame resistant, meaning it self-extinguishes when the ignition source is removed. This makes it the preferred choice for electrical panels, fume hoods, and applications with fire safety requirements.'
        },
        {
            'q': 'Can PVC be solvent cemented or welded?',
            'a': 'Yes — PVC bonds readily with PVC solvent cement for strong, permanent joints. Also weldable with hot gas welding for fabricated tanks and chemical vessels. One of the easiest plastics to fabricate and assemble.'
        },
        {
            'q': 'What is the temperature limit for PVC?',
            'a': 'PVC is rated for continuous service from 32°F to 140°F. Above 140°F, PVC begins to soften and lose structural integrity. For higher temperature chemical applications, consider CPVC or polypropylene as alternatives.'
        },
        {
            'q': 'Can PVC be used for outdoor applications?',
            'a': 'Standard PVC can yellow and become brittle with prolonged UV exposure. For outdoor use, specify UV-stabilized PVC or apply a UV-protective paint or coating to extend service life significantly.'
        },
    ],
    'polypropylene': [
        {
            'q': 'Is polypropylene food safe?',
            'a': 'Yes — FDA 21 CFR and NSF compliant. Polypropylene is one of the most widely used food-safe plastics for food processing equipment, containers, and food contact surfaces. Resists most cleaning chemicals and sanitizers.'
        },
        {
            'q': 'What makes polypropylene unique compared to other plastics?',
            'a': 'Polypropylene is the only common plastic capable of a true living hinge — it can flex millions of times without fatigue, making it ideal for snap-lid containers, hinged enclosures, and repeated-flex applications.'
        },
        {
            'q': 'How chemical resistant is polypropylene?',
            'a': 'Excellent resistance to acids, bases, alcohols, and most organic solvents — comparable to HDPE in many chemical environments. Particularly resistant to cleaning agents and detergents. Verify against a PP chemical resistance chart for aggressive solvents.'
        },
        {
            'q': 'Can polypropylene be welded or fabricated?',
            'a': 'Yes — polypropylene welds well with hot gas and extrusion welding. Machines cleanly with standard tooling. Like HDPE, it does not bond reliably with solvent cement — use mechanical fasteners or welding for fabricated assemblies.'
        },
        {
            'q': 'What is the temperature limit for polypropylene?',
            'a': 'Polypropylene handles continuous service from -20°F to 200°F — higher than HDPE and suitable for hot-fill food applications and steam sterilization at moderate temperatures.'
        },
    ],
    'peek': [
        {
            'q': 'Will PEEK hold up at high continuous temperatures?',
            'a': 'Yes — PEEK maintains structural integrity at continuous service temperatures up to 480°F (250°C), with a heat deflection temperature of 600°F. One of the highest performance thermoplastics available for demanding high-temperature applications.'
        },
        {
            'q': 'Is PEEK biocompatible for medical device applications?',
            'a': 'Yes — PEEK is USP Class VI and FDA compliant, making it widely used in medical devices, surgical instruments, and implantable components. Its biocompatibility and radiolucency make it the preferred polymer for medical and dental applications.'
        },
        {
            'q': 'Can PEEK replace metal in my application?',
            'a': 'Often yes — PEEK delivers 14,500 PSI tensile strength, excellent chemical resistance, and thermal stability up to 480°F at a fraction of the weight of metal. Used to replace aluminum, stainless steel, and titanium in aerospace, medical, and semiconductor applications.'
        },
        {
            'q': 'Is PEEK machinable?',
            'a': 'Yes — PEEK machines to extremely tight tolerances with standard CNC equipment. Produces clean chips, holds dimensions well, and does not require special tooling. The preferred high-performance polymer for precision machined components.'
        },
        {
            'q': 'What industries use PEEK?',
            'a': 'Aerospace, medical devices, semiconductor manufacturing, oil and gas, and high-performance automotive. PEEK is specified whenever the application demands the absolute highest combination of temperature resistance, chemical resistance, strength, and dimensional stability.'
        },
    ],
    'delrin': [
        {
            'q': 'Will Delrin hold tight tolerances after machining?',
            'a': 'Yes — Delrin (acetal/POM) has less than 0.2% moisture absorption, delivering exceptional dimensional stability before and after machining. Holds tolerances tighter than nylon in humid environments, making it the preferred choice for precision gears, cams, and mechanical components.'
        },
        {
            'q': 'How does Delrin compare to nylon for gears?',
            'a': 'Delrin is stiffer, absorbs less moisture, and holds tighter tolerances than nylon — preferred for precision gears where dimensional stability is critical. Nylon has better impact resistance and performs better in shock-loaded applications. Both are self-lubricating.'
        },
        {
            'q': 'Is Delrin self-lubricating?',
            'a': 'Yes — Delrin\'s inherent low friction coefficient allows it to run dry in many applications without additional lubrication. Widely used for gears, bushings, rollers, and sliding components where maintenance-free operation is required.'
        },
        {
            'q': 'Can Delrin be used in food processing applications?',
            'a': 'FDA-compliant grades of Delrin are available and widely used in food processing equipment, conveyor components, and food contact applications. Confirm the specific grade\'s FDA compliance for your application.'
        },
        {
            'q': 'What is Delrin used for?',
            'a': 'Precision gears, pulleys, bushings, bearings, cams, valve components, conveyor parts, and any application requiring a stiff, dimensionally stable, self-lubricating plastic. The go-to engineering plastic for precision mechanical components.'
        },
    ],
    'ptfe': [
        {
            'q': 'Is PTFE (Teflon) truly non-stick?',
            'a': 'Yes — PTFE has the lowest coefficient of friction of any solid material, making it essentially non-stick to virtually all substances. Used for release liners, non-stick surfaces, and any application where material adhesion must be prevented.'
        },
        {
            'q': 'Will PTFE hold up to my aggressive chemical environment?',
            'a': 'PTFE is chemically inert to virtually all industrial chemicals including strong acids, strong bases, oxidizers, and solvents. One of the very few materials resistant to hydrofluoric acid. The standard choice for chemical-resistant seals, gaskets, and linings.'
        },
        {
            'q': 'Can PTFE be used continuously at high temperatures?',
            'a': 'Yes — PTFE handles continuous service from -328°F to 500°F, one of the widest temperature ranges of any plastic. Maintains chemical inertness and non-stick properties across this entire range.'
        },
        {
            'q': 'Is PTFE electrically insulating?',
            'a': 'Yes — PTFE is an excellent electrical insulator with a very low dielectric constant, making it the standard material for high-frequency electrical insulation, wire coating, and circuit board components.'
        },
        {
            'q': 'What is PTFE used for?',
            'a': 'Chemical-resistant seals and gaskets, non-stick release liners, electrical insulation, laboratory equipment, medical devices, high-temperature bushings, and any application requiring the combination of chemical inertness, non-stick properties, and temperature resistance.'
        },
    ],
    'polyethylene': [
        {
            'q': 'What is polyethylene plastic used for?',
            'a': 'Polyethylene is one of the most widely used plastics — lightweight, flexible, and chemically resistant. Used for containers, packaging, tanks, liners, tubing, and general-purpose fabrication where chemical resistance and low cost are priorities.'
        },
        {
            'q': 'Is polyethylene chemical resistant?',
            'a': 'Yes — polyethylene resists most acids, bases, alcohols, and many solvents. Excellent choice for chemical storage containers, tanks, and liners. Check a PE chemical resistance chart for aggressive oxidizing acids or aromatic solvents.'
        },
        {
            'q': 'Can polyethylene be welded or fabricated?',
            'a': 'Yes — polyethylene welds well with hot gas and extrusion welding. Machines cleanly with standard tools. Does not bond reliably with solvent cement — use mechanical fasteners or welding for assemblies.'
        },
        {
            'q': 'Is polyethylene food safe?',
            'a': 'FDA-compliant grades of polyethylene are widely used in food processing, packaging, and food contact applications. Verify the specific grade meets FDA 21 CFR requirements for your application.'
        },
        {
            'q': 'How does polyethylene compare to HDPE?',
            'a': 'HDPE is a specific high-density grade of polyethylene offering superior stiffness and strength. Lower density PE grades are more flexible and impact resistant. The right grade depends on your specific application requirements.'
        },
    ],
    'noryl': [
        {
            'q': 'What is Noryl plastic and what is it used for?',
            'a': 'Noryl (modified PPO/PPE) is an engineering thermoplastic known for its exceptional dimensional stability, low moisture absorption, and excellent electrical insulating properties. Used in electrical housings, automotive components, and medical devices.'
        },
        {
            'q': 'Does Noryl absorb moisture or change dimensions?',
            'a': 'No — Noryl has extremely low moisture absorption (under 0.1%), one of the lowest of any engineering thermoplastic. This makes it ideal for precision components that must maintain tight tolerances in humid or wet environments.'
        },
        {
            'q': 'Is Noryl flame retardant?',
            'a': 'Yes — Noryl is available in UL 94 V-0 flame-rated grades, making it the preferred choice for electrical enclosures, connectors, and components requiring both dimensional stability and fire safety compliance.'
        },
        {
            'q': 'What temperature range does Noryl handle?',
            'a': 'Noryl handles continuous service temperatures from -40°F to 220°F with a heat deflection temperature up to 265°F depending on grade. Suitable for under-hood automotive applications and elevated-temperature electrical environments.'
        },
        {
            'q': 'Can Noryl be machined?',
            'a': 'Yes — Noryl machines well on standard CNC equipment. Produces clean chips, holds tight tolerances, and its low moisture absorption means dimensions stay consistent before and after machining.'
        },
    ],
    'petg': [
        {
            'q': 'What is PETG plastic used for?',
            'a': 'PETG combines the clarity of acrylic with the toughness of polycarbonate at a lower cost. Widely used for retail displays, point-of-purchase fixtures, protective guards, medical packaging, and 3D printing — anywhere clarity and impact resistance are both needed.'
        },
        {
            'q': 'How does PETG compare to acrylic?',
            'a': 'PETG is significantly tougher than acrylic (much higher impact resistance) and less brittle, making it less likely to crack during fabrication or in service. Acrylic offers slightly better optical clarity. PETG is the preferred choice when toughness matters as much as clarity.'
        },
        {
            'q': 'Is PETG food safe?',
            'a': 'Yes — PETG is FDA compliant for food contact applications. Widely used in food packaging, beverage containers, and food service displays.'
        },
        {
            'q': 'Can PETG be cut and fabricated?',
            'a': 'Yes — PETG cuts cleanly with standard saw blades and routers. Easier to fabricate than polycarbonate with less tendency to chip than acrylic. Bonds with PETG-compatible solvent cements and thermoforms at moderate temperatures.'
        },
        {
            'q': 'Is PETG UV resistant for outdoor use?',
            'a': 'Standard PETG has moderate UV resistance and can yellow with prolonged direct sun exposure. For outdoor applications, specify UV-stabilized PETG or apply a UV-protective coating.'
        },
    ],
    'polystyrene': [
        {
            'q': 'What is polystyrene used for?',
            'a': 'Polystyrene is a rigid, lightweight plastic used for display cases, laboratory equipment, signage, model making, and low-cost fabrication applications where clarity or white color and rigidity are needed without demanding mechanical requirements.'
        },
        {
            'q': 'Is polystyrene the same as Styrofoam?',
            'a': 'No — Styrofoam is expanded (foamed) polystyrene. Solid polystyrene sheet and rod is a rigid, clear or white engineering plastic used for display fabrication, laboratory trays, and light-duty enclosures.'
        },
        {
            'q': 'Can polystyrene be cut and fabricated?',
            'a': 'Yes — polystyrene cuts and machines easily with standard tools. Score-and-snap works for thin sheets. Bonds readily with polystyrene solvent cement. One of the easiest plastics to fabricate for display and model-making applications.'
        },
        {
            'q': 'What are the limitations of polystyrene?',
            'a': 'Polystyrene is brittle compared to acrylic or polycarbonate and not suitable for impact-demanding applications. It has limited chemical resistance and can craze or crack with exposure to certain solvents. Best suited for light-duty indoor applications.'
        },
        {
            'q': 'Is polystyrene food safe?',
            'a': 'FDA-compliant grades of polystyrene are used in food packaging and food service applications. However, polystyrene is not recommended for hot food contact or microwave use in most applications.'
        },
    ],
    'polyurethane': [
        {
            'q': 'What makes polyurethane different from other plastics?',
            'a': 'Polyurethane combines the toughness of rubber with the rigidity of hard plastic — available in a wide range of hardnesses (durometers). Exceptional abrasion resistance, high load-bearing capacity, and outstanding resistance to cuts, tears, and impact.'
        },
        {
            'q': 'What is polyurethane used for?',
            'a': 'Wheels, rollers, wear pads, bumpers, seals, squeegees, vibration isolation mounts, forming dies, and any application requiring a material that absorbs impact and resists abrasion better than rubber or rigid plastics.'
        },
        {
            'q': 'How does polyurethane compare to rubber?',
            'a': 'Polyurethane typically outlasts rubber 3-5x in wear applications, handles higher load capacities, and offers better oil and chemical resistance. It can be machined to tighter tolerances than rubber, making it preferred for precision wear components.'
        },
        {
            'q': 'Is polyurethane oil and chemical resistant?',
            'a': 'Polyurethane has good resistance to oils, greases, fuels, and many industrial chemicals. It is not recommended for prolonged exposure to strong acids, ketones, or chlorinated solvents. Excellent for hydraulic and lubrication system components.'
        },
        {
            'q': 'Can polyurethane be machined?',
            'a': 'Yes — cast polyurethane machines well with sharp tooling. Use slow speeds and sharp cutters to prevent heat buildup. Holds tolerances well for seals, rollers, and precision wear components.'
        },
    ],
    'torlon': [
        {
            'q': 'What is Torlon and why is it used in demanding applications?',
            'a': 'Torlon (PAI — Polyamide-Imide) is one of the highest performing thermoplastics available — combining exceptional strength (21,000 PSI tensile), continuous service at 500°F, and outstanding chemical resistance. Used when PEEK or other high-performance plastics are not sufficient.'
        },
        {
            'q': 'What temperature can Torlon handle?',
            'a': 'Torlon handles continuous service temperatures up to 500°F (260°C) — one of the highest of any thermoplastic. Maintains structural integrity and dimensional stability at temperatures that would cause other engineering plastics to fail.'
        },
        {
            'q': 'What industries use Torlon?',
            'a': 'Aerospace, semiconductor manufacturing, automotive (under-hood), oil and gas, and defense. Torlon is specified for the most demanding tribological and structural applications — bearing cages, thrust washers, valve seats, and structural components at extreme temperatures.'
        },
        {
            'q': 'Can Torlon be machined?',
            'a': 'Yes — Torlon machines to extremely tight tolerances with carbide tooling and CNC equipment. Note that Torlon requires stress-relief annealing after machining for critical applications to prevent stress cracking. Work with an experienced machinist familiar with high-performance thermoplastics.'
        },
        {
            'q': 'How does Torlon compare to PEEK?',
            'a': 'Torlon offers higher tensile strength and better performance at extreme temperatures than PEEK. PEEK has better chemical resistance in some environments and is easier to machine. Torlon is the choice when maximum strength and temperature resistance are the absolute priority.'
        },
    ],
    'ultem': [
        {
            'q': 'What is Ultem plastic used for?',
            'a': 'Ultem (PEI — Polyetherimide) is a high-performance thermoplastic used in aerospace, medical, automotive, and electronics applications requiring continuous service at elevated temperatures (340°F), excellent flame resistance, and strong mechanical properties.'
        },
        {
            'q': 'Is Ultem flame resistant?',
            'a': 'Yes — Ultem is rated UL 94 V-0 with extremely low smoke and toxic gas emission, making it the preferred choice for aircraft interior components, electrical connectors, and any application with strict fire safety requirements.'
        },
        {
            'q': 'Is Ultem used in medical devices?',
            'a': 'Yes — Ultem is USP Class VI compliant and withstands steam autoclave sterilization, making it widely used in medical instruments, surgical trays, and reusable medical device components that require repeated sterilization cycles.'
        },
        {
            'q': 'Can Ultem be machined?',
            'a': 'Yes — Ultem machines well on CNC equipment with carbide tooling. Holds tight tolerances and produces clean finishes. Widely used for precision aerospace and medical components requiring close dimensional control.'
        },
        {
            'q': 'How does Ultem compare to PEEK?',
            'a': 'Ultem offers better flame resistance and lower cost than PEEK. PEEK provides better chemical resistance and higher continuous service temperature (480°F vs 340°F for Ultem). Ultem is preferred when flame compliance is critical; PEEK when chemical resistance and maximum temperature performance are priorities.'
        },
    ],
    'vespel': [
        {
            'q': 'What is Vespel and what makes it unique?',
            'a': 'Vespel (DuPont polyimide) is one of the highest performance engineering plastics ever developed — maintains structural integrity from cryogenic temperatures (-452°F) up to 600°F continuous, with exceptional wear resistance and dimensional stability under extreme conditions.'
        },
        {
            'q': 'What industries use Vespel?',
            'a': 'Aerospace, semiconductor manufacturing, defense, and scientific instrumentation. Vespel is specified for the most extreme applications — thrust washers in jet engines, semiconductor wafer handling components, cryogenic seals, and any application where other materials simply cannot survive.'
        },
        {
            'q': 'Can Vespel be machined?',
            'a': 'Yes — Vespel machines to extremely tight tolerances with carbide tooling. It is one of the most dimensionally stable plastics available, making it the material of choice for precision components that must maintain tolerances across extreme temperature ranges.'
        },
        {
            'q': 'How does Vespel compare to Torlon and PEEK?',
            'a': 'Vespel offers the highest temperature performance of the three — continuous service to 600°F vs 500°F for Torlon and 480°F for PEEK. Vespel also has unmatched dimensional stability across extreme temperature ranges. It is also significantly more expensive, making it the choice of last resort when nothing else will work.'
        },
        {
            'q': 'Is Vespel used in semiconductor manufacturing?',
            'a': 'Yes — Vespel is the standard material for semiconductor wafer handling components, process chamber parts, and fixtures in high-temperature vacuum environments. Its low outgassing, dimensional stability, and resistance to semiconductor process chemicals make it uniquely suited for this application.'
        },
    ],
    'phenolic': [
        {
            'q': 'What is phenolic plastic used for?',
            'a': 'Phenolic (Bakelite/Garolite) is one of the oldest engineering plastics — still widely used for electrical insulation panels, circuit board substrates (G-10/FR4), structural components, gears, and bearings where high stiffness, electrical insulation, and heat resistance are required.'
        },
        {
            'q': 'Is phenolic a good electrical insulator?',
            'a': 'Yes — phenolic is an excellent electrical insulator and one of the standard materials for electrical panel boards, terminal strips, and switchgear components. Available in grades specifically optimized for electrical insulation applications.'
        },
        {
            'q': 'What temperature can phenolic handle?',
            'a': 'Phenolic handles continuous service temperatures up to 300-350°F depending on grade. Its heat resistance combined with excellent electrical insulation makes it preferred for motor components, transformer parts, and high-temperature electrical applications.'
        },
        {
            'q': 'Can phenolic be machined?',
            'a': 'Yes — phenolic machines well but produces abrasive dust that can wear tooling. Use carbide tooling, good ventilation, and a dust mask. Holds tight tolerances and produces excellent surface finishes for precision electrical and structural components.'
        },
        {
            'q': 'What is the difference between phenolic grades?',
            'a': 'Paper-based phenolic (XX, XXX) offers good electrical insulation at low cost. Canvas-based phenolic (C, CE) adds mechanical toughness for gears and bearings. Glass-based phenolic (G-10, G-11, FR4) provides the highest strength and moisture resistance for demanding structural and electrical applications.'
        },
    ],
    'hydlar_z': [
        {
            'q': 'What is Hydlar Z and what makes it special?',
            'a': 'Hydlar Z is a nylon composite reinforced with Kevlar fibers — combining nylon\'s machinability and chemical resistance with Kevlar\'s exceptional wear resistance and low friction. Delivers 3-5x longer wear life than standard nylon in demanding bearing and structural applications.'
        },
        {
            'q': 'What is Hydlar Z used for?',
            'a': 'Bearings, bushings, wear pads, thrust washers, gears, and structural components in demanding applications where standard nylon wears too quickly. Preferred in aerospace, defense, and industrial applications requiring extended service life without lubrication.'
        },
        {
            'q': 'How does Hydlar Z compare to standard nylon?',
            'a': 'Hydlar Z offers significantly higher wear resistance and lower friction than unfilled nylon, with comparable chemical resistance and machinability. The Kevlar reinforcement dramatically extends service life in sliding and abrasive contact applications.'
        },
        {
            'q': 'Can Hydlar Z be machined?',
            'a': 'Yes — Hydlar Z machines similarly to nylon with standard CNC equipment. The Kevlar fiber reinforcement can accelerate tool wear; use sharp carbide tooling and monitor tool condition closely for precision components.'
        },
        {
            'q': 'Is Hydlar Z self-lubricating?',
            'a': 'Yes — the Kevlar fiber matrix provides inherent lubricity, allowing Hydlar Z to run dry in many bearing and wear applications without external lubrication. This makes it ideal for food processing, medical, and clean-room environments where lubricants are not acceptable.'
        },
    ],
    'vinyl': [
        {
            'q': 'What is vinyl plastic used for?',
            'a': 'Flexible vinyl (PVC) is used for protective covers, curtains, signage, upholstery, industrial curtain walls, equipment covers, clear barriers, and any application requiring a flexible, durable, transparent or colored film or sheet material.'
        },
        {
            'q': 'How does vinyl differ from rigid PVC?',
            'a': 'Vinyl is plasticized flexible PVC — the addition of plasticizers makes it soft and flexible rather than rigid. It offers the chemical resistance of PVC in a bendable, rollable form. Used for applications where flexibility is required rather than structural rigidity.'
        },
        {
            'q': 'Is vinyl waterproof and weather resistant?',
            'a': 'Yes — vinyl is waterproof and resists moisture, mildew, and most environmental exposure. Widely used for outdoor curtains, protective covers, and weather barriers.'
        },
        {
            'q': 'Can vinyl be cut and fabricated?',
            'a': 'Yes — vinyl cuts easily with scissors, utility knife, or die cutting. Bonds with vinyl-compatible adhesives and can be heat sealed or RF welded for bag and curtain fabrication.'
        },
        {
            'q': 'Is vinyl flame retardant?',
            'a': 'Standard vinyl inherits PVC\'s natural flame resistance — it is self-extinguishing. FR-rated grades are available for applications requiring compliance with specific fire codes.'
        },
    ],
}

# ============================================================
# RECOMMENDED USES PER MATERIAL AND SHAPE
# Feeds Rufus and Amazon filtering directly
# ============================================================

RECOMMENDED_USES = {
    'acrylic': {
        'sheet': 'signage, display cases, window replacement, laser cutting, picture frames, sneeze guards, retail displays, aquariums, UV filtering panels',
        'rod': 'standoffs, display hardware, light pipes, decorative accents, craft projects, custom fabrication',
        'tube': 'display cases, light covers, fluid handling, aquarium plumbing, scientific apparatus',
        'sphere': 'decorative displays, craft projects, photography props, educational models, ornaments',
        'half_sphere': 'display domes, decorative accents, craft projects, photography props',
        'cube': 'display blocks, paperweights, decorative accents, photography props, awards',
        'default': 'fabrication, display, signage, laser cutting, CNC machining',
    },
    'abs': {
        'sheet': 'enclosures, machine guards, automotive trim, vacuum forming, prototyping, panels, electronic housings',
        'rod': 'standoffs, spacers, handles, prototyping, model making, custom components',
        'tube': 'protective sleeves, conduit, structural tubes, model making',
        'default': 'enclosures, prototyping, automotive, fabrication, vacuum forming',
    },
    'hdpe': {
        'sheet': 'cutting boards, food prep surfaces, marine applications, chemical tanks, wear pads, outdoor furniture',
        'cutting_board': 'commercial food prep, restaurant kitchens, catering, food service, butcher blocks',
        'cutting_board_oem': 'OEM replacement, commercial kitchen equipment, food service equipment repair',
        'rod': 'bushings, wear parts, food processing equipment, marine hardware, chemical processing',
        'default': 'food service, marine, chemical, outdoor, wear applications',
    },
    'polycarbonate': {
        'sheet': 'machine guards, safety shields, greenhouse panels, skylights, bullet-resistant barriers, riot shields',
        'rod': 'structural components, handles, custom machined safety parts',
        'tube': 'protective covers, structural tubes, light covers',
        'default': 'machine guards, safety, greenhouse, construction, industrial protection',
    },
    'nylon': {
        'sheet': 'wear pads, slide pads, structural panels, bearing plates, food processing equipment',
        'rod': 'gears, bushings, bearings, pulleys, wear parts, precision machined components, rollers',
        'tube': 'bushings, sleeves, wear liners, food processing components',
        'default': 'machining, gears, bearings, wear parts, food processing, industrial',
    },
    'uhmw': {
        'sheet': 'wear strips, conveyor liners, chute liners, truck bed liners, cutting boards, star wheels, guide rails',
        'rod': 'wear parts, bushings, rollers, food processing components',
        'default': 'wear, conveyor, food processing, industrial lining, cryogenic applications',
    },
    'pvc': {
        'sheet': 'chemical tanks, fume hoods, laboratory equipment, signage, electrical panels, wet environments',
        'rod': 'chemical process components, laboratory fixtures, custom machined parts',
        'tube': 'chemical tubing, laboratory equipment, plumbing, electrical conduit',
        'angle_bar': 'trim, framing, chemical-resistant structural components, tank construction',
        'default': 'chemical, laboratory, plumbing, electrical, industrial fabrication',
    },
    'polypropylene': {
        'sheet': 'chemical tanks, food processing equipment, living hinges, packaging, containers, laboratory',
        'default': 'chemical, food processing, living hinges, packaging, laboratory, sterilizable equipment',
    },
    'peek': {
        'sheet': 'aerospace components, medical devices, semiconductor equipment, high-temperature structural parts',
        'rod': 'precision machined aerospace and medical components, bushings, seals, surgical instruments',
        'default': 'aerospace, medical, semiconductor, high temperature, precision machining, metal replacement',
    },
    'delrin': {
        'sheet': 'precision machined parts, wear pads, structural panels, gears, cams, valve components',
        'rod': 'gears, pulleys, bearings, bushings, precision CNC machined components, rollers',
        'default': 'precision machining, gears, bearings, food processing, industrial, conveyor',
    },
    'ptfe': {
        'sheet': 'release liners, chemical-resistant linings, electrical insulation, gaskets, seals, laboratory surfaces',
        'rod': 'seals, bushings, chemical process components, electrical insulators, valve seats',
        'tube': 'chemical tubing, electrical insulation sleeves, high-temperature applications, laboratory',
        'default': 'chemical, electrical insulation, high temperature, non-stick, medical, seals and gaskets',
    },
    'polyethylene': {
        'sheet': 'chemical tanks, containers, liners, packaging, general fabrication, outdoor applications',
        'rod': 'chemical process components, bushings, general wear parts',
        'default': 'chemical storage, packaging, general fabrication, outdoor use',
    },
    'noryl': {
        'sheet': 'electrical panels, enclosures, precision components, automotive parts, medical devices',
        'rod': 'electrical connectors, precision machined components, structural parts',
        'default': 'electrical insulation, precision components, automotive, medical, dimensional stability applications',
    },
    'petg': {
        'sheet': 'retail displays, point-of-purchase fixtures, protective guards, medical packaging, thermoforming',
        'rod': 'display hardware, custom fabrication, prototyping',
        'tube': 'display cases, protective covers, medical packaging',
        'default': 'retail displays, medical packaging, protective guards, thermoforming',
    },
    'polystyrene': {
        'sheet': 'display cases, signage, laboratory trays, model making, light-duty enclosures',
        'rod': 'model making, display hardware, light-duty components',
        'default': 'displays, laboratory, model making, light-duty fabrication',
    },
    'polyurethane': {
        'sheet': 'wear pads, forming dies, vibration isolation, bumpers, squeegees',
        'rod': 'wheels, rollers, bushings, wear components, custom formed parts',
        'default': 'wear parts, wheels, rollers, vibration isolation, forming dies, seals',
    },
    'torlon': {
        'sheet': 'aerospace structural components, semiconductor fixtures, high-temperature wear parts',
        'rod': 'bearing cages, thrust washers, valve seats, precision machined aerospace components',
        'default': 'aerospace, semiconductor, oil and gas, extreme temperature and wear applications',
    },
    'ultem': {
        'sheet': 'aerospace interior panels, electrical insulation, medical trays, semiconductor fixtures',
        'rod': 'medical instruments, aerospace components, electrical connectors, precision machined parts',
        'default': 'aerospace, medical, electronics, flame-critical applications',
    },
    'vespel': {
        'sheet': 'semiconductor process components, aerospace structural parts, cryogenic applications',
        'rod': 'thrust washers, bearing components, precision aerospace and semiconductor parts',
        'default': 'aerospace, semiconductor, cryogenic, extreme temperature precision applications',
    },
    'phenolic': {
        'sheet': 'electrical panels, circuit board substrates, structural components, terminal boards',
        'rod': 'gears, bearings, bushings, structural components, electrical insulators',
        'default': 'electrical insulation, gears, bearings, structural, high-temperature applications',
    },
    'hydlar_z': {
        'sheet': 'wear pads, bearing plates, structural panels, high-wear industrial components',
        'rod': 'bearings, bushings, thrust washers, gears, precision wear components',
        'default': 'bearings, bushings, wear parts, aerospace, defense, clean-room applications',
    },
    'vinyl': {
        'sheet': 'protective covers, curtains, signage, upholstery, industrial barriers, equipment covers',
        'default': 'protective covers, curtains, barriers, outdoor applications, flexible fabrication',
    },
}

# ============================================================
# MATERIAL COMPOSITION
# ============================================================

MATERIAL_COMPOSITION = {
    'acrylic': '100% Acrylic (PMMA)',
    'abs': '100% ABS (Acrylonitrile Butadiene Styrene)',
    'hdpe': '100% HDPE (High Density Polyethylene)',
    'polycarbonate': '100% Polycarbonate (PC)',
    'nylon': '100% Nylon (Polyamide)',
    'uhmw': '100% UHMW Polyethylene',
    'pvc': '100% PVC (Polyvinyl Chloride)',
    'polypropylene': '100% Polypropylene (PP)',
    'peek': '100% PEEK (Polyether Ether Ketone)',
    'delrin': '100% Acetal/Delrin (POM)',
    'ptfe': '100% PTFE (Polytetrafluoroethylene)',
    'polyethylene': '100% Polyethylene (PE)',
    'noryl': '100% Noryl (Modified PPO/PPE)',
    'petg': '100% PETG (Polyethylene Terephthalate Glycol)',
    'polystyrene': '100% Polystyrene (PS)',
    'polyurethane': '100% Polyurethane (PU)',
    'torlon': '100% Torlon (Polyamide-Imide / PAI)',
    'ultem': '100% Ultem (Polyetherimide / PEI)',
    'vespel': '100% Vespel (Polyimide / PI)',
    'phenolic': '100% Phenolic (Thermoset Composite)',
    'hydlar_z': 'Nylon reinforced with Kevlar fiber (Hydlar Z)',
    'vinyl': '100% Flexible PVC (Vinyl)',
}

# ============================================================
# SPANISH BACKEND TERMS — Per Material
# Feeds Spanish-speaking buyers in the US market
# ============================================================

SPANISH_TERMS = {
    'acrylic': 'lamina acrilica varilla acrilica tubo acrilico plastico acrilico plexiglass hoja acrilica',
    'abs': 'lamina abs plastico abs hoja abs varilla abs',
    'hdpe': 'polietileno alta densidad tabla de cortar plastico hdpe lamina hdpe',
    'polycarbonate': 'policarbonato lamina policarbonato plastico irrompible vidrio plastico',
    'nylon': 'nylon plastico varilla nylon lamina nylon engranaje plastico',
    'uhmw': 'polietileno uhmw plastico desgaste lamina uhmw',
    'pvc': 'pvc cloruro de polivinilo lamina pvc varilla pvc tubo pvc angulo pvc',
    'polypropylene': 'polipropileno lamina polipropileno plastico pp',
    'peek': 'plastico peek peek lamina peek varilla peek alta temperatura',
    'delrin': 'delrin acetal pom plastico delrin varilla delrin engranaje plastico',
    'ptfe': 'teflon ptfe plastico antiadherente lamina ptfe varilla ptfe',
}

# ============================================================
# HELPER — Get Rufus Q&A Block For Description
# ============================================================

def get_rufus_qa_block(material_key, ptype):
    # Returns a formatted Q&A block for the description.
    # Uses material-specific questions most relevant to the product type.
    qa_list = RUFUS_QA.get(material_key, [])
    if not qa_list:
        return ''

    # Pick best 3 questions — prioritize ones relevant to this product type
    selected = qa_list[:3]

    lines = ['COMMON QUESTIONS:']
    for qa in selected:
        lines.append('Q: ' + qa['q'])
        lines.append('A: ' + qa['a'])
        lines.append('')

    return '\n'.join(lines).strip()

def get_recommended_uses(material_key, ptype):
    """Get recommended uses for this material and shape combination."""
    mat_uses = RECOMMENDED_USES.get(material_key, {})
    return mat_uses.get(ptype) or mat_uses.get('default') or ''

def get_spanish_terms(material_key):
    """Get Spanish backend search terms for this material."""
    return SPANISH_TERMS.get(material_key, '')

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
            'Can I cut this with my tools at home?',
            'Will this yellow or crack outdoors?',
        ],
        'intended_use_by_shape': {
            'sheet': 'signage, display cases, window replacement, laser cutting, picture frames, sneeze guards, retail displays',
            'rod': 'standoffs, display hardware, light pipes, decorative accents, craft projects',
            'tube': 'display cases, light covers, fluid handling, aquarium plumbing, scientific apparatus',
            'sphere': 'decorative displays, craft projects, photography props, educational models',
            'half_sphere': 'display domes, decorative accents, craft projects, photography props',
            'cube': 'display blocks, paperweights, decorative accents, photography props',
            'default': 'fabrication, display, signage, laser cutting, CNC machining',
        },
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
            'Can I paint or bond this easily?',
            'Will this hold up in heat or outdoor use?',
        ],
        'intended_use_by_shape': {
            'sheet': 'enclosures, machine guards, automotive trim, vacuum forming, prototyping, panels',
            'rod': 'standoffs, spacers, handles, prototyping, model making',
            'tube': 'protective sleeves, conduit, structural tubes, model making',
            'default': 'enclosures, prototyping, automotive, fabrication, vacuum forming',
        },
    },
    'hdpe': {
        'tensile_psi': 4500,
        'impact_strength': 'No break (notched)',
        'heat_deflection': '180°F (82°C)',
        'service_temp': '-220°F to 180°F',
        'key_stat': 'FDA and NSF compliant — safe for direct food contact',
        'compliance': ['FDA 21 CFR', 'NSF Compliant', 'ASTM D4976', 'USDA Approved'],
        'pain_points': [
            'Is this actually food safe and NSF compliant?',
            'Will this warp, absorb odors, or stain?',
            'Can I use this in outdoor or marine environments?',
        ],
        'intended_use_by_shape': {
            'sheet': 'cutting boards, food prep surfaces, marine applications, chemical tanks, wear pads',
            'cutting_board': 'commercial food prep, restaurant kitchens, catering, food service',
            'cutting_board_oem': 'OEM replacement, commercial kitchen equipment, food service',
            'rod': 'bushings, wear parts, food processing equipment, marine hardware',
            'default': 'food service, marine, chemical, outdoor, wear applications',
        },
    },
    'polycarbonate': {
        'tensile_psi': 9500,
        'impact_strength': '16.0 ft-lb/in — 250x stronger than glass',
        'heat_deflection': '270°F (132°C)',
        'service_temp': '-40°F to 280°F',
        'key_stat': '250x stronger than glass — virtually unbreakable',
        'compliance': ['ASTM D3935', 'UL 94 V-2 Flame Rated'],
        'pain_points': [
            'Is this actually shatterproof for my machine guard?',
            'Will this hold up in extreme temperatures?',
            'Is this UV stable for outdoor or greenhouse use?',
        ],
        'intended_use_by_shape': {
            'sheet': 'machine guards, safety shields, greenhouse panels, skylights, bulletresistant barriers',
            'rod': 'structural components, handles, custom machined parts',
            'tube': 'protective covers, structural tubes, light covers',
            'default': 'machine guards, safety, greenhouse, construction, industrial',
        },
    },
    'nylon': {
        'tensile_psi': 12000,
        'impact_strength': '1.2 ft-lb/in notched',
        'heat_deflection': '400°F (204°C)',
        'service_temp': '-40°F to 250°F continuous',
        'key_stat': '12,000 PSI tensile strength with self-lubricating properties',
        'compliance': ['ASTM D4066', 'FDA Compliant grades available'],
        'pain_points': [
            'Will this machine cleanly on my lathe or mill?',
            'Does nylon absorb moisture and swell?',
            'Is this strong enough for my gear or bearing application?',
        ],
        'intended_use_by_shape': {
            'sheet': 'wear pads, slide pads, structural panels, bearing plates, food processing equipment',
            'rod': 'gears, bushings, bearings, pulleys, wear parts, precision machined components',
            'tube': 'bushings, sleeves, wear liners, food processing tubing',
            'default': 'machining, gears, bearings, wear parts, food processing, industrial',
        },
    },
    'uhmw': {
        'tensile_psi': 6500,
        'impact_strength': 'No break — highest impact of any thermoplastic',
        'heat_deflection': '180°F (82°C)',
        'service_temp': '-300°F to 180°F',
        'key_stat': 'Lowest friction coefficient of any plastic — superior wear resistance',
        'compliance': ['FDA 21 CFR', 'NSF Compliant', 'USDA Approved', 'ASTM D4020'],
        'pain_points': [
            'How long will this last in my high-wear application?',
            'Is this food safe for my conveyor or food processing line?',
            'Can I weld or bond UHMW to other surfaces?',
        ],
        'intended_use_by_shape': {
            'sheet': 'wear strips, conveyor liners, chute liners, truck bed liners, cutting boards, star wheels',
            'rod': 'wear parts, bushings, rollers, food processing components',
            'default': 'wear, conveyor, food processing, industrial lining applications',
        },
    },
    'pvc': {
        'tensile_psi': 7500,
        'impact_strength': '0.9 ft-lb/in notched',
        'heat_deflection': '158°F (70°C)',
        'service_temp': '32°F to 140°F',
        'key_stat': 'Resistant to 90%+ of acids, bases, and salts',
        'compliance': ['ASTM D1784', 'UL 94 V-0 Flame Rated'],
        'pain_points': [
            'Will this hold up to my specific chemicals?',
            'Is this flame retardant for my application?',
            'Can I solvent-cement or weld PVC easily?',
        ],
        'intended_use_by_shape': {
            'sheet': 'chemical tanks, fume hoods, laboratory equipment, signage, electrical panels',
            'rod': 'chemical process components, laboratory fixtures, custom machined parts',
            'tube': 'chemical tubing, laboratory equipment, plumbing, electrical conduit',
            'angle_bar': 'trim, framing, chemical-resistant structural components, tank construction',
            'default': 'chemical, laboratory, plumbing, electrical, industrial fabrication',
        },
    },
    'polypropylene': {
        'tensile_psi': 4800,
        'impact_strength': '1.2 ft-lb/in notched',
        'heat_deflection': '200°F (93°C)',
        'service_temp': '-20°F to 200°F',
        'key_stat': 'Unique living hinge capability — flexes millions of times without fatigue',
        'compliance': ['FDA 21 CFR', 'NSF Compliant', 'ASTM D4101'],
        'pain_points': [
            'Is this food safe and FDA compliant?',
            'Can this be used for living hinges?',
            'How chemical resistant is polypropylene?',
        ],
        'intended_use_by_shape': {
            'sheet': 'chemical tanks, food processing equipment, living hinges, packaging, containers',
            'default': 'chemical, food processing, living hinges, packaging, laboratory',
        },
    },
    'peek': {
        'tensile_psi': 14500,
        'impact_strength': '1.6 ft-lb/in notched',
        'heat_deflection': '600°F (316°C)',
        'service_temp': 'Continuous 480°F (250°C)',
        'key_stat': 'Continuous service at 480°F — one of the highest performance thermoplastics available',
        'compliance': ['ASTM D6262', 'USP Class VI', 'FDA Compliant', 'RoHS Compliant'],
        'pain_points': [
            'Will this hold up at continuous high temperatures?',
            'Is this biocompatible for my medical device application?',
            'Can PEEK replace metal in my application?',
        ],
        'intended_use_by_shape': {
            'sheet': 'aerospace components, medical devices, semiconductor equipment, high-temp structural parts',
            'rod': 'precision machined aerospace and medical components, bushings, seals',
            'default': 'aerospace, medical, semiconductor, high temperature, precision machining',
        },
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
            'How does Delrin compare to nylon for gears?',
            'Is Delrin self-lubricating for my bearing application?',
        ],
        'intended_use_by_shape': {
            'sheet': 'precision machined parts, wear pads, structural panels, gears, cams',
            'rod': 'gears, pulleys, bearings, bushings, precision CNC machined components',
            'default': 'precision machining, gears, bearings, food processing, industrial',
        },
    },
    'ptfe': {
        'tensile_psi': 3500,
        'impact_strength': '3.0 ft-lb/in notched',
        'heat_deflection': 'Continuous 500°F (260°C)',
        'service_temp': '-328°F to 500°F',
        'key_stat': 'Lowest friction coefficient of any solid material — near-zero stick',
        'compliance': ['FDA 21 CFR', 'USP Class VI', 'RoHS Compliant', 'ASTM D3294'],
        'pain_points': [
            'Is this truly non-stick for my release application?',
            'Will this hold up to my aggressive chemical environment?',
            'Can PTFE be used continuously at high temperatures?',
        ],
        'intended_use_by_shape': {
            'sheet': 'release liners, chemical-resistant linings, electrical insulation, gaskets, seals',
            'rod': 'seals, bushings, chemical process components, electrical insulators',
            'tube': 'chemical tubing, electrical insulation sleeves, high-temperature applications',
            'default': 'chemical, electrical insulation, high temperature, non-stick, medical',
        },
    },
}

# ============================================================
# MATERIAL DENSITIES (lb/in³)
# ============================================================

DENSITIES = {
    'acrylic': 0.0433, 'pmma': 0.0433,
    'abs': 0.0385,
    'hdpe': 0.0341,  # Updated: industry range 0.0340-0.0350 lb/in³
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
# MM TO INCHES CONVERSION
# ============================================================

MM_PER_INCH = 25.4

def convert_mm_to_inches(value_mm):
    """Convert millimeters to inches."""
    return round(value_mm / MM_PER_INCH, 4)

def normalize_mm_in_title(title):
    """
    Find all MM measurements in a title and convert to inches.
    Handles formats like: 3 MM, 3mm, 3MM, (3mm), 3 mm thick
    Returns (normalized_title, mm_found, conversions_made)
    """
    conversions = []
    normalized = title

    # Pattern: number followed by mm (with optional space)
    mm_pattern = re.compile(
        r'(\d+(?:\.\d+)?)\s*[Mm][Mm]',
        re.IGNORECASE
    )

    def replace_mm(match):
        mm_val = float(match.group(1))
        inches = convert_mm_to_inches(mm_val)
        conversions.append((mm_val, inches))
        # Return the inch value — the title formatter will add fraction/decimal format
        return f'{inches}"'

    normalized = mm_pattern.sub(replace_mm, normalized)
    return normalized, len(conversions) > 0, conversions

# ============================================================
# FRACTION CONVERSION
# ============================================================

def decimal_to_fraction(decimal):
    if decimal is None: return None
    if decimal == int(decimal): return str(int(decimal))
    sixteenths = round(decimal * 16)
    def gcd(a, b):
        while b: a, b = b, a % b
        return a
    whole = sixteenths // 16
    remainder = sixteenths % 16
    if remainder == 0: return str(whole)
    g = gcd(remainder, 16)
    num = remainder // g
    den = 16 // g
    if whole > 0: return f'{whole}-{num}/{den}'
    return f'{num}/{den}'

def format_dim(decimal):
    """Format dimension as Fraction" (Decimal") or whole number"."""
    if decimal is None: return None
    if decimal == int(decimal): return f'{int(decimal)}"'
    frac = decimal_to_fraction(decimal)
    dec_str = f'{decimal:.4g}'
    return f'{frac}" ({dec_str}")'

def dims_equal(val1, val2, tol=0.06):
    """
    Check if two dimension values are equal within tolerance.
    Handles fraction/decimal equivalents like 0.5 and 1/2.
    Tolerance of 0.06 covers 1/16th inch rounding.
    """
    if val1 is None or val2 is None: return False
    return abs(val1 - val2) <= tol

# ============================================================
# PRODUCT TYPE DETECTION
# ============================================================

PRODUCT_TYPE_KEYWORDS = {
    'cutting_board_oem': ['cutting board', 'oem', 'replacement'],
    'cutting_board': ['cutting board'],
    'square_tube': ['square tube', 'sq tube', 'square tubing'],
    'square_rod': ['square rod', 'sq rod', 'square bar', 'square stock'],
    'triangular_tube': ['triangular tube', 'triangle tube', 'tri tube'],
    'triangular_rod': ['triangular rod', 'triangle rod', 'tri rod', 'triangular bar'],
    'angle_bar': ['angle bar', 'angle stock', 'right angle', 'l-angle', 'l angle', 'angle shape'],
    'tube': ['tube', 'tubing'],
    'rod': ['rod', 'bar stock', 'round bar', 'dowel'],
    'half_sphere': ['half sphere', 'half-sphere', 'hemisphere', 'half ball', 'dome'],
    'sphere': ['sphere', 'ball', 'plexiglass ball', 'acrylic ball'],
    'cube': ['cube', 'block', 'plexiglass block', 'acrylic block'],
    'sheet': ['sheet', 'panel', 'board', 'plate'],
    'adhesive': ['weld-on', 'weldon', 'cement', 'adhesive', 'solvent', 'glue'],
    'accessory': [],  # fallback
}

def detect_product_type(title):
    """Detect product type from title using keyword matching."""
    title_lower = title.lower()

    # Check cutting board OEM first (most specific)
    if 'cutting board' in title_lower and (
        'oem' in title_lower or 'replacement' in title_lower or
        'continental' in title_lower or 'model' in title_lower
    ):
        return 'cutting_board_oem'

    # Check all other types in priority order
    for ptype, keywords in PRODUCT_TYPE_KEYWORDS.items():
        if ptype in ('cutting_board_oem', 'accessory'): continue
        for kw in keywords:
            if kw in title_lower:
                return ptype

    return 'accessory'

# ============================================================
# THIRD PARTY BRAND DETECTION
# ============================================================

def detect_brand(title):
    """
    Detect if this is a third party brand product.
    Returns (is_third_party, brand_name, weld_on_number)
    """
    title_lower = title.lower()

    for key, brand in THIRD_PARTY_BRANDS.items():
        if key in title_lower:
            # Check for Weld-On number
            weld_on_num = None
            if 'weld' in key:
                num_match = re.search(r'(?:weld[- ]?on\s*#?\s*)(\d+)', title_lower)
                if num_match:
                    weld_on_num = num_match.group(1)
            return True, brand, weld_on_num

    return False, None, None

# ============================================================
# COLOR AND MODIFIER DETECTION
# ============================================================

def detect_color_and_modifiers(title):
    """
    Detect color, optical modifiers (transparent/translucent/opaque),
    cast/extruded, solid/hollow/tumbled descriptors.
    """
    title_lower = title.lower()

    # Optical modifiers
    modifiers = []
    for mod in ['transparent', 'translucent', 'opaque']:
        if mod in title_lower:
            modifiers.append(mod.capitalize())

    # Cast/Extruded (only relevant for Acrylic and Nylon)
    cast_ext = None
    if 'cast' in title_lower: cast_ext = 'Cast'
    elif 'extruded' in title_lower: cast_ext = 'Extruded'

    # Solid/Hollow/Tumbled
    solid_hollow = None
    if 'solid' in title_lower: solid_hollow = 'Solid'
    elif 'hollow' in title_lower: solid_hollow = 'Hollow'
    if 'tumbled' in title_lower:
        solid_hollow = (solid_hollow + ' Tumbled') if solid_hollow else 'Tumbled'

    # Color mapping — strip color codes like #2422
    color_map = {
        'red': 'Red', 'orange': 'Orange', 'yellow': 'Yellow',
        'green': 'Green', 'blue': 'Blue', 'indigo': 'Indigo',
        'violet': 'Violet', 'purple': 'Purple', 'black': 'Black',
        'white': 'White', 'clear': 'Clear', 'natural': 'Natural',
        'brown': 'Brown', 'gray': 'Gray', 'grey': 'Gray',
        'pink': 'Pink', 'bronze': 'Bronze', 'amber': 'Amber',
        'smoke': 'Smoke', 'woodgrain': 'Woodgrain',
    }

    color = None
    # Strip color codes first (e.g. #2422 Red → Red)
    clean_title = re.sub(r'#\w+\s+', '', title_lower)
    # Also strip parenthetical codes like (2423)
    clean_title = re.sub(r'\(\d{4}\)', '', clean_title)

    for key, val in color_map.items():
        if re.search(rf'\b{key}\b', clean_title):
            color = val
            break

    return modifiers, cast_ext, solid_hollow, color

def detect_pack(title):
    """
    Extract pack quantity from title.
    Guards against false positives on color codes like (2422)
    and model numbers. Only accepts values 2-100 as valid packs.
    """
    # Explicit "Pack of N" is always reliable
    m = re.search(r'pack\s+of\s+(\d+)', title, re.IGNORECASE)
    if m: return int(m.group(1))

    # Trailing (N) — only accept if N is a realistic pack size (2-100)
    # This prevents color codes like (2422) being treated as Pack of 2422
    m = re.search(r'\((\d+)\)\s*$', title)
    if m:
        val = int(m.group(1))
        if 2 <= val <= 100:
            return val
    return None

# ============================================================
# DIMENSION PARSING
# ============================================================

def parse_fraction(text):
    """Parse fraction strings like 1/4, 1-1/2, 0.25 etc."""
    text = text.strip()
    # Remove trailing inch mark if present
    text = text.rstrip('"').strip()
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
    """
    Extract dimensions from title string.
    Handles inches (fractions and decimals) and MM (auto-converted).
    Returns dict with product_type and relevant dimensions.
    """
    # First normalize any MM measurements to inches
    title_normalized, had_mm, conversions = normalize_mm_in_title(title)

    ptype = detect_product_type(title)
    dims = {'product_type': ptype, 'had_mm': had_mm}

    if had_mm:
        dims['mm_conversions'] = conversions

    # num pattern handles: 1/4, 1-1/2, 0.25, .236, 12, 3/16
    num = r'(\d*\.?\d+(?:-\d+/\d+|\.\d+|/\d+)?)'

    try:
        if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
            # Thickness — handles: 1/4" Thick, 1/4" T, 0.25 Thick
            thick = re.search(
                rf'{num}["\s]*(?:thick(?:ness)?|t\b)',
                title_normalized, re.IGNORECASE
            )

            # Handle L/W prefix format: L11.8125" x W72" or W12" x L24"
            lw_prefix = re.search(
                rf'[LW]({num.strip("()")})["\s]*[xX×]\s*[LW]({num.strip("()")})',
                title_normalized, re.IGNORECASE
            )

            # Width x Length — handles:
            # 12" W x 24" L, 12" Wide x 24" Long, 12" x 24"
            wl = re.search(
                rf'{num}["\s]*(?:w(?:ide|idth)?\b)?\s*[xX×]\s*{num}["\s]*(?:l(?:ong|ength)?\b)?',
                title_normalized, re.IGNORECASE
            )
            # Three dimensions: Thick x Wide x Long
            twl = re.search(
                rf'{num}["\s]*[xX×]\s*{num}["\s]*[xX×]\s*{num}',
                title_normalized, re.IGNORECASE
            )

            if thick:
                val = parse_fraction(thick.group(1))
                if val and 0 < val < 20:
                    dims['thickness'] = round(val, 4)

            if lw_prefix:
                # L11.8125" x W72" format — figure out which is length vs width
                v1 = parse_fraction(lw_prefix.group(1))
                v2 = parse_fraction(lw_prefix.group(2))
                if v1 and v2 and 0 < v1 < 300 and 0 < v2 < 300:
                    # Larger = length, smaller = width
                    dims['width'] = round(min(v1, v2), 4)
                    dims['length'] = round(max(v1, v2), 4)
                # If no explicit thickness found, first number might be thickness
                # in format: 1/4" x 12" x 24" — first is thickness
                pass

            if twl:
                # Three dimension format: thickness x width x length
                v1 = parse_fraction(twl.group(1))
                v2 = parse_fraction(twl.group(2))
                v3 = parse_fraction(twl.group(3))
                if v1 and v2 and v3:
                    # Smallest is thickness, other two are width/length
                    vals = sorted([
                        round(v1, 4), round(v2, 4), round(v3, 4)
                    ])
                    if 0 < vals[0] < 20 and 0 < vals[1] < 200 and 0 < vals[2] < 200:
                        dims['thickness'] = vals[0]
                        dims['width'] = vals[1]
                        dims['length'] = vals[2]
            elif wl:
                w = parse_fraction(wl.group(1))
                l = parse_fraction(wl.group(2))
                if w and l:
                    if not dims.get('thickness') and w < 5:
                        # Likely thickness x width or thickness x length
                        pass
                    if 0 < w < 200 and 0 < l < 200:
                        dims['width'] = round(w, 4)
                        dims['length'] = round(l, 4)

            # If we have thickness and width/length from wl but no 3-dim match
            if dims.get('thickness') and not dims.get('width') and wl:
                w = parse_fraction(wl.group(1))
                l = parse_fraction(wl.group(2))
                if w and l and 0 < w < 200 and 0 < l < 200:
                    dims['width'] = round(w, 4)
                    dims['length'] = round(l, 4)

        elif ptype == 'rod':
            # Length — handles: 48" Length, 48" L, 48" long
            len_m = re.search(
                rf'{num}["\s]*(?:length|long|l\b)',
                title_normalized, re.IGNORECASE
            )
            # Diameter/OD — handles: 1/4" Diameter, 1/4" OD, 2-1/2" Thick
            # NOTE: In your catalog, rods use "Thick" to mean diameter/OD
            dia_m = re.search(
                rf'{num}["\s]*(?:diameter|dia(?:meter)?\b|od\b|thick(?:ness)?\b)',
                title_normalized, re.IGNORECASE
            )

            if len_m:
                val = parse_fraction(len_m.group(1))
                if val and 0 < val < 300: dims['length'] = round(val, 4)
            if dia_m:
                val = parse_fraction(dia_m.group(1))
                if val and 0 < val < 24: dims['od'] = round(val, 4)

            # Fallback: any two numbers where smaller = OD, larger = length
            if not dims.get('length') or not dims.get('od'):
                pat = re.search(
                    rf'{num}["\s]*(?:diameter|dia|od|thick)?\s*[xX×]\s*{num}',
                    title_normalized, re.IGNORECASE
                )
                if pat:
                    v1 = parse_fraction(pat.group(1))
                    v2 = parse_fraction(pat.group(2))
                    if v1 and v2:
                        if v1 < v2:
                            if 'od' not in dims and 0 < v1 < 24: dims['od'] = round(v1, 4)
                            if 'length' not in dims and 0 < v2 < 300: dims['length'] = round(v2, 4)
                        else:
                            if 'od' not in dims and 0 < v2 < 24: dims['od'] = round(v2, 4)
                            if 'length' not in dims and 0 < v1 < 300: dims['length'] = round(v1, 4)

            # Last resort — if only OD found, look for any larger number as length
            if dims.get('od') and not dims.get('length'):
                all_nums = re.findall(rf'{num}', title_normalized)
                for n_str in all_nums:
                    n = parse_fraction(n_str)
                    if n and n > dims.get('od', 0) and n < 300:
                        dims['length'] = round(n, 4)
                        break

        elif ptype == 'tube':
            # Length — also handles "72" Long" at START of title like "72" Long Acrylic Tube..."
            len_m = re.search(
                rf'{num}["\s]*(?:l\b|length|long)',
                title_normalized, re.IGNORECASE
            )
            # Also catch leading length like "72" Long Acrylic..."
            if not len_m:
                len_m = re.search(
                    rf'^[^0-9]*({num.strip("()")})["\s]*(?:long|length|l\b)',
                    title_normalized, re.IGNORECASE
                )
            id_m = re.search(rf'{num}["\s]*id', title_normalized, re.IGNORECASE)
            od_m = re.search(rf'{num}["\s]*od', title_normalized, re.IGNORECASE)
            if id_m:
                val = parse_fraction(id_m.group(1))
                if val and 0 < val < 24: dims['id'] = round(val, 4)
            if od_m:
                val = parse_fraction(od_m.group(1))
                if val and 0 < val < 24: dims['od'] = round(val, 4)
            if len_m:
                val = parse_fraction(len_m.group(1))
                if val and 0 < val < 300: dims['length'] = round(val, 4)

        elif ptype == 'square_rod':
            # Handles: 3/16" (0.1875") Square x 12" Length
            # Also handles: 1/2" x 1/2", 12" L
            size_m = re.search(
                rf'{num}["\s]*(?:\([^)]*\)["\s]*)?(?:square|sq\b)',
                title_normalized, re.IGNORECASE
            )
            if not size_m:
                size_m = re.search(rf'{num}["\s]*x\s*{num}["\s]', title_normalized, re.IGNORECASE)
            len_m = re.search(
                rf'{num}["\s]*(?:length|long|l\b)',
                title_normalized, re.IGNORECASE
            )
            if size_m:
                val = parse_fraction(size_m.group(1))
                if val and 0 < val < 24: dims['size'] = round(val, 4)
            if len_m:
                val = parse_fraction(len_m.group(1))
                if val and 0 < val < 300: dims['length'] = round(val, 4)
            # Fallback: if size found but no length, look for larger number
            if dims.get('size') and not dims.get('length'):
                all_nums = re.findall(rf'{num}', title_normalized)
                for n_str in all_nums:
                    n = parse_fraction(n_str)
                    if n and n > dims.get('size', 0) and n < 300:
                        dims['length'] = round(n, 4)
                        break

        elif ptype == 'square_tube':
            # OD x OD x wall, length
            od_m = re.search(rf'{num}["\s]*x\s*{num}', title_normalized, re.IGNORECASE)
            wall_m = re.search(rf'{num}["\s]*wall', title_normalized, re.IGNORECASE)
            len_m = re.search(rf'{num}["\s]*(?:l\b|length)', title_normalized, re.IGNORECASE)
            if od_m:
                val = parse_fraction(od_m.group(1))
                if val and 0 < val < 24: dims['od'] = round(val, 4)
            if wall_m:
                val = parse_fraction(wall_m.group(1))
                if val and 0 < val < 4: dims['wall'] = round(val, 4)
            if len_m:
                val = parse_fraction(len_m.group(1))
                if val and 0 < val < 300: dims['length'] = round(val, 4)

        elif ptype in ('triangular_rod', 'triangular_tube'):
            # side length, each side
            side_m = re.search(rf'{num}["\s]*(?:each\s*side|side|per\s*side)',
                               title_normalized, re.IGNORECASE)
            len_m = re.search(rf'{num}["\s]*(?:l\b|length)', title_normalized, re.IGNORECASE)
            wall_m = re.search(rf'{num}["\s]*wall', title_normalized, re.IGNORECASE)
            if side_m:
                val = parse_fraction(side_m.group(1))
                if val and 0 < val < 24: dims['side'] = round(val, 4)
            if len_m:
                val = parse_fraction(len_m.group(1))
                if val and 0 < val < 300: dims['length'] = round(val, 4)
            if wall_m and ptype == 'triangular_tube':
                val = parse_fraction(wall_m.group(1))
                if val and 0 < val < 4: dims['wall'] = round(val, 4)

        elif ptype == 'angle_bar':
            # W x H x wall, length
            wh_m = re.search(rf'{num}["\s]*x\s*{num}["\s]*x\s*{num}',
                             title_normalized, re.IGNORECASE)
            len_m = re.search(rf'{num}["\s]*(?:l\b|length)', title_normalized, re.IGNORECASE)
            if wh_m:
                w = parse_fraction(wh_m.group(1))
                h = parse_fraction(wh_m.group(2))
                wall = parse_fraction(wh_m.group(3))
                if w and 0 < w < 24: dims['width'] = round(w, 4)
                if h and 0 < h < 24: dims['height'] = round(h, 4)
                if wall and 0 < wall < 4: dims['wall'] = round(wall, 4)
            if len_m:
                val = parse_fraction(len_m.group(1))
                if val and 0 < val < 300: dims['length'] = round(val, 4)

        elif ptype in ('sphere', 'half_sphere'):
            # diameter
            dia_m = re.search(rf'{num}["\s]*(?:diameter|dia\b)',
                              title_normalized, re.IGNORECASE)
            # Also try: number inch sphere/ball
            if not dia_m:
                dia_m = re.search(rf'{num}["\s]*(?:sphere|ball|half)',
                                  title_normalized, re.IGNORECASE)
            if dia_m:
                val = parse_fraction(dia_m.group(1))
                if val and 0 < val < 24: dims['diameter'] = round(val, 4)
            # Wall for hollow half sphere
            wall_m = re.search(rf'{num}["\s]*wall', title_normalized, re.IGNORECASE)
            if wall_m and ptype == 'half_sphere':
                val = parse_fraction(wall_m.group(1))
                if val and 0 < val < 4: dims['wall'] = round(val, 4)

        elif ptype == 'cube':
            # Handles: 5/8" (Pack of N), 1-1/2", 1" x 1" x 1"
            # Pattern: trailing dimension after last dash or at end
            # Format: "Clear - 5/8\" (Pack of 50)" or "Clear - 1-1/2\""
            # First try explicit size
            size_m = re.search(
                rf'[-–]\s*({num.strip("()")})["\s]',
                title_normalized, re.IGNORECASE
            )
            if not size_m:
                # Try x x x format
                size_m = re.search(rf'{num}["\s]*x\s*{num}', title_normalized, re.IGNORECASE)
            if not size_m:
                # Last number in title before pack info
                size_m = re.search(
                    rf'({num.strip("()")})["\s]*(?:\(pack|\Z)',
                    title_normalized, re.IGNORECASE
                )
            if size_m:
                val = parse_fraction(size_m.group(1))
                if val and 0 < val < 24: dims['size'] = round(val, 4)

    except Exception as e:
        pass

    return dims

# ============================================================
# MATERIAL DETECTION
# ============================================================

def detect_material(title):
    """Detect material from title. Returns (material_key, material_name)."""
    title_lower = title.lower()
    materials = [
        # High-performance — check first as they contain common substrings
        ('vespel', 'Vespel'),
        ('torlon', 'Torlon'),
        ('hydlar', 'Hydlar Z'),
        ('ultem', 'Ultem'),
        ('peek', 'PEEK'),
        # Standard engineering plastics
        ('polycarbonate', 'Polycarbonate'),
        ('polystyrene', 'Polystyrene'),
        ('polypropylene', 'Polypropylene'),
        ('polyurethane', 'Polyurethane'),
        ('polyethylene', 'Polyethylene'),
        ('polypropene', 'Polypropylene'),
        ('uhmw', 'UHMW'),
        ('hdpe', 'HDPE'),
        ('petg', 'PETG'),
        ('noryl', 'Noryl'),
        ('phenolic', 'Phenolic'),
        ('acrylic', 'Acrylic'),
        ('plexiglass', 'Acrylic'),
        ('abs', 'ABS'),
        ('nylon', 'Nylon'),
        ('pvc', 'PVC'),
        ('vinyl', 'Vinyl'),
        ('delrin', 'Delrin'),
        ('acetal', 'Acetal'),
        ('ptfe', 'PTFE'),
        ('teflon', 'PTFE'),
    ]
    for key, name in materials:
        if key in title_lower:
            # Normalize aliases
            if key == 'plexiglass': return 'acrylic', name
            if key == 'polypropene': return 'polypropylene', name
            if key == 'teflon': return 'ptfe', name
            if key == 'acetal': return 'delrin', name
            if key == 'hydlar': return 'hydlar_z', name
            return key, name
    if 'cutting board' in title_lower:
        return 'hdpe', 'HDPE'
    return 'unknown', 'Unknown'

# ============================================================
# TITLE BUILDER
# ============================================================

def build_title(title, dims, material_key, material_name,
                modifiers, cast_ext, solid_hollow, color, pack,
                is_third_party, brand_name, weld_on_num):
    """
    Build the correctly formatted title for any product type.
    """
    ptype = dims.get('product_type', 'accessory')

    # Handle color normalization
    color_str = color or ''
    if material_key == 'hdpe' and color_str.lower() in ('natural', ''):
        color_str = 'White'
    elif material_key == 'nylon' and color_str.lower() == 'natural':
        color_str = 'Natural (White)'

    # Handle optical modifier prefix
    modifier_str = f'{" ".join(modifiers)} ' if modifiers else ''

    # Cast/Extruded only for Acrylic and Nylon
    cast_str = ''
    if cast_ext and material_name in ('Acrylic', 'Nylon'):
        cast_str = f'{cast_ext} '

    # Solid/Hollow/Tumbled prefix
    solid_str = f'{solid_hollow} ' if solid_hollow else ''

    # Pack suffix
    pack_str = f' (Pack of {pack})' if pack else ''

    # ---- WELD-ON / THIRD PARTY ADHESIVE ----
    if is_third_party and brand_name == 'Weld-On':
        dcm_str = ''
        if weld_on_num and weld_on_num in WELD_ON_DCM_FREE:
            dcm_str = ', DCM & TCE Free Formula'

        # Extract size from title
        size_str = ''
        for size in ['gallon', 'quart', 'pint', 'ounce', 'oz', 'liter']:
            if size in title.lower():
                size_str = f', {size.capitalize()}'
                break
        # Also check for oz amounts like "4 oz"
        oz_match = re.search(r'(\d+)\s*oz', title, re.IGNORECASE)
        if oz_match and not size_str:
            size_str = f', {oz_match.group(1)} oz'

        num_str = f' #{weld_on_num}' if weld_on_num else ''

        # Extract cement type
        cement_type = 'Cement'
        if 'acrylic' in title.lower(): cement_type = 'Acrylic Cement'
        elif 'pvc' in title.lower(): cement_type = 'PVC Cement'
        elif 'polycarbonate' in title.lower(): cement_type = 'Polycarbonate Cement'
        elif 'urethane' in title.lower(): cement_type = 'Urethane Adhesive'

        return f'Weld-On{num_str} {cement_type}{dcm_str}{size_str}{pack_str}'

    # ---- OTHER THIRD PARTY BRAND ----
    if is_third_party and brand_name:
        # Keep original title structure but clean it up
        return f'{brand_name} {title.split(brand_name, 1)[-1].strip()}'[:200]

    # ---- PLASTIC-CRAFT OWNED PRODUCTS ----
    prefix = 'Plastic-Craft | '

    if ptype == 'sheet':
        t = format_dim(dims.get('thickness'))
        w = format_dim(dims.get('width'))
        l = format_dim(dims.get('length'))
        dim_str = f', {t} x {w} W x {l} L' if all([t, w, l]) else ''
        return f'{prefix}{cast_str}{material_name} Sheet, {modifier_str}{color_str}{dim_str}{pack_str}'

    elif ptype == 'rod':
        od = format_dim(dims.get('od'))
        l = format_dim(dims.get('length'))
        dim_str = f', {od} OD x {l} L' if od and l else ''
        return f'{prefix}{cast_str}{material_name} Round Rod, {modifier_str}{color_str}{dim_str}{pack_str}'

    elif ptype == 'tube':
        id_ = format_dim(dims.get('id'))
        od = format_dim(dims.get('od'))
        l = format_dim(dims.get('length'))
        dim_str = f', {id_} ID x {od} OD x {l} L' if all([id_, od, l]) else ''
        return f'{prefix}{cast_str}{material_name} Round Tube, {modifier_str}{color_str}{dim_str}{pack_str}'

    elif ptype == 'square_rod':
        sz = format_dim(dims.get('size'))
        l = format_dim(dims.get('length'))
        dim_str = f', {sz} x {sz}, {l} L' if sz and l else ''
        return f'{prefix}{material_name} Square Rod, {color_str}{dim_str}{pack_str}'

    elif ptype == 'square_tube':
        od = format_dim(dims.get('od'))
        wall = format_dim(dims.get('wall'))
        l = format_dim(dims.get('length'))
        dim_str = f', {od} x {od} x {wall} Wall, {l} L' if all([od, wall, l]) else ''
        return f'{prefix}{material_name} Square Tube, {color_str}{dim_str}{pack_str}'

    elif ptype == 'triangular_rod':
        side = format_dim(dims.get('side'))
        l = format_dim(dims.get('length'))
        dim_str = f', {side} Each Side, {l} L' if side and l else ''
        return f'{prefix}{cast_str}{material_name} Triangular Rod, {modifier_str}{color_str}{dim_str}{pack_str}'

    elif ptype == 'triangular_tube':
        side = format_dim(dims.get('side'))
        wall = format_dim(dims.get('wall'))
        l = format_dim(dims.get('length'))
        dim_str = f', {side} x {wall} Wall, {l} L' if all([side, wall, l]) else ''
        return f'{prefix}{cast_str}{material_name} Triangular Tube, {modifier_str}{color_str}{dim_str}{pack_str}'

    elif ptype == 'angle_bar':
        w = format_dim(dims.get('width'))
        h = format_dim(dims.get('height'))
        wall = format_dim(dims.get('wall'))
        l = format_dim(dims.get('length'))
        dim_str = f', {w} x {h} x {wall} Wall, {l} L' if all([w, h, wall, l]) else ''
        return f'{prefix}{material_name} Angle Bar, {modifier_str}{color_str}{dim_str}{pack_str}'

    elif ptype == 'sphere':
        dia = format_dim(dims.get('diameter'))
        dim_str = f', {dia} OD' if dia else ''
        return f'{prefix}{solid_str}{cast_str}{material_name} Sphere, {modifier_str}{color_str}{dim_str}{pack_str}'

    elif ptype == 'half_sphere':
        dia = format_dim(dims.get('diameter'))
        wall = format_dim(dims.get('wall'))
        if solid_hollow and 'Hollow' in solid_hollow and wall:
            dim_str = f', {dia} OD x {wall} Wall' if dia else ''
        else:
            dim_str = f', {dia} OD' if dia else ''
        return f'{prefix}{solid_str}{cast_str}{material_name} Half Sphere, {modifier_str}{color_str}{dim_str}{pack_str}'

    elif ptype == 'cube':
        sz = format_dim(dims.get('size'))
        dim_str = f', {sz} x {sz} x {sz}' if sz else ''
        return f'{prefix}{solid_str}{cast_str}{material_name} Cube, {modifier_str}{color_str}{dim_str}{pack_str}'

    elif ptype == 'cutting_board':
        t = format_dim(dims.get('thickness'))
        w = format_dim(dims.get('width'))
        l = format_dim(dims.get('length'))
        # Always include thickness in cutting board title
        if all([t, w, l]):
            dim_str = f', {t} x {w} W x {l} L'
        elif all([w, l]):
            dim_str = f', {w} W x {l} L'
        else:
            dim_str = ''
        return f'{prefix}Plastic Cutting Board Sheet, Food Grade HDPE, {color_str}{dim_str}{pack_str}'

    elif ptype == 'cutting_board_oem':
        # Extract model number — look for alphanumeric codes after model/# keywords
        # Also catches codes like 5-316, KP26, etc.
        model_match = re.search(
            r'(?:model|#|no\.?|part)\s*:?\s*([A-Z0-9][A-Z0-9\-]+)',
            title, re.IGNORECASE
        )
        # Fallback — look for standalone model-like codes (letters+numbers)
        if not model_match:
            model_match = re.search(
                r'\b([A-Z]{1,3}[0-9]{2,}(?:-[A-Z0-9]+)?)\b',
                title, re.IGNORECASE
            )
        model_str = f', Model: {model_match.group(1).upper()}' if model_match else ''
        w = format_dim(dims.get('width'))
        l = format_dim(dims.get('length'))
        dim_str = f', {w} W x {l} L' if w and l else ''
        # Find equipment brand name — stop before 'Model', 'Inc', numbers etc.
        brand_match = re.search(
            r'(?:for|replacement for)\s+([A-Za-z]+(?:\s+[A-Za-z]+)??)(?:\s+(?:model|inc|corp|co\b|#|\d))',
            title, re.IGNORECASE
        )
        if not brand_match:
            brand_match = re.search(
                r'(?:for|replacement for)\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
                title, re.IGNORECASE
            )
        equip_brand = brand_match.group(1).strip().title() if brand_match else 'Commercial Equipment'
        return f'{prefix}Cutting Board OEM Replacement for {equip_brand}{dim_str}{model_str}'

    else:
        # Generic accessory
        clean = re.sub(r'plastic.craft\s*\|?\s*', '', title, flags=re.IGNORECASE).strip()
        return f'{prefix}{clean}'[:200]

# ============================================================
# WEIGHT CALCULATIONS — ALL SHAPES
# ============================================================

def calculate_weight(material_key, dims):
    """Calculate shipping weight for all product types."""
    density = DENSITIES.get(material_key)
    if not density: return None
    ptype = dims.get('product_type', '')
    volume = None

    try:
        if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
            # For OEM cutting boards, thickness is often not in the title
            # Use a standard commercial cutting board thickness (3/4") as default
            thickness = dims.get('thickness')
            if not thickness and ptype == 'cutting_board_oem':
                thickness = 0.75  # Standard commercial cutting board thickness
            if thickness and all(k in dims for k in ('width', 'length')):
                volume = thickness * dims['width'] * dims['length']
            elif all(k in dims for k in ('thickness', 'width', 'length')):
                volume = dims['thickness'] * dims['width'] * dims['length']

        elif ptype == 'rod':
            if all(k in dims for k in ('od', 'length')):
                r = dims['od'] / 2
                volume = math.pi * r**2 * dims['length']

        elif ptype == 'tube':
            if all(k in dims for k in ('id', 'od', 'length')):
                volume = math.pi * (
                    (dims['od']/2)**2 - (dims['id']/2)**2
                ) * dims['length']

        elif ptype == 'square_rod':
            if all(k in dims for k in ('size', 'length')):
                volume = dims['size']**2 * dims['length']

        elif ptype == 'square_tube':
            if all(k in dims for k in ('od', 'wall', 'length')):
                id_ = dims['od'] - 2 * dims['wall']
                volume = (dims['od']**2 - id_**2) * dims['length']

        elif ptype in ('triangular_rod', 'triangular_tube'):
            if 'side' in dims and 'length' in dims:
                # Equilateral triangle area = (sqrt(3)/4) * side^2
                tri_area = (math.sqrt(3) / 4) * dims['side']**2
                if ptype == 'triangular_rod':
                    volume = tri_area * dims['length']
                elif 'wall' in dims:
                    inner_side = dims['side'] - 2 * dims['wall']
                    inner_area = (math.sqrt(3) / 4) * inner_side**2
                    volume = (tri_area - inner_area) * dims['length']

        elif ptype == 'angle_bar':
            if all(k in dims for k in ('width', 'height', 'wall', 'length')):
                # L-shape cross section area
                area = (dims['width'] + dims['height'] - dims['wall']) * dims['wall']
                volume = area * dims['length']

        elif ptype == 'sphere':
            if 'diameter' in dims:
                r = dims['diameter'] / 2
                volume = (4/3) * math.pi * r**3

        elif ptype == 'half_sphere':
            if 'diameter' in dims:
                r = dims['diameter'] / 2
                if dims.get('wall'):
                    # Hollow half sphere
                    r_inner = r - dims['wall']
                    volume = (2/3) * math.pi * (r**3 - r_inner**3)
                else:
                    # Solid half sphere
                    volume = (2/3) * math.pi * r**3

        elif ptype == 'cube':
            if 'size' in dims:
                volume = dims['size']**3

    except Exception:
        return None

    if volume and volume > 0:
        raw_weight = volume * density * WEIGHT_BUFFER
        # Round to 4 decimal places for tiny parts, 2 for normal parts
        weight = round(raw_weight, 4) if raw_weight < 0.01 else round(raw_weight, 2)
        # Sanity: min 0.0001 lbs (tiny parts/spheres), max 500 lbs
        if 0.0001 < weight < 500:
            # Multiply by pack quantity if applicable
            # Amazon weight field should reflect total shipment weight
            pack = dims.get('pack', 1)
            if pack and pack > 1:
                weight = round(weight * pack, 2)
            return weight
    return None

def calculate_box_dims(dims):
    """Calculate box dimensions with 2 inch buffer for all product types."""
    ptype = dims.get('product_type', '')

    try:
        if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
            if all(k in dims for k in ('thickness', 'width', 'length')):
                return {
                    'box_length': round(dims['length'] + BOX_BUFFER, 1),
                    'box_width': round(dims['width'] + BOX_BUFFER, 1),
                    'box_height': round(dims['thickness'] + BOX_BUFFER, 1),
                }

        elif ptype in ('rod', 'square_rod', 'triangular_rod', 'angle_bar'):
            od = dims.get('od') or dims.get('size') or dims.get('side') or dims.get('width', 1)
            if 'length' in dims:
                return {
                    'box_length': round(dims['length'] + BOX_BUFFER, 1),
                    'box_width': round(od + BOX_BUFFER, 1),
                    'box_height': round(od + BOX_BUFFER, 1),
                }

        elif ptype in ('tube', 'square_tube', 'triangular_tube'):
            od = dims.get('od') or dims.get('side', 1)
            if 'length' in dims:
                return {
                    'box_length': round(dims['length'] + BOX_BUFFER, 1),
                    'box_width': round(od + BOX_BUFFER, 1),
                    'box_height': round(od + BOX_BUFFER, 1),
                }

        elif ptype in ('sphere', 'half_sphere'):
            d = dims.get('diameter', 1)
            return {
                'box_length': round(d + BOX_BUFFER, 1),
                'box_width': round(d + BOX_BUFFER, 1),
                'box_height': round(d + BOX_BUFFER, 1),
            }

        elif ptype == 'cube':
            s = dims.get('size', 1)
            return {
                'box_length': round(s + BOX_BUFFER, 1),
                'box_width': round(s + BOX_BUFFER, 1),
                'box_height': round(s + BOX_BUFFER, 1),
            }

    except Exception:
        pass
    return {}

# ============================================================
# DIM WEIGHT AND CHARGEABLE WEIGHT
# ============================================================

def calculate_dim_weight(box_dims):
    """
    Dimensional weight = L x W x H / 139
    Standard carrier divisor for ground/freight shipping.
    Carriers charge whichever is higher — actual or DIM weight.
    """
    l = box_dims.get('box_length', 0)
    w = box_dims.get('box_width', 0)
    h = box_dims.get('box_height', 0)
    if l and w and h:
        return round((l * w * h) / 139, 2)
    return None

def get_chargeable_weight(actual_weight, box_dims):
    """
    Returns (chargeable_weight, dim_weight, dim_wins).
    Chargeable = max(actual, DIM) — what carrier actually bills you.
    """
    dim = calculate_dim_weight(box_dims)
    if dim and actual_weight:
        chargeable = max(actual_weight, dim)
        return round(chargeable, 2), round(dim, 2), dim > actual_weight
    return actual_weight, None, False

def is_lightweight(box_dims, chargeable_weight):
    """
    Lightweight = under 1 cubic foot AND under 1 lb chargeable weight.
    1 cubic foot = 1728 cubic inches.
    """
    l = box_dims.get('box_length', 0)
    w = box_dims.get('box_width', 0)
    h = box_dims.get('box_height', 0)
    if l and w and h:
        cubic_inches = l * w * h
        cubic_feet = cubic_inches / 1728
        return cubic_feet < 1.0 and (chargeable_weight or 0) < 1.0
    return False

# ============================================================
# IMAGE AUDIT FLAG
# ============================================================


# ============================================================
# HTML IMAGE REVIEW WINDOW
# ============================================================


# ============================================================
# VALIDATION
# ============================================================

CERTIFICATION_CLOSING = "Plastic-Craft Products has been a trusted supplier"
CAPS_PATTERN = re.compile(r'^[A-Z][A-Z\s&/+\-,]{2,}—')

CERTIFICATION_PARAGRAPH = (
    'Plastic-Craft Products has been a trusted supplier of quality plastic '
    'materials since 1934. We are ISO 9001:2015 and AS9100D certified, ensuring '
    'rigorous quality management standards across our entire operation. All '
    'dimensions are held to +/- 0.010" tolerances. Whether you\'re a hobbyist, '
    'fabricator, engineer, or procurement professional — we have the materials '
    'and expertise to support your project.'
)

def validate_input(dims, material_key, title):
    issues = []
    ptype = dims.get('product_type', 'unknown')
    if ptype == 'unknown': issues.append('PRODUCT_TYPE_UNKNOWN')
    if material_key == 'unknown': issues.append('MATERIAL_UNKNOWN')
    if ptype == 'sheet':
        if 'thickness' not in dims: issues.append('MISSING_THICKNESS')
        if 'width' not in dims or 'length' not in dims: issues.append('MISSING_WIDTH_LENGTH')
    elif ptype == 'rod':
        if 'od' not in dims: issues.append('MISSING_OD')
        if 'length' not in dims: issues.append('MISSING_LENGTH')
    elif ptype == 'tube':
        if 'od' not in dims or 'id' not in dims: issues.append('MISSING_ID_OD')
        if 'length' not in dims: issues.append('MISSING_LENGTH')
    return issues

def validate_output(content, freight_needed):
    issues = []
    title = content.get('title', '')
    if not title: issues.append('TITLE_MISSING')
    elif len(title) > LIMITS['title']: issues.append(f'TITLE_TOO_LONG:{len(title)}')
    for i in range(1, 6):
        bullet = content.get(f'bullet{i}', '')
        if not bullet: issues.append(f'BULLET{i}_MISSING')
        elif len(bullet) > LIMITS['bullet']: issues.append(f'BULLET{i}_TOO_LONG:{len(bullet)}')
        if bullet and not CAPS_PATTERN.match(bullet): issues.append(f'BULLET{i}_NO_CAPS_LEAD')
    desc = content.get('description', '')
    if not desc: issues.append('DESCRIPTION_MISSING')
    elif len(desc) > LIMITS['description']: issues.append(f'DESCRIPTION_TOO_LONG:{len(desc)}')
    if desc and CERTIFICATION_CLOSING not in desc: issues.append('DESCRIPTION_MISSING_CERTIFICATION')
    if freight_needed and FREIGHT_BLURB[:30] not in desc: issues.append('FREIGHT_NOTICE_MISSING')
    backend = content.get('backend_search_terms', '')
    if not backend: issues.append('BACKEND_MISSING')
    elif len(backend.encode('utf-8')) > LIMITS['backend_search_terms']:
        issues.append(f'BACKEND_TOO_LONG:{len(backend.encode())}bytes')
    return issues

def auto_correct(content, issues, freight_needed):
    corrected = content.copy()
    fixes = []

    for issue in issues:
        if issue.startswith('TITLE_TOO_LONG'):
            t = corrected.get('title', '')
            while len(t) > LIMITS['title']:
                lc = t[:LIMITS['title']].rfind(',')
                t = t[:lc].strip() if lc > 100 else t[:LIMITS['title']].strip()
            corrected['title'] = t
            fixes.append('TITLE_TRIMMED')

        elif issue.startswith('BACKEND_TOO_LONG'):
            b = corrected.get('backend_search_terms', '')
            while len(b.encode('utf-8')) > LIMITS['backend_search_terms']:
                parts = b.rsplit(' ', 1)
                b = parts[0] if len(parts) > 1 else b
            corrected['backend_search_terms'] = b
            fixes.append('BACKEND_TRIMMED')

        elif issue in ('DESCRIPTION_MISSING_CERTIFICATION', 'FREIGHT_NOTICE_MISSING'):
            # Fix: calculate combined length of BOTH cert + freight together
            # before appending either, to prevent exceeding 2000 chars
            desc = corrected.get('description', '')
            cert_needed = CERTIFICATION_CLOSING not in desc
            freight_needed_flag = freight_needed and FREIGHT_BLURB[:30] not in desc

            # Calculate what we need to append
            append_parts = []
            if cert_needed:
                append_parts.append(CERTIFICATION_PARAGRAPH)
            if freight_needed_flag:
                append_parts.append(FREIGHT_BLURB)

            if not append_parts:
                continue

            append_text = '\n\n'.join(append_parts)
            combined = desc + '\n\n' + append_text

            if len(combined) <= LIMITS['description']:
                corrected['description'] = combined
            else:
                # Trim body to make room for everything we need to append
                available = LIMITS['description'] - len(append_text) - 4
                corrected['description'] = desc[:available].strip() + '\n\n' + append_text

            if cert_needed: fixes.append('CERTIFICATION_APPENDED')
            if freight_needed_flag: fixes.append('FREIGHT_NOTICE_APPENDED')

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
            fixes.append('DESCRIPTION_TRIMMED')

        elif re.match(r'BULLET(\d)_TOO_LONG', issue):
            num = re.match(r'BULLET(\d)_TOO_LONG', issue).group(1)
            key = f'bullet{num}'
            corrected[key] = corrected.get(key, '')[:LIMITS['bullet'] - 3].strip() + '...'
            fixes.append(f'BULLET{num}_TRIMMED')

    return corrected, fixes

# ============================================================
# CLAUDE API
# ============================================================

# anthropic client is initialized inside process_listings()
# after the API key check — see fix for issue #3
client = None  # placeholder, set in process_listings()

def build_system_prompt():
    return """You are an expert Amazon listing copywriter for Plastic-Craft Products,
founded 1934, ISO 9001:2015 and AS9100D certified, West Nyack NY.

TITLE: Already provided — use as-is, do not regenerate.

BULLETS — EVERY bullet MUST start with ALL CAPS benefit phrase + em dash (—):
- B1: Material properties with specific data (PSI, temp ratings, compliance)
- B2: Exact specs — dimensions, tolerances +/-0.010", pack quantity, weight
- B3: Applications — specific industries, projects, use cases (use provided intended_use)
- B4: Fabrication — tools, bonding, forming, machining methods
- B5: EXACTLY: "PRECISION TOLERANCES & QUALITY — Dimensions held to +/- 0.010" ensuring consistent, precise fit for fabrication and engineering applications. Manufactured under ISO 9001:2015 certified quality management standards for reliable material quality on every order."
- Each bullet under 500 characters
- Feature + Benefit + Proof with numbers and data
- Answer real buyer pain points
- NO description — bullets only this run

BACKEND TERMS:
- Under 249 bytes, space-separated, NO commas
- NO words from title, NO prohibited terms (best top guaranteed free sale discount cheap)
- Include alternate names, misspellings, applications
- Include "west nyack ny" "plastic supplier" "usa stocked"

Return ONLY valid JSON no markdown:
{"bullet1":"...","bullet2":"...","bullet3":"...","bullet4":"...","bullet5":"...","backend_search_terms":"...","material_type":"...","color":"...","size_description":"...","finish_type":"..."}"""

def generate_content(row, dims, material_key, material_name,
                     weight, box_dims, base_backend, freight_needed,
                     new_title, intended_use):
    global client
    original_title = row.get('item-name', '').strip()
    original_desc = clean_text(row.get('item-description', '').strip())
    modifiers, cast_ext, solid_hollow, color = detect_color_and_modifiers(original_title)
    pack = detect_pack(original_title)
    mat_data = MATERIAL_DATA.get(material_key, {})

    rufus_qa = get_rufus_qa_block(material_key, dims.get('product_type',''))
    rec_uses = get_recommended_uses(material_key, dims.get('product_type',''))
    spanish = get_spanish_terms(material_key)
    mat_comp = MATERIAL_COMPOSITION.get(material_key, '')

    context = f"""Pre-built title (use exactly): {new_title}
SKU: {row.get('seller-sku','')} | ASIN: {row.get('asin1','')}
Material: {material_name} | Key: {material_key}
Product type: {dims.get('product_type','unknown')}
Dimensions: {json.dumps({k:v for k,v in dims.items() if k not in ('product_type','had_mm','mm_conversions')})}
Color: {color} | Modifiers: {modifiers} | Pack: {pack}
Weight: {weight} lbs
Key stat: {mat_data.get('key_stat','N/A')}
Tensile strength: {mat_data.get('tensile_psi','N/A')} PSI
Service temp: {mat_data.get('service_temp','N/A')}
Compliance: {', '.join(mat_data.get('compliance',[]))}
Pain points: {' | '.join(mat_data.get('pain_points',[]))}
Intended use: {intended_use}
Base backend terms: {' '.join(base_backend[:8])}"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            system=build_system_prompt(),
            messages=[{"role": "user", "content": context}]
        )
        raw = response.content[0].text.strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        result = json.loads(raw)
        result['title'] = new_title
        return result
    except json.JSONDecodeError:
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group(0))
                result['title'] = new_title
                return result
            except: pass
        return None
    except Exception:
        return None

def retry_single_field(field_name, content, row, dims, material_key, material_name, new_title):
    global client
    prompts = {
        'description': f"""Write only the product description for this Amazon listing.
Product: {new_title}
Material: {material_name} | Type: {dims.get('product_type')}
Rules: Under 2000 chars, conversational, answer buyer questions, MUST end with exactly:
"Plastic-Craft Products has been a trusted supplier of quality plastic materials since 1934. We are ISO 9001:2015 and AS9100D certified, ensuring rigorous quality management standards across our entire operation. All dimensions are held to +/- 0.010" tolerances. Whether you're a hobbyist, fabricator, engineer, or procurement professional — we have the materials and expertise to support your project."
Return ONLY the description text.""",
        'backend_search_terms': f"""Write only Amazon backend search terms.
Product: {new_title}
Material: {material_name}
Rules: Under 249 bytes, space-separated, no words from title, no prohibited terms.
Return ONLY the space-separated terms.""",
    }
    bullet_focus = {
        '1': 'material properties with specific PSI and temperature data',
        '2': 'exact dimensions, tolerances +/-0.010", pack quantity',
        '3': 'applications, industries, specific use cases',
        '4': 'fabrication — tools, cutting, bonding, machining',
        '5': 'EXACTLY: "PRECISION TOLERANCES & QUALITY — Dimensions held to +/- 0.010" ensuring consistent, precise fit for fabrication and engineering applications. Manufactured under ISO 9001:2015 certified quality management standards for reliable material quality on every order."',
    }
    if field_name.startswith('bullet'):
        num = field_name[-1]
        prompts[field_name] = f"""Write only bullet {num} for this Amazon listing.
Product: {new_title} | Material: {material_name}
Focus: {bullet_focus.get(num,'product benefit')}
Rules: MUST start with ALL CAPS benefit phrase followed by em dash (—), under 500 chars.
Example: "IMPACT RESISTANT — ABS delivers..."
Return ONLY the bullet text."""

    prompt = prompts.get(field_name)
    if not prompt: return None
    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
    except: return None

# ============================================================
# CHECKPOINT
# ============================================================

def load_checkpoint():
    if Path(CHECKPOINT_FILE).exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                logger.info(f"↻ Checkpoint found: {len(data.get('completed',[]))} SKUs already done")
                return data
        except: pass
    return {'completed': [], 'run_id': RUN_ID}

def save_checkpoint(data):
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logger.warning(f"Could not save checkpoint: {e}")

def clear_checkpoint():
    if Path(CHECKPOINT_FILE).exists():
        os.remove(CHECKPOINT_FILE)

# ============================================================
# VIDEO FLAG
# ============================================================

def should_flag_video(material_key, ptype, weight):
    return (
        material_key in ('peek', 'ptfe', 'polycarbonate', 'delrin') or
        (weight and weight > 10) or
        ptype in ('cutting_board_oem', 'tube', 'sphere')
    )

# ============================================================
# ITEM TYPE KEYWORD MAP
# Amazon Browse Tree Guide validated values
# ============================================================

ITEM_TYPE_KEYWORDS = {
    'sheet':             'plastic-sheets',
    'rod':               'plastic-rods',
    'tube':              'plastic-tubes',
    'square_rod':        'plastic-rods',
    'square_tube':       'plastic-tubes',
    'triangular_rod':    'plastic-rods',
    'triangular_tube':   'plastic-tubes',
    'angle_bar':         'plastic-bars',
    'sphere':            'plastic-spheres',
    'half_sphere':       'plastic-spheres',
    'cube':              'plastic-blocks',
    'cutting_board':     'plastic-cutting-boards',
    'cutting_board_oem': 'plastic-cutting-boards',
    'adhesive':          'plastic-adhesives',
    'accessory':         'plastic-accessories',
}

# ============================================================
# STYLE MAP
# ============================================================

STYLE_MAP = {
    'sheet':             'Sheet',
    'rod':               'Round Rod',
    'tube':              'Round Tube',
    'square_rod':        'Square Rod',
    'square_tube':       'Square Tube',
    'triangular_rod':    'Triangular Rod',
    'triangular_tube':   'Triangular Tube',
    'angle_bar':         'Angle Bar',
    'sphere':            'Sphere',
    'half_sphere':       'Half Sphere',
    'cube':              'Cube',
    'cutting_board':     'Cutting Board',
    'cutting_board_oem': 'Cutting Board',
    'adhesive':          'Adhesive',
    'accessory':         'Accessory',
}

# ============================================================
# ITEM FORM MAP
# ============================================================

ITEM_FORM_MAP = {
    'sheet':             'Solid',
    'rod':               'Solid',
    'tube':              'Hollow',
    'square_rod':        'Solid',
    'square_tube':       'Hollow',
    'triangular_rod':    'Solid',
    'triangular_tube':   'Hollow',
    'angle_bar':         'Solid',
    'sphere':            'Solid',
    'half_sphere':       'Solid',  # hollow half spheres handled below
    'cube':              'Solid',
    'cutting_board':     'Solid',
    'cutting_board_oem': 'Solid',
    'adhesive':          'Liquid',
    'accessory':         'Solid',
}

def get_item_form(ptype, solid_hollow):
    """Get item form — handles hollow half spheres and square tubes."""
    if solid_hollow and 'Hollow' in solid_hollow:
        return 'Hollow'
    return ITEM_FORM_MAP.get(ptype, 'Solid')

def get_style(ptype, cast_ext, material_name):
    """
    Get style field — includes Cast/Extruded for Acrylic and Nylon only.
    Example: 'Cast Acrylic Sheet', 'Extruded Nylon Round Rod', 'ABS Sheet'
    """
    base_style = STYLE_MAP.get(ptype, 'Standard')
    # Cast/Extruded only applies to Acrylic and Nylon
    if cast_ext and material_name in ('Acrylic', 'Nylon'):
        return f'{cast_ext} {base_style}'
    return base_style

def get_item_type_keyword(ptype, cast_ext, material_name):
    """
    Get ITK — uses more specific cast/extruded values for Acrylic and Nylon
    where Amazon has specific browse nodes for these.
    """
    base_itk = ITEM_TYPE_KEYWORDS.get(ptype, 'plastic-raw-materials')

    # Amazon has specific ITKs for cast vs extruded acrylic
    if material_name == 'Acrylic' and cast_ext:
        itk_map = {
            ('sheet', 'Cast'):     'cast-acrylic-plastic-sheets',
            ('sheet', 'Extruded'): 'extruded-acrylic-plastic-sheets',
            ('rod',   'Cast'):     'cast-acrylic-plastic-rods',
            ('rod',   'Extruded'): 'extruded-acrylic-plastic-rods',
            ('tube',  'Cast'):     'cast-acrylic-plastic-tubes',
            ('tube',  'Extruded'): 'extruded-acrylic-plastic-tubes',
        }
        specific = itk_map.get((ptype, cast_ext))
        if specific:
            return specific

    return base_itk

# ============================================================
# SHIPPING TEMPLATE ASSIGNMENT
# ============================================================

def get_shipping_template(dims, box_dims, chargeable_weight, lightweight, is_third_party, brand_name):
    """
    Assign the correct shipping template based on box dimensions,
    chargeable weight (max of actual vs DIM), and product type.
    When a listing could fit two templates, always go with the LARGER one.
    """
    ptype = dims.get('product_type', '')
    bl = box_dims.get('box_length', 0)
    bw = box_dims.get('box_width', 0)
    bh = box_dims.get('box_height', 0)
    w = chargeable_weight or 0

    # NOVUS brand
    if brand_name and 'novus' in brand_name.lower():
        return 'NOVUS'

    # Hazmat — Weld-On adhesives
    if ptype == 'adhesive':
        # Lightweight Hazmat: 1.5 oz, 5 oz tubes and Quarter Pints only
        # Detect from dims — very small/light
        if w < 0.5:
            return 'Lightweight Hazmat'
        return 'Hazmat'

    # Lightweight — under 1 cu ft AND under 1 lb chargeable
    if lightweight:
        return 'Lightweight'

    # TUBES AND RODS — by box length, ties go larger
    if ptype in ('rod', 'tube', 'square_rod', 'square_tube',
                 'triangular_rod', 'triangular_tube', 'angle_bar'):
        if bl > 96:
            return 'Tube-Rod - Over 96'
        elif bl >= 48 or dims.get('od', 0) > 4 or dims.get('size', 0) > 4:
            return 'Tube-Rod - 48 to 95 or Diameter Over 4'
        elif bl >= 25:
            return 'Tube-Rod - 25 to 47'
        else:
            return 'Tube-Rod - Up to 24'

    # SHEETS — by box dimensions, OR logic, ties go larger
    if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
        long_side = max(bl, bw)
        short_side = min(bl, bw)

        # Large Sheet: long side > 48 AND short side > 36 (both must exceed)
        if long_side > 48 and short_side > 36:
            return 'Large Sheet - Larger than 48x36'

        # Mid Sheet: long side >= 48 OR short side >= 30 OR weight >= 20
        if long_side >= 48 or short_side >= 30 or w >= 20:
            return 'Mid Sheet - LS 48+ or SLS 30+, 20+ LBS SS 70+ LBS'

        # Small Sheet: long side < 48 OR short side < 30, weight 20-70
        if w >= 20:
            return 'Small Sheet - LS LT 48 or SLS LT 30, 20-70 LBS'

    # Spheres, cubes, half spheres — check dimensions
    if ptype in ('sphere', 'half_sphere', 'cube'):
        if lightweight:
            return 'Lightweight'
        # If too big for lightweight, fall through to weight check below

    # Default — use FedEx/UPS based on box size
    if bl > 48 or bw > 48:
        return 'Mid Sheet - LS 48+ or SLS 30+, 20+ LBS SS 70+ LBS'

    return 'Lightweight' if w < 1.0 else 'Small Sheet - LS LT 48 or SLS LT 30, 20-70 LBS'

# ============================================================
# BRAND HELPER
# ============================================================

def get_brand(is_third_party, brand_name):
    """Return correct brand — third party brand or Plastic-Craft."""
    if is_third_party and brand_name:
        return brand_name
    return 'Plastic-Craft'

# ============================================================
# AGE RESTRICTION
# Small spheres and cubes are choking hazards
# ============================================================

def get_age_restriction(dims, ptype):
    """Flag choking hazard for small spheres and cubes."""
    if ptype in ('sphere', 'half_sphere'):
        dia = dims.get('diameter', 999)
        if dia and dia < 1.75:
            return '3+'
    if ptype == 'cube':
        size = dims.get('size', 999)
        if size and size < 1.25:
            return '3+'
    return 'None'

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
    'item_weight_lbs', 'dim_weight_lbs', 'chargeable_weight_lbs', 'dim_wins',
    'shipping_weight_lbs',
    'box_length_in', 'box_width_in', 'box_height_in',
    'item_length_in', 'item_width_in', 'item_height_in',
    'listing_status', 'incomplete_reason', 'product_type', 'material_key',
    'had_mm_conversion', 'mm_conversions',
    'freight_notice_applied', 'video_recommended',
    'compliance_flags', 'is_third_party_brand',
    'material_composition', 'recommended_uses', 'rufus_qa_included',
    # New attribute fields
    'item_type_keyword',
    'condition',
    'brand',
    'manufacturer',
    'country_of_origin',
    'part_number',
    'model_number',
    'style',
    'item_form',
    'contains_liquid',
    'age_restriction',
    'shipping_template',
    # Audit fields
    'l1_input_issues', 'l2_output_issues',
    'l3_fixes_applied', 'l4_field_retries',
    'validation_final', 'status'
]

# ============================================================
# INCOMPLETE LISTING DIAGNOSIS
# ============================================================

def get_incomplete_reason(row):
    """
    Diagnose why a listing is incomplete.
    Note: Script fixes description automatically.
    Price must be set by your pricing system — never touched here.
    """
    status = row.get('status', '').strip()
    if status != 'Incomplete':
        return ''

    reasons = []
    missing_price = not row.get('price', '').strip()
    missing_desc = not row.get('item-description', '').strip()
    missing_title = not row.get('item-name', '').strip()
    missing_asin = not row.get('asin1', '').strip()

    if missing_price:
        reasons.append('Missing price — needs pricing system update')
    if missing_desc:
        reasons.append('Missing description — FIXED by script')
    if missing_title:
        reasons.append('Missing title — FIXED by script')
    if missing_asin:
        reasons.append('Missing ASIN — may need Amazon catalog match')
    if not reasons:
        reasons.append('Unknown — review in Seller Central')

    return ' | '.join(reasons)

# ============================================================
# TEXT CLEANING
# Removes encoding artifacts like â€" from listing content
# ============================================================

def clean_text(text):
    if not text:
        return text
    # Fix common UTF-8 mojibake
    fixes = [
        ('â€"', '-'), ('â€™', "'"), ('â€œ', '"'), ('â€', '"'),
        ('â€˜', "'"), ('Â°', '°'), ('Â·', '·'), ('Â½', '1/2'),
        ('Â¼', '1/4'), ('Â¾', '3/4'), ('Ã—', 'x'),
        ('â„¢', ''), ('Â®', ''), ('Â©', ''), ('\u00a0', ' '),
        ('\u2013', '-'), ('\u2014', '-'), ('\u2019', "'"),
        ('\u201c', '"'), ('\u201d', '"'), ('\u00ae', ''), ('\u2122', ''),
        ('ï¿½', ''), ('\ufffd', ''), ('\ufeff', ''),
    ]
    for bad, good in fixes:
        text = text.replace(bad, good)
    # Remove remaining non-ASCII
    text = re.sub(r'[^\x00-\x7F]+', '', text)
    # Clean extra spaces
    text = re.sub(r' +', ' ', text).strip()
    return text

# ============================================================
# MAIN PROCESSING
# ============================================================

def process_listings():
    global client
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        logger.critical("ANTHROPIC_API_KEY not set — cannot run")
        return None

    # Initialize client here, AFTER confirming API key exists
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    start_time = datetime.now()
    logger.info("=" * 65)
    logger.info("Plastic-Craft Amazon Listing Optimizer v4.0")
    logger.info(f"Mode: {'TEST (' + str(TEST_LIMIT) + ')' if TEST_MODE else 'FULL CATALOG RUN'}")
    logger.info(f"Output: {OUTPUT_FILE}")
    logger.info("=" * 65)

    jlog.log('run_start', {
        'mode': 'test' if TEST_MODE else 'full',
        'version': '4.0',
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
    logger.info("✓ Self-healing engine initialized")
    logger.info("✓ Dimension sanity checker initialized")


    # Load checkpoint
    checkpoint = load_checkpoint()
    completed_skus = set(checkpoint.get('completed', []))

    # Read listings
    all_listings = []
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                # Include ALL statuses — Active, Inactive, Incomplete
                # One run fixes everything. Review CSV to decide what to keep/delete.
                status = row.get('status', '').strip()
                if status in ('Active', 'Inactive', 'Incomplete'):
                    all_listings.append(row)
        status_counts = {}
        for r in all_listings:
            s = r.get('status', '').strip()
            status_counts[s] = status_counts.get(s, 0) + 1
        logger.info(f"✓ Loaded {len(all_listings):,} total listings — Active: {status_counts.get('Active',0):,} | Inactive: {status_counts.get('Inactive',0):,} | Incomplete: {status_counts.get('Incomplete',0):,}")
    except Exception as e:
        logger.critical(f"✗ Failed to read input file: {e}")
        return None

    listings = [r for r in all_listings if r.get('seller-sku', '') not in completed_skus]
    logger.info(f"📋 Remaining: {len(listings):,}")

    if TEST_MODE:
        samples = []
        seen_types = set()
        seen_statuses = {'Active': 0, 'Inactive': 0, 'Incomplete': 0}

        # Target diverse product types AND all three statuses
        targets = ['tube', 'rod', 'sheet', 'cutting board', 'acrylic',
                   'nylon', 'polycarbonate', 'hdpe', 'uhmw', 'sphere']

        # First pass — get one of each status
        for status in ('Active', 'Inactive', 'Incomplete'):
            for row in listings:
                if row.get('status', '').strip() == status and seen_statuses[status] == 0:
                    samples.append(row)
                    seen_statuses[status] += 1
                    break

        # Second pass — get diverse product types
        for t in targets:
            for row in listings:
                if len(samples) >= TEST_LIMIT: break
                title = row.get('item-name', '').lower()
                if t in title and t not in seen_types and row not in samples:
                    samples.append(row)
                    seen_types.add(t)

        # Fill remaining slots
        for row in listings:
            if len(samples) >= TEST_LIMIT: break
            if row not in samples:
                samples.append(row)

        listings = samples[:TEST_LIMIT]
        status_summary = {s: sum(1 for r in listings if r.get('status','').strip() == s)
                         for s in ('Active', 'Inactive', 'Incomplete')}
        logger.info(f"Test mode: {len(listings)} listings — {status_summary}")

    logger.info("=" * 65)

    # Stats
    stats = {
        'total': 0, 'successful': 0, 'warnings': 0, 'errors': 0,
        'freight_flagged': 0, 'video_flagged': 0, 'compliance_flagged': 0,
        'mm_converted': 0,
        'l1_flagged': 0, 'l2_caught': 0, 'l3_fixes': 0, 'l4_retries': 0,
        'top_errors': {},
    }

    errors = []
    processed_count = 0

    out_exists = Path(OUTPUT_FILE).exists()
    out_f = open(OUTPUT_FILE, 'a', newline='', encoding='utf-8')
    out_writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES)
    if not out_exists: out_writer.writeheader()

    err_exists = Path(ERROR_LOG).exists()
    err_f = open(ERROR_LOG, 'a', newline='', encoding='utf-8')
    err_writer = csv.DictWriter(err_f, fieldnames=['sku', 'asin', 'title', 'issues', 'status'])
    if not err_exists: err_writer.writeheader()

    for i, row in enumerate(listings):
        title = clean_text(row.get('item-name', '').strip())
        sku = row.get('seller-sku', '').strip()
        asin = row.get('asin1', '').strip()

        logger.info(f"\n[{i+1}/{len(listings)}] SKU: {sku}")
        logger.debug(f"  Title: {title[:80]}...")

        jlog.log('listing_start', {'sku': sku, 'asin': asin, 'title': title[:100]})

        # Detect brand
        is_third_party, brand_name, weld_on_num = detect_brand(title)

        # L1: Parse input
        dims = extract_dimensions(title)
        material_key, material_name = detect_material(title)
        modifiers, cast_ext, solid_hollow, color = detect_color_and_modifiers(title)
        pack = detect_pack(title)

        # Pass pack into dims so weight calculation can multiply correctly
        if pack and pack > 1:
            dims['pack'] = pack

        if dims.get('had_mm'):
            logger.info(f"  ↻ MM conversion: {dims.get('mm_conversions')}")
            stats['mm_converted'] += 1

        l1_issues = validate_input(dims, material_key, title)
        if l1_issues:
            logger.warning(f"  ⚠ L1: {', '.join(l1_issues)}")
            stats['l1_flagged'] += 1
            healer.record_failing_title(title)

        # Build title using our formatter
        new_title = build_title(
            title, dims, material_key, material_name,
            modifiers, cast_ext, solid_hollow, color, pack,
            is_third_party, brand_name, weld_on_num
        )
        logger.debug(f"  Title built: {new_title[:80]}...")

        # Weight and box
        weight = calculate_weight(material_key, dims)
        box_dims = calculate_box_dims(dims)

        # Sanity check
        san_valid, san_issues, dims, weight, box_dims = sanity.check_all(
            dims, weight, box_dims, material_key, title
        )
        for issue in san_issues:
            if issue['severity'] == 'ERROR':
                logger.error(f"  ✗ SANITY [{issue['field']}]: {issue['msg']}")
                if 'corrected_to' in issue:
                    logger.info(f"    Corrected to: {issue['corrected_to']}")
            else:
                logger.warning(f"  ⚠ SANITY [{issue['field']}]: {issue['msg']}")
                if issue.get('suggested_weight'):
                    weight = issue['suggested_weight']

        # Calculate DIM weight and chargeable weight
        chargeable_weight, dim_weight, dim_wins = get_chargeable_weight(weight, box_dims)
        lightweight = is_lightweight(box_dims, chargeable_weight)

        freight_needed = needs_freight_notice(dims, chargeable_weight, box_dims, actual_weight=weight)
        if freight_needed:
            logger.info(f"  🚛 Freight notice applied")
            if dim_wins:
                logger.info(f"    DIM weight ({dim_weight} lbs) > actual ({weight} lbs) — using DIM")
            stats['freight_flagged'] += 1


        video_flag = should_flag_video(material_key, dims.get('product_type', ''), weight)
        if video_flag: stats['video_flagged'] += 1

        compliance = ', '.join(MATERIAL_DATA.get(material_key, {}).get('compliance', []))
        if compliance: stats['compliance_flagged'] += 1

        base_backend = get_base_backend_terms(material_key, dims.get('product_type', 'unknown'))
        subject_matter = get_subject_matter(material_key, dims.get('product_type', 'unknown'))
        intended_use_attr = get_intended_use(material_key, dims.get('product_type', 'unknown'))
        target_audience = get_target_audience(material_key, dims.get('product_type', 'unknown'))

        # Get shape-specific intended use for bullet/description
        mat_data = MATERIAL_DATA.get(material_key, {})
        shape_use = mat_data.get('intended_use_by_shape', {})
        ptype = dims.get('product_type', 'accessory')
        intended_use_content = shape_use.get(ptype) or shape_use.get('default') or intended_use_attr

        ptype = dims.get('product_type', '')
        item_l = dims.get('length', '')
        item_w = dims.get('width') or dims.get('od') or dims.get('size') or dims.get('diameter') or ''
        item_h = dims.get('thickness') or dims.get('od') or dims.get('size') or ''

        # Generate content
        content = None
        attempt = 0
        while attempt < MAX_RETRIES and content is None:
            attempt += 1
            if attempt > 1:
                logger.warning(f"  ↻ API retry {attempt}/{MAX_RETRIES}...")
                time.sleep(RETRY_DELAY * attempt)
            content = generate_content(
                row, dims, material_key, material_name,
                weight, box_dims, base_backend, freight_needed,
                new_title, intended_use_content
            )

        if content is None:
            logger.error(f"  ✗ All {MAX_RETRIES} API attempts failed")
            stats['errors'] += 1
            result = {
                'sku': sku, 'asin': asin, 'original_title': title,
                'new_title': new_title, 'title_char_count': len(new_title),
                'title_mobile_ok': 'ERROR',
                'bullet1': '', 'bullet2': '', 'bullet3': '',
                'bullet4': '', 'bullet5': '', 'description': '',
                'description_char_count': 0,
                'backend_search_terms': '', 'backend_byte_count': 0,
                'subject_matter': subject_matter,
                'intended_use': intended_use_attr,
                'target_audience': target_audience,
                'material_type': material_name, 'color': color or '',
                'size_description': '', 'finish_type': '',
                'item_weight_lbs': weight or '',
                'shipping_weight_lbs': weight or '',
                'box_length_in': box_dims.get('box_length', ''),
                'box_width_in': box_dims.get('box_width', ''),
                'box_height_in': box_dims.get('box_height', ''),
                'item_length_in': item_l, 'item_width_in': item_w, 'item_height_in': item_h,
                'listing_status': row.get('status', '').strip(),
            'incomplete_reason': get_incomplete_reason(row),
            'product_type': ptype, 'material_key': material_key,
                'had_mm_conversion': 'YES' if dims.get('had_mm') else 'NO',
                'mm_conversions': str(dims.get('mm_conversions', '')),
                'freight_notice_applied': 'YES' if freight_needed else 'NO',
                'video_recommended': 'YES' if video_flag else 'NO',
                'compliance_flags': compliance,
                'is_third_party_brand': 'YES' if is_third_party else 'NO',
                'item_type_keyword':  get_item_type_keyword(ptype, cast_ext, material_name),
                'condition':          'New',
                'brand':              get_brand(is_third_party, brand_name),
                'manufacturer':       'Plastic-Craft Products',
                'country_of_origin':  'United States',
                'part_number':        sku,
                'model_number':       sku,
                'style':              get_style(ptype, cast_ext, material_name),
                'item_form':          get_item_form(ptype, solid_hollow),
                'contains_liquid':    'Yes' if ptype == 'adhesive' else 'No',
                'age_restriction':    get_age_restriction(dims, ptype),
                'shipping_template':  get_shipping_template(dims, box_dims, chargeable_weight, lightweight, is_third_party, brand_name),
                        'l1_input_issues': ' | '.join(l1_issues) if l1_issues else 'PASS',
                'l2_output_issues': 'API_FAILED',
                'l3_fixes_applied': 'NONE', 'l4_field_retries': 'NONE',
                'validation_final': 'FAILED', 'status': 'error'
            }
            out_writer.writerow(result)
            out_f.flush()
            err_writer.writerow({'sku': sku, 'asin': asin, 'title': title,
                                 'issues': 'API_FAILED', 'status': 'error'})
            err_f.flush()
            errors.append(sku)
            checkpoint['completed'].append(sku)
            processed_count += 1
            stats['total'] += 1
            healer.record_result(sku, l1_issues, [], [])
            if processed_count % CHECKPOINT_INTERVAL == 0:
                save_checkpoint(checkpoint)
            time.sleep(0.5)
            continue

        # L2 Validation
        l2_issues = validate_output(content, freight_needed)
        if l2_issues:
            logger.warning(f"  ⚠ L2: {', '.join(l2_issues)}")
            stats['l2_caught'] += len(l2_issues)

        # L3 Auto-correction
        l3_fixes = []
        if l2_issues:
            content, l3_fixes = auto_correct(content, l2_issues, freight_needed)
            if l3_fixes:
                logger.info(f"  ✓ L3: {', '.join(l3_fixes)}")
                stats['l3_fixes'] += len(l3_fixes)

        # L4 Targeted retry
        remaining = validate_output(content, freight_needed)
        l4_retries = []
        for issue in remaining:
            field = None
            if 'TITLE' in issue and 'TRIMMED' not in issue: field = 'title'
            elif re.match(r'BULLET(\d)_(MISSING|NO_CAPS)', issue):
                field = f'bullet{re.match(r"BULLET(\d)", issue).group(1)}'
            elif 'DESCRIPTION_MISSING' in issue and 'CERTIFICATION' not in issue: field = 'description'
            elif 'BACKEND' in issue and 'TRIMMED' not in issue: field = 'backend_search_terms'
            if field:
                logger.info(f"  ↻ L4: retrying {field}...")
                new_val = retry_single_field(field, content, row, dims,
                                             material_key, material_name, new_title)
                if new_val:
                    content[field] = new_val
                    l4_retries.append(field)
                    logger.info(f"  ✓ L4: fixed {field}")
                    stats['l4_retries'] += 1

        # Final backend cleanup
        raw_backend = content.get('backend_search_terms', '')
        final_backend = clean_and_trim_backend_terms(
            raw_backend.split() + base_backend,
            content.get('title', new_title)
        )
        content['backend_search_terms'] = final_backend

        # Always use our pre-built title
        content['title'] = new_title

        # Final validation
        final_issues = validate_output(content, freight_needed)
        final_status = 'ready' if not final_issues else 'ready_with_warnings'
        if final_issues: stats['warnings'] += 1
        else: stats['successful'] += 1

        result = {
            'sku': sku, 'asin': asin,
            'original_title': title,
            'new_title': content.get('title', new_title),
            'title_char_count': len(content.get('title', new_title)),
            'title_mobile_ok': 'YES' if 'Plastic-Craft' in content.get('title', new_title)[:80] else 'CHECK',
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
            'intended_use': intended_use_attr,
            'target_audience': target_audience,
            'material_type': content.get('material_type', material_name),
            'color': content.get('color', color or ''),
            'size_description': content.get('size_description', ''),
            'finish_type': content.get('finish_type', 'Standard'),
            'item_weight_lbs': weight or '',
            'dim_weight_lbs': dim_weight or '',
            'chargeable_weight_lbs': chargeable_weight or '',
            'dim_wins': 'YES' if dim_wins else 'NO',
            'shipping_weight_lbs': chargeable_weight or weight or '',
            'box_length_in': box_dims.get('box_length', ''),
            'box_width_in': box_dims.get('box_width', ''),
            'box_height_in': box_dims.get('box_height', ''),
            'item_length_in': item_l, 'item_width_in': item_w, 'item_height_in': item_h,
            'listing_status': row.get('status', '').strip(),
            'incomplete_reason': get_incomplete_reason(row),
            'product_type': ptype, 'material_key': material_key,
            'had_mm_conversion': 'YES' if dims.get('had_mm') else 'NO',
            'mm_conversions': str(dims.get('mm_conversions', '')),
            'freight_notice_applied': 'YES' if freight_needed else 'NO',
            'video_recommended': 'YES' if video_flag else 'NO',
            'compliance_flags': compliance,
            'is_third_party_brand': 'YES' if is_third_party else 'NO',
            'material_composition': MATERIAL_COMPOSITION.get(material_key, ''),
            'recommended_uses': get_recommended_uses(material_key, ptype),
            'rufus_qa_included': 'YES' if RUFUS_QA.get(material_key) else 'NO',
            'item_type_keyword':  get_item_type_keyword(ptype, cast_ext, material_name),
            'condition':          'New',
            'brand':              get_brand(is_third_party, brand_name),
            'manufacturer':       'Plastic-Craft Products',
            'country_of_origin':  'United States',
            'part_number':        sku,
            'model_number':       sku,
            'style':              get_style(ptype, cast_ext, material_name),
            'item_form':          get_item_form(ptype, solid_hollow),
            'contains_liquid':    'Yes' if ptype == 'adhesive' else 'No',
            'age_restriction':    get_age_restriction(dims, ptype),
            'shipping_template':  get_shipping_template(dims, box_dims, chargeable_weight, lightweight, is_third_party, brand_name),
            'l1_input_issues': ' | '.join(l1_issues) if l1_issues else 'PASS',
            'l2_output_issues': ' | '.join(l2_issues) if l2_issues else 'PASS',
            'l3_fixes_applied': ' | '.join(l3_fixes) if l3_fixes else 'NONE',
            'l4_field_retries': ' | '.join(l4_retries) if l4_retries else 'NONE',
            'validation_final': ' | '.join(final_issues) if final_issues else 'PASS',
            'status': final_status,
        }

        out_writer.writerow(result)
        out_f.flush()

        logger.info(f"  ✓ {final_status.upper()}")
        logger.info(f"    Title ({len(content.get('title',new_title))} chars): {content.get('title',new_title)[:65]}...")
        logger.debug(f"    Backend: {len(final_backend.encode())} bytes | Weight: {weight} lbs")

        jlog.log('listing_complete', {
            'sku': sku, 'status': final_status,
            'freight': freight_needed,
            'had_mm': dims.get('had_mm', False),
            'l1': len(l1_issues), 'l2': len(l2_issues),
            'l3': len(l3_fixes), 'l4': len(l4_retries),
        })

        checkpoint['completed'].append(sku)
        processed_count += 1
        stats['total'] += 1

        healer.record_result(sku, l1_issues, l2_issues, final_issues or [])
        if l1_issues: healer.record_failing_title(title)

        if processed_count % CHECKPOINT_INTERVAL == 0:
            save_checkpoint(checkpoint)
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = processed_count / elapsed * 60 if elapsed > 0 else 0
            logger.info(f"\n  💾 Checkpoint saved — {processed_count} done ({rate:.1f}/min)")

        time.sleep(0.5)

    out_f.close()
    err_f.close()

    if not TEST_MODE and not errors:
        clear_checkpoint()

    # Self-heal summary
    heal_summary = healer.get_summary()
    stats['self_heal_attempted'] = sum(heal_summary['rewrites_attempted'].values())
    stats['self_heal_applied'] = len(heal_summary['rewrites_applied'])

    duration = (datetime.now() - start_time).total_seconds()
    stats['duration_seconds'] = duration
    stats['output_file'] = OUTPUT_FILE
    stats['error_log'] = ERROR_LOG

    summary_file = write_run_summary(LOG_DIR, RUN_ID, stats)

    logger.info("\n" + "=" * 65)
    logger.info("RUN COMPLETE — v4.0")
    logger.info(f"  Total     : {stats['total']:,}")
    logger.info(f"  Successful: {stats['successful']:,}")
    logger.info(f"  Warnings  : {stats['warnings']:,}")
    logger.info(f"  Errors    : {stats['errors']:,}")
    logger.info(f"  MM conv   : {stats['mm_converted']:,}")
    logger.info(f"  Freight   : {stats['freight_flagged']:,}")
    logger.info(f"  L3 fixes  : {stats['l3_fixes']:,}")
    logger.info(f"  L4 retries: {stats['l4_retries']:,}")
    logger.info(f"  Duration  : {int(duration//60)}m {int(duration%60)}s")
    logger.info(f"  Output    : {OUTPUT_FILE}")
    if summary_file: logger.info(f"  Summary   : {summary_file}")
    logger.info("=" * 65)

    jlog.log('run_complete', {**stats, 'error_skus': errors[:20]})
    return OUTPUT_FILE

if __name__ == '__main__':
    process_listings()
