"""
pc_policy_validator.py — Amazon listing policy compliance filter.

Catches and auto-fixes content that violates Amazon's listing policies
before it reaches the push scripts or the CSV output.

Hard violations (auto-removed/replaced):
  - Contact info: email addresses, phone numbers, URLs
  - Discount/promotional language: free, sale, discount, cheap, save X%
  - Guarantee language
  - "#1" / "number one" marketing claims
  - "Limited time", "act now", "order now"
  - HTML tags

Soft violations (replaced with compliant alternatives):
  - Vague superlatives: "best available", "highest quality", "superior"
  - Unverified "the only" claims
  - "World-class", "industry-leading" without data

Usage:
    from pc_policy_validator import validate_and_fix, check_backend_terms

    clean_text, violations = validate_and_fix(text, field='description')
    clean_terms, violations = check_backend_terms(terms_string)
"""

import re

# ── Hard prohibited patterns ───────────────────────────────────────────────────
# These are removed or replaced automatically.

_HARD_PATTERNS = [
    # Contact info
    (re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'),
     '', 'email address in listing'),
    (re.compile(r'\b\d{3}[\s.\-]?\d{3}[\s.\-]?\d{4}\b'),
     '', 'phone number in listing'),
    (re.compile(r'https?://\S+', re.IGNORECASE),
     '', 'URL in listing'),
    (re.compile(r'\bwww\.\S+', re.IGNORECASE),
     '', 'URL in listing'),

    # HTML tags
    (re.compile(r'<[^>]+>'),
     '', 'HTML tag in listing'),

    # Discount / promotional language
    (re.compile(r'\b(?:limited[\s-]time|act\s+now|order\s+now|while\s+supplies\s+last)\b', re.IGNORECASE),
     '', 'promotional urgency language'),
    (re.compile(r'\b\d+\s*%\s*off\b', re.IGNORECASE),
     '', 'percentage discount claim'),
    (re.compile(r'\bsave\s+\$[\d,.]+\b', re.IGNORECASE),
     '', 'dollar savings claim'),

    # Guarantee language (standalone — not "quality guarantee" as a concept)
    (re.compile(r'\b(?:money[\s-]back\s+guarantee|satisfaction\s+guaranteed)\b', re.IGNORECASE),
     '', 'guarantee claim'),

    # Seller feedback solicitation
    (re.compile(r'\bleave\s+(?:a\s+)?(?:review|feedback|rating)\b', re.IGNORECASE),
     '', 'feedback solicitation'),
    (re.compile(r'\bplease\s+(?:rate|review|leave)\b', re.IGNORECASE),
     '', 'feedback solicitation'),

    # FDA / EPA / regulatory claims (prohibited without actual registration)
    (re.compile(r'\bFDA[\s-](?:approved|cleared|registered|compliant|certified|listed)\b', re.IGNORECASE),
     '', 'unverified FDA claim'),
    (re.compile(r'\bcompliant\s+with\s+(?:the\s+)?(?:FDA|EPA|USDA|NSF|OSHA|REACH|RoHS)\b', re.IGNORECASE),
     '', 'unverified regulatory compliance claim'),
    (re.compile(r'\bFDA\s+\d+\s+CFR\b', re.IGNORECASE),
     '', 'unverified FDA CFR reference'),
    (re.compile(r'\b21\s+CFR\b', re.IGNORECASE),
     '', 'unverified FDA CFR reference'),
    (re.compile(r'\bNSF[\s-](?:certified|approved|compliant|listed|rated|standard[s]?)\b', re.IGNORECASE),
     '', 'unverified NSF claim'),
    (re.compile(r'\bNSF\s+standard[s]?\b', re.IGNORECASE),
     '', 'unverified NSF claim'),
    (re.compile(r'\bEPA[\s-](?:approved|registered|certified|compliant)\b', re.IGNORECASE),
     '', 'unverified EPA claim'),
    (re.compile(r'\bUSDA[\s-](?:approved|certified|compliant)\b', re.IGNORECASE),
     '', 'unverified USDA claim'),
    (re.compile(r'\bapproved\s+by\s+(?:the\s+)?(?:FDA|EPA|USDA)\b', re.IGNORECASE),
     '', 'unverified regulatory approval claim'),
    (re.compile(r'\bregistered\s+with\s+(?:the\s+)?(?:FDA|EPA)\b', re.IGNORECASE),
     '', 'unverified regulatory registration claim'),
    (re.compile(r'\bcertified\s+by\s+(?:the\s+)?(?:FDA|EPA|USDA)\b', re.IGNORECASE),
     '', 'unverified regulatory certification claim'),
    (re.compile(r'\bdirect\s+food\s+contact\b', re.IGNORECASE),
     '', 'unverified food-contact claim'),

    # Antimicrobial / pesticide claims (requires EPA registration — hard ban)
    (re.compile(r'\bbacteria[\s-]free\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\bgerm[\s-]free\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\bmicrobe[\s-]free\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\bpathogen[\s-]free\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\bantimicrobial\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\bantibacterial\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\bantifungal\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\bkills?\s+(?:bacteria|germs|microbes|pathogens)\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\beliminates?\s+(?:bacteria|germs|microbes|odors?)\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\brepels?\s+(?:bacteria|germs|microbes)\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\bresists?\s+(?:bacteria|germs|microbes|mold|mildew)\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\binhibits?\s+(?:bacteria|germ|microbial|mold|mildew)\s*(?:growth)?\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\bprevents?\s+(?:bacterial|germ|microbial)\s+(?:growth|buildup|spread)\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),

    # Sterilization claims — Amazon reads as pesticide/antimicrobial claim
    (re.compile(r'\bsteriliz(?:e|ed|able|ation|ing)\b', re.IGNORECASE),
     '', 'sterilization claim (pesticide flag)'),
    (re.compile(r'\bautoclavable\b', re.IGNORECASE),
     'heat resistant', 'autoclavable → heat resistant'),

    # Sanitizer references — using "sanitizer" as a claim is flagged
    (re.compile(r'\bsanitiz(?:e|er|ers|ed|able|ation|ing)\b', re.IGNORECASE),
     '', 'sanitizer/sanitize claim (pesticide flag)'),
    (re.compile(r'\bdisinfect(?:s|ed|ant|ion|ing)?\b', re.IGNORECASE),
     '', 'disinfectant claim (pesticide flag)'),

    # "Resists chemicals" is fine, but "resists bacteria/pathogens" isn't.
    # Catch any resist/-resistant to biological entities
    (re.compile(r'\bresists?\s+(?:pathogens?|viruses?|fungi|microorganisms?)\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),
    (re.compile(r'\b(?:pathogen|virus|fungi|microorganism)[\s-]resistant\b', re.IGNORECASE),
     '', 'unregistered antimicrobial claim'),

    # Unverified clinical / lab claims
    (re.compile(r'\bclinically[\s-](?:proven|tested|verified|studied)\b', re.IGNORECASE),
     '', 'unverified clinical claim'),
    (re.compile(r'\bscientifically[\s-](?:proven|tested|verified)\b', re.IGNORECASE),
     '', 'unverified clinical claim'),
    (re.compile(r'\b(?:lab|laboratory)[\s-](?:proven|tested|verified|certified)\b', re.IGNORECASE),
     '', 'unverified lab claim'),
    (re.compile(r'\bdoctor[\s-](?:approved|recommended|tested)\b', re.IGNORECASE),
     '', 'unverified professional endorsement'),
    (re.compile(r'\bdermatologist[\s-](?:approved|recommended|tested)\b', re.IGNORECASE),
     '', 'unverified professional endorsement'),

    # Chemical-free / toxic-free (scientifically misleading — Amazon prohibited)
    (re.compile(r'\bchemical[\s-]free\b', re.IGNORECASE),
     '', 'misleading chemical-free claim'),
    (re.compile(r'\btoxin[\s-]free\b', re.IGNORECASE),
     '', 'misleading toxin-free claim'),

    # Unverified environmental claims (Amazon requires certification)
    (re.compile(r'\beco[\s-]friendly\b', re.IGNORECASE),
     '', 'unverified environmental claim'),
    (re.compile(r'\benvironmentally[\s-]friendly\b', re.IGNORECASE),
     '', 'unverified environmental claim'),
    (re.compile(r'\b(?:100\s*%\s*)?sustainable\b', re.IGNORECASE),
     '', 'unverified sustainability claim'),
    (re.compile(r'\bgreen\s+(?:product|material|plastic|choice)\b', re.IGNORECASE),
     '', 'unverified environmental claim'),
    (re.compile(r'\ball[\s-]natural\b', re.IGNORECASE),
     '', 'unverified natural claim'),
    (re.compile(r'\b100\s*%\s*natural\b', re.IGNORECASE),
     '', 'unverified natural claim'),
    (re.compile(r'\bzero[\s-]waste\b', re.IGNORECASE),
     '', 'unverified environmental claim'),
    (re.compile(r'\bcarbon[\s-]neutral\b', re.IGNORECASE),
     '', 'unverified environmental claim'),

    # Media / external validation claims
    (re.compile(r'\bas\s+seen\s+on\s+(?:tv|television|hgtv|cnn|fox|nbc|abc|cbs)\b', re.IGNORECASE),
     '', 'media claim'),
    (re.compile(r'\bas\s+featured\s+in\b', re.IGNORECASE),
     '', 'media claim'),

    # Amazon program references (prohibited by Amazon ToS)
    (re.compile(r"\bamazon'?s?\s+choice\b", re.IGNORECASE),
     '', "Amazon program reference"),
    (re.compile(r'\bbest\s+seller\s+(?:badge|rank|status)\b', re.IGNORECASE),
     '', 'Amazon program reference'),
    (re.compile(r'\b(?:fulfilled\s+by\s+amazon|fba|ships?\s+from\s+amazon)\b', re.IGNORECASE),
     '', 'Amazon program reference'),
    (re.compile(r'\bprime\s+(?:eligible|shipping|delivery)\b', re.IGNORECASE),
     '', 'Amazon Prime reference'),

    # Price / comparison claims
    (re.compile(r'\b(?:best|lowest|cheapest|unbeatable)\s+price\b', re.IGNORECASE),
     '', 'price superlative'),
    (re.compile(r'\bcompare\s+(?:at|to|with)\b', re.IGNORECASE),
     '', 'comparative pricing claim'),
    (re.compile(r'\bprice\s+match\b', re.IGNORECASE),
     '', 'price match claim'),
    (re.compile(r'\bmanufacturer[\s-]direct\b', re.IGNORECASE),
     '', 'pricing/origin claim'),
    (re.compile(r'\bbetter\s+than\s+(?:any|all|competitors?|the\s+rest)\b', re.IGNORECASE),
     '', 'competitor comparison claim'),

    # Urgency / scarcity (more variants)
    (re.compile(r'\blast\s+chance\b', re.IGNORECASE),
     '', 'urgency claim'),
    (re.compile(r'\bhurry\b', re.IGNORECASE),
     '', 'urgency claim'),
    (re.compile(r"\bdon'?t\s+miss\b", re.IGNORECASE),
     '', 'urgency claim'),
    (re.compile(r'\bselling\s+(?:out\s+)?fast\b', re.IGNORECASE),
     '', 'urgency claim'),
    (re.compile(r'\bwhile\s+they\s+last\b', re.IGNORECASE),
     '', 'urgency claim'),
    (re.compile(r'\bclearance\b', re.IGNORECASE),
     '', 'clearance claim'),

    # Medical / therapeutic claims (hard ban without registration)
    (re.compile(r'\btreats?\s+(?:infection|disease|illness|pain|symptoms?)\b', re.IGNORECASE),
     '', 'medical treatment claim'),
    (re.compile(r'\bcures?\s+\w+', re.IGNORECASE),
     '', 'medical cure claim'),
    (re.compile(r'\bheals?\s+(?:wounds?|cuts?|burns?|injuries?)\b', re.IGNORECASE),
     '', 'medical healing claim'),
    (re.compile(r'\brelieve[sd]?\s+(?:pain|stress|anxiety|symptoms?)\b', re.IGNORECASE),
     '', 'medical relief claim'),
    (re.compile(r'\btherapeutic\b', re.IGNORECASE),
     '', 'therapeutic claim'),
    (re.compile(r'\bmedical[\s-]grade\b', re.IGNORECASE),
     '', 'unverified medical-grade claim'),
    (re.compile(r'\bhospital[\s-]grade\b', re.IGNORECASE),
     '', 'unverified hospital-grade claim'),
    (re.compile(r'\bpharmaceutical[\s-]grade\b', re.IGNORECASE),
     '', 'unverified pharmaceutical-grade claim'),

    # Certification claims without proof (require substantiation)
    (re.compile(r'\bBPA[\s-]free\b', re.IGNORECASE),
     '', 'unverified BPA-free claim'),
    (re.compile(r'\blead[\s-]free\b', re.IGNORECASE),
     '', 'unverified lead-free claim'),
    (re.compile(r'\bphthalate[\s-]free\b', re.IGNORECASE),
     '', 'unverified phthalate-free claim'),
    (re.compile(r'\bnon[\s-]toxic\b', re.IGNORECASE),
     '', 'unverified non-toxic claim'),

    # Pest / insect claims (trigger pesticide review)
    (re.compile(r'\b(?:kills?|repels?|deters?)\s+(?:insects?|bugs?|pests?|rodents?|mice|rats?|ants?|roaches|mosquitoes)\b', re.IGNORECASE),
     '', 'pesticide claim'),
    (re.compile(r'\binsect[\s-](?:proof|resistant|repellent)\b', re.IGNORECASE),
     '', 'pesticide claim'),
    (re.compile(r'\bpest[\s-](?:proof|resistant|repellent|control)\b', re.IGNORECASE),
     '', 'pesticide claim'),

    # Food safety / food contact claims (require FDA or NSF certification)
    (re.compile(r'\bfood[\s-]grade\b', re.IGNORECASE),
     '', 'unverified food-grade claim'),
    (re.compile(r'\bfood[\s-]safe\b', re.IGNORECASE),
     '', 'unverified food-safe claim'),
    (re.compile(r'\bfood[\s-]contact\s+(?:safe|approved|rated|compliant)\b', re.IGNORECASE),
     '', 'unverified food-contact claim'),
    (re.compile(r'\bsafe\s+for\s+food\s+(?:contact|use|storage)\b', re.IGNORECASE),
     '', 'unverified food-safe claim'),

    # Third-party standards claims (require actual certification to substantiate)
    (re.compile(r'\bRoHS[\s-](?:compliant|certified|approved|listed)\b', re.IGNORECASE),
     '', 'unverified RoHS claim'),
    (re.compile(r'\bNSF[\s-](?:certified|approved|compliant|listed|rated)\b', re.IGNORECASE),
     '', 'unverified NSF claim'),
    (re.compile(r'\bISO[\s-]?(?:\d{3,5}(?::\d{4})?[\s-]*)?(?:certified|compliant|approved|listed|standard)\b', re.IGNORECASE),
     '', 'unverified ISO certification claim'),
    (re.compile(r'\bASTM\s+[A-Z]\d+\b', re.IGNORECASE),
     '', 'unverified ASTM standard reference'),
    (re.compile(r'\bCE[\s-](?:certified|compliant|marked|approved|listed)\b', re.IGNORECASE),
     '', 'unverified CE mark claim'),
    (re.compile(r'\bUL[\s-](?:listed|certified|approved|rated)\b', re.IGNORECASE),
     '', 'unverified UL listing claim'),
    (re.compile(r'\bREACH[\s-](?:compliant|certified)\b', re.IGNORECASE),
     '', 'unverified REACH compliance claim'),
    (re.compile(r'\bOSHA[\s-](?:compliant|approved|certified)\b', re.IGNORECASE),
     '', 'unverified OSHA claim'),

    # All guarantee language (including "guaranteed optical clarity", etc.)
    (re.compile(r'\bguarantee[d]?\b', re.IGNORECASE),
     '', 'guarantee claim'),

    # Comparison to banned competitor brand names in promotional context
    (re.compile(r'\b(?:better|stronger|clearer|superior)\s+than\s+(?:plexiglas|lucite|lexan)\b', re.IGNORECASE),
     '', 'competitor disparagement'),
]

# ── Soft prohibited patterns ───────────────────────────────────────────────────
# Vague superlatives replaced with compliant alternatives.

_SOFT_PATTERNS = [
    # "#1" and "number one" marketing claims
    (re.compile(r'#\s*1\b(?!\s*\w)', re.IGNORECASE),
     'top-rated', '#1 claim'),
    (re.compile(r'\bnumber\s+one\b', re.IGNORECASE),
     'a trusted choice', 'number one claim'),

    # "Best X available / on the market" — replace with factual framing
    (re.compile(r'\bthe\s+best\s+\w+(?:\s+\w+)?\s+(?:available|on\s+the\s+market)\b', re.IGNORECASE),
     'an excellent choice', '"best available" superlative'),
    (re.compile(r'\bbest(?:-in-class|-in-industry)?\b', re.IGNORECASE),
     'high-performance', '"best" superlative'),

    # "The clearest / strongest / toughest plastic available"
    (re.compile(r'\bthe\s+(?:clearest|strongest|toughest|hardest|most\s+\w+)\s+plastic\s+available\b', re.IGNORECASE),
     'an exceptionally high-performing plastic', 'unverified superlative'),

    # "Highest quality" without data
    (re.compile(r'\bhighest[\s-]quality\b', re.IGNORECASE),
     'precision-manufactured', '"highest quality" superlative'),

    # "World-class" / "industry-leading"
    (re.compile(r'\bworld[\s-]class\b', re.IGNORECASE),
     'professional-grade', '"world-class" superlative'),
    (re.compile(r'\bindustry[\s-]leading\b', re.IGNORECASE),
     'industry-trusted', '"industry-leading" superlative'),

    # "Unmatched" / "unrivaled" / "unsurpassed"
    (re.compile(r'\b(?:unmatched|unrivaled|unsurpassed|unbeatable)\b', re.IGNORECASE),
     'exceptional', 'unverified superlative'),

    # "The only plastic that..."
    (re.compile(r'\bthe\s+only\s+(?:plastic|material|product)\s+that\b', re.IGNORECASE),
     'a plastic that', 'unverified "the only" claim'),

    # "Perfect for" — Amazon flags this as vague
    (re.compile(r'\bperfect\s+for\b', re.IGNORECASE),
     'ideal for', '"perfect for" superlative'),

    # "Superior" without data
    (re.compile(r'\bsuperior\s+(?:quality|performance|strength|durability|clarity|product|material|plastic)\b', re.IGNORECASE),
     'high-quality', '"superior" superlative'),
    (re.compile(r'\bof\s+superior\s+quality\b', re.IGNORECASE),
     'of exceptional quality', '"superior quality" superlative'),

    # "Top quality" / "top-tier"
    (re.compile(r'\btop[\s-](?:quality|tier|grade|notch)\b', re.IGNORECASE),
     'high-quality', '"top quality" superlative'),

    # "State-of-the-art" / "cutting-edge" / "revolutionary"
    (re.compile(r'\bstate[\s-]of[\s-]the[\s-]art\b', re.IGNORECASE),
     'advanced', '"state-of-the-art" superlative'),
    (re.compile(r'\bcutting[\s-]edge\b', re.IGNORECASE),
     'advanced', '"cutting-edge" superlative'),
    (re.compile(r'\brevolutionary\b', re.IGNORECASE),
     'innovative', '"revolutionary" superlative'),
    (re.compile(r'\bgroundbreaking\b', re.IGNORECASE),
     'innovative', '"groundbreaking" superlative'),
    (re.compile(r'\bbreakthrough\b', re.IGNORECASE),
     'innovative', '"breakthrough" superlative'),

    # "Award-winning" without specifying the award
    (re.compile(r'\baward[\s-]winning\b', re.IGNORECASE),
     'highly rated', '"award-winning" unverified claim'),

    # Germ/bacteria resistance claims (softer form — replace rather than remove)
    (re.compile(r'\bgerm[\s-]resistant\b', re.IGNORECASE),
     'easy to clean', 'germ-resistant claim'),
    (re.compile(r'\bbacteria[\s-]resistant\b', re.IGNORECASE),
     'easy to clean', 'bacteria-resistant claim'),
    (re.compile(r'\bmold[\s-]resistant\b', re.IGNORECASE),
     'easy to clean and maintain', 'mold-resistant claim'),

    # "Proven" as standalone unverified marketing term
    (re.compile(r'\btime[\s-]proven\b', re.IGNORECASE),
     'trusted', '"time-proven" unverified claim'),
    (re.compile(r'\btried[\s-]and[\s-](?:true|proven|tested)\b', re.IGNORECASE),
     'trusted', '"tried-and-proven" unverified claim'),

    # "Ultimate" as superlative
    (re.compile(r'\bthe\s+ultimate\s+(?:\w+\s+){0,3}(?:solution|choice|option|material|plastic)\b', re.IGNORECASE),
     'an excellent choice', '"ultimate" superlative'),
]

# ── Backend search term prohibited words ──────────────────────────────────────
_BACKEND_PROHIBITED = re.compile(
    r'\b(?:best|top|great|cheap|sale|discount|free|guaranteed|'
    r'number\s+one|#1|new|amazing|hot|buy|get|wholesale|clearance)\b',
    re.IGNORECASE
)

_COMPETITOR_BRANDS = re.compile(
    r'\b(?:plexiglas|lucite|lexan|formica|corian|kydex|ultra[\s-]high[\s-]molecular)\b',
    re.IGNORECASE
)


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def validate_and_fix(text: str, field: str = 'text') -> tuple[str, list[str]]:
    """
    Scan text for policy violations, auto-fix what we can, and return
    (cleaned_text, list_of_violation_descriptions).
    """
    if not text:
        return text, []

    violations = []
    clean = text

    for pattern, replacement, label in _HARD_PATTERNS:
        if pattern.search(clean):
            clean = pattern.sub(replacement, clean)
            violations.append(f'[HARD] {field}: {label} — removed')

    for pattern, replacement, label in _SOFT_PATTERNS:
        if pattern.search(clean):
            clean = pattern.sub(replacement, clean)
            violations.append(f'[SOFT] {field}: {label} — replaced with "{replacement}"')

    # Collapse multiple spaces left by removals
    clean = re.sub(r'  +', ' ', clean).strip()

    return clean, violations


def check_backend_terms(terms: str) -> tuple[str, list[str]]:
    """
    Scrub Amazon-prohibited words from backend search terms.
    Returns (clean_terms, violations).
    """
    if not terms:
        return terms, []

    violations = []
    words = terms.split()
    clean_words = []

    for word in words:
        if _BACKEND_PROHIBITED.fullmatch(word):
            violations.append(f'[HARD] backend_terms: prohibited word "{word}" — removed')
        elif _COMPETITOR_BRANDS.search(word):
            violations.append(f'[HARD] backend_terms: competitor brand "{word}" — removed')
        else:
            clean_words.append(word)

    return ' '.join(clean_words), violations


def validate_all_fields(content: dict, mkt_id: str = '') -> tuple[dict, list[str]]:
    """
    Validate and fix all generated fields in a content dict.
    Returns (cleaned_content, all_violations).
    """
    all_violations = []
    clean = dict(content)

    text_fields = ['bullet1', 'bullet2', 'bullet3', 'bullet4', 'bullet5', 'description']
    for field in text_fields:
        if clean.get(field):
            fixed, v = validate_and_fix(clean[field], field)
            clean[field] = fixed
            all_violations.extend(v)

    if clean.get('backend_search_terms'):
        fixed, v = check_backend_terms(clean['backend_search_terms'])
        clean['backend_search_terms'] = fixed
        all_violations.extend(v)

    return clean, all_violations
