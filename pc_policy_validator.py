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
     'ISO 9001:2015 certified quality', '"highest quality" superlative'),

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
