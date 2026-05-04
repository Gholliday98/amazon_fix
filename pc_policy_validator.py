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

    # Implied quality guarantee / process claims (Amazon 99300 violations)
    (re.compile(r'\binspected\s+before\s+shipment\b', re.IGNORECASE),
     'shipped', 'implied inspection guarantee'),
    (re.compile(r'\btrusted\s+(?:across|by|among|throughout)\b', re.IGNORECASE),
     'established', 'promotional trust claim'),
    (re.compile(r'\btrusted\b(?=\s+(?:supplier|partner|source|brand|name))', re.IGNORECASE),
     'established', 'promotional trust claim'),
    (re.compile(r'\bmachines?\s+cleanly\b', re.IGNORECASE),
     'easy to machine', 'unverifiable machining claim'),
    (re.compile(r'\bcuts?\s+cleanly\b', re.IGNORECASE),
     'easy to cut', 'unverifiable condition claim'),
    (re.compile(r'\bships?\s+clean(?:ly)?\b', re.IGNORECASE),
     '', 'unverifiable condition claim'),

    # Food service context — implies food-grade/food-safe without certification
    (re.compile(r'\bfood[\s-]service\s+(?:operations?|equipment|applications?|environments?|use|industry)\b', re.IGNORECASE),
     'commercial operations', 'implicit food-grade claim'),
    (re.compile(r'\bfood[\s-]service\b', re.IGNORECASE),
     'commercial', 'implicit food-grade claim'),
    (re.compile(r'\bfood[\s-]prep\s+(?:environments?|stations?|areas?|surfaces?|applications?)\b', re.IGNORECASE),
     'prep environments', 'implicit food-grade claim'),
    (re.compile(r'\binstitutional\s+food\s+prep\b', re.IGNORECASE),
     'commercial prep', 'implicit food-grade claim'),
    (re.compile(r'\bbutcher\s+(?:counters?|shops?|blocks?|stations?)\b', re.IGNORECASE),
     'work surfaces', 'implicit food-grade claim'),
    (re.compile(r'\brestaurant\s+kitchen[s]?\b', re.IGNORECASE),
     'commercial kitchens', 'implicit food-grade claim'),
    (re.compile(r'\bcatering\s+(?:facilities|operations?|environments?|applications?)\b', re.IGNORECASE),
     'commercial facilities', 'implicit food-grade claim'),
    (re.compile(r'\bfood\s+(?:processing|production)\s+(?:facilities|environments?|applications?)\b', re.IGNORECASE),
     'industrial facilities', 'implicit food-grade claim'),
    (re.compile(r'\bfood\s+safety\s+standards?\b', re.IGNORECASE),
     'industry standards', 'implicit food-safe claim'),
    (re.compile(r'\bstrictest\s+food\b', re.IGNORECASE),
     'industry', 'implicit food-safe claim'),

    # Certification claims without verification
    (re.compile(r'\bAS9100[A-Z]?\b', re.IGNORECASE),
     '', 'unverified AS9100 certification claim'),

    # Logistics / shipping info in listing content
    (re.compile(r'\bstocked\s+and\s+ships?\s+from\s+[\w\s,]+(?:\.|$)', re.IGNORECASE),
     '', 'shipping info in listing content'),
    (re.compile(r'\bships?\s+from\s+[\w\s,]+(?:\.|$)', re.IGNORECASE),
     '', 'shipping info in listing content'),

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
    (re.compile(r'\bISO[\s-]?\d{3,5}(?::\d{4})?\b', re.IGNORECASE),
     '', 'unverified ISO certification claim'),
    (re.compile(r'\bISO[\s-]?(?:certified|compliant|approved|listed|standard)\b', re.IGNORECASE),
     '', 'unverified ISO certification claim'),
    (re.compile(r'\bASTM\b', re.IGNORECASE),
     '', 'unverified ASTM standard reference'),

    # Clarity / appearance claims (unverifiable without lab data)
    (re.compile(r'\boptical\s+clarity\b', re.IGNORECASE),
     'visual clarity', 'optical clarity claim'),
    (re.compile(r'\bcrystal[\s-]?clear\b', re.IGNORECASE),
     'clear', 'unverified clarity claim'),
    (re.compile(r'\bcrystal[\s-]?clarity\b', re.IGNORECASE),
     'clarity', 'unverified clarity claim'),
    (re.compile(r'\bglass[\s-]?clear\b', re.IGNORECASE),
     'clear', 'unverified clarity claim'),
    (re.compile(r'\bglass[\s-]?like\s+clarity\b', re.IGNORECASE),
     'high clarity', 'unverified clarity claim'),
    (re.compile(r'\bwater[\s-]?clear\b', re.IGNORECASE),
     'clear', 'unverified clarity claim'),
    (re.compile(r'\bdiamond[\s-]?clear\b', re.IGNORECASE),
     'clear', 'unverified clarity claim'),
    (re.compile(r'\boptically[\s-]?clear\b', re.IGNORECASE),
     'transparent', 'unverified optical claim'),
    (re.compile(r'\bflawless\b', re.IGNORECASE),
     '', 'unverifiable appearance claim'),
    (re.compile(r'\bpristine\b', re.IGNORECASE),
     '', 'unverifiable appearance claim'),
    (re.compile(r'\bimmaculate\b', re.IGNORECASE),
     '', 'unverifiable appearance claim'),
    (re.compile(r'\bblemish[\s-]free\b', re.IGNORECASE),
     '', 'unverifiable appearance claim'),
    (re.compile(r'\bspotless\b', re.IGNORECASE),
     '', 'unverifiable appearance claim'),

    # Safety claims (require certification)
    (re.compile(r'\bsafe\s+(?:to\s+use|for\s+use|around|with|for\s+children|for\s+pets)\b', re.IGNORECASE),
     '', 'unverified safety claim'),
    (re.compile(r'\bhygienic\b', re.IGNORECASE),
     '', 'unverified hygiene claim'),
    (re.compile(r'\bsanitary\b', re.IGNORECASE),
     '', 'unverified sanitary claim'),
    (re.compile(r'\bodor[\s-]?(?:free|resistant|proof|reducing|eliminating)\b', re.IGNORECASE),
     '', 'unverified odor claim'),
    (re.compile(r'\bstain[\s-]?(?:resistant|proof|free|repellent)\b', re.IGNORECASE),
     '', 'unverified stain claim'),

    # Strength / durability claims without specs
    (re.compile(r'\bunbreakable\b', re.IGNORECASE),
     'impact resistant', 'unverifiable strength claim'),
    (re.compile(r'\bindestructible\b', re.IGNORECASE),
     'highly durable', 'unverifiable strength claim'),
    (re.compile(r'\bshatterproof\b', re.IGNORECASE),
     'impact resistant', 'unverified shatterproof claim'),
    (re.compile(r'\bbullet[\s-]?(?:proof|resistant)\b', re.IGNORECASE),
     '', 'unverified ballistic claim'),

    # Craftsmanship / selection claims
    (re.compile(r'\bhand[\s-]?(?:picked|selected|crafted|finished|made)\b', re.IGNORECASE),
     '', 'unverifiable craftsmanship claim'),
    (re.compile(r'\bcarefully[\s-]?(?:selected|crafted|inspected|sourced)\b', re.IGNORECASE),
     '', 'unverifiable process claim'),
    (re.compile(r'\bmeticulously\b', re.IGNORECASE),
     '', 'unverifiable process claim'),
    (re.compile(r'\bpainstakingly\b', re.IGNORECASE),
     '', 'unverifiable process claim'),
    (re.compile(r'\battention\s+to\s+detail\b', re.IGNORECASE),
     '', 'unverifiable process claim'),

    # Comparison / competitive claims
    (re.compile(r'\bunlike\s+(?:others?|competitors?|other\s+brands?)\b', re.IGNORECASE),
     '', 'competitor comparison claim'),
    (re.compile(r'\bcompared\s+to\s+other\b', re.IGNORECASE),
     '', 'competitor comparison claim'),

    # "Pure" as marketing claim
    (re.compile(r'\bpure\s+(?:clarity|quality|performance|plastic|material)\b', re.IGNORECASE),
     '', 'unverifiable purity claim'),

    # Percentage claims without context
    (re.compile(r'\b\d+\s*%\s*(?:effective|efficient|pure|clear|stronger|better)\b', re.IGNORECASE),
     '', 'unverified percentage claim'),

    # "Approved" in any context (implies regulatory approval)
    (re.compile(r'\b(?:widely\s+)?approved\s+(?:for|by|in)\b', re.IGNORECASE),
     '', 'implied regulatory approval'),
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

    # Restricted / hazardous chemicals
    (re.compile(r'\bmethylene[\s-]chloride\b', re.IGNORECASE),
     '', 'restricted chemical (methylene chloride)'),
    (re.compile(r'\bdichloromethane\b', re.IGNORECASE),
     '', 'restricted chemical (dichloromethane / methylene chloride)'),
]

# ── Soft prohibited patterns ───────────────────────────────────────────────────
# Vague superlatives replaced with compliant alternatives.

_SOFT_PATTERNS = [
    # "#1" and "number one" marketing claims
    (re.compile(r'#\s*1\b(?!\s*\w)', re.IGNORECASE),
     'top-rated', '#1 claim'),
    (re.compile(r'\bnumber\s+one\b', re.IGNORECASE),
     'a leading choice', 'number one claim'),

    # "Best X available / on the market" — replace with factual framing
    (re.compile(r'\bthe\s+best\s+\w+(?:\s+\w+)?\s+(?:available|on\s+the\s+market)\b', re.IGNORECASE),
     'a strong choice', '"best available" superlative'),
    (re.compile(r'\bbest(?:-in-class|-in-industry)?\b', re.IGNORECASE),
     'high-performance', '"best" superlative'),

    # "The clearest / strongest / toughest plastic available"
    (re.compile(r'\bthe\s+(?:clearest|strongest|toughest|hardest|most\s+\w+)\s+plastic\s+available\b', re.IGNORECASE),
     'a high-performing plastic', 'unverified superlative'),

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
     'high-performance', 'unverified superlative'),

    # "Significant" / "significantly" — vague intensifier without data
    (re.compile(r'\bsignificant(?:ly)?\b', re.IGNORECASE),
     '', 'vague intensifier'),

    # "Exceptional" — superlative without data
    (re.compile(r'\bexceptional(?:ly)?\b', re.IGNORECASE),
     'high', 'unverified superlative'),

    # "Outstanding" — superlative without data
    (re.compile(r'\boutstanding\b', re.IGNORECASE),
     'strong', 'unverified superlative'),

    # "Remarkable" — superlative without data
    (re.compile(r'\bremarkable(?:ly)?\b', re.IGNORECASE),
     '', 'unverified superlative'),

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
     'of high quality', '"superior quality" superlative'),

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

    # "The only" claims — broadened to catch "the only common/known/true X"
    (re.compile(r'\bthe\s+only\s+(?:\w+\s+){0,3}(?:plastic|material|product|polymer)\b', re.IGNORECASE),
     'a high-performance plastic', '"the only" unverified claim'),

    # Unverifiable quantitative performance claims
    (re.compile(r'\bmillions?\s+of\s+times\b', re.IGNORECASE),
     'repeatedly', 'unverifiable quantitative claim'),
    (re.compile(r'\bbillions?\s+of\s+(?:times|cycles)\b', re.IGNORECASE),
     'repeatedly', 'unverifiable quantitative claim'),

    # Freight / LTL shipping info in listing content (remove entirely)
    (re.compile(r'FREIGHT\s+SHIPPING\s+NOTICE.*?(?=\n\n|\Z)', re.IGNORECASE | re.DOTALL),
     '', 'freight notice in listing'),
    (re.compile(r'\b(?:ships?|shipped|shipping)\s+(?:via|by|using)\s+(?:freight|LTL|truck|common\s+carrier)\b', re.IGNORECASE),
     '', 'freight shipping reference'),
    (re.compile(r'\bLTL\s+(?:freight|shipping|delivery|carrier)\b', re.IGNORECASE),
     '', 'LTL freight reference'),
    (re.compile(r'\bfreight\s+(?:shipping|delivery|carrier|required|only)\b', re.IGNORECASE),
     '', 'freight reference in listing'),
    (re.compile(r'\boversized\s+(?:item|product|shipment|order)\b', re.IGNORECASE),
     '', 'freight/oversize reference in listing'),

    # All-caps section headers in description
    (re.compile(r'\bCOMMON\s+QUESTIONS\s*:', re.IGNORECASE),
     'Common Questions:', 'all-caps header'),

    # "Proven" as standalone unverified marketing term
    (re.compile(r'\btime[\s-]proven\b', re.IGNORECASE),
     'established', '"time-proven" unverified claim'),
    (re.compile(r'\btried[\s-]and[\s-](?:true|proven|tested)\b', re.IGNORECASE),
     'established', '"tried-and-proven" unverified claim'),

    # "Ultimate" as superlative
    (re.compile(r'\bthe\s+ultimate\s+(?:\w+\s+){0,3}(?:solution|choice|option|material|plastic)\b', re.IGNORECASE),
     'a solid choice', '"ultimate" superlative'),

    # "Ideal" as unverifiable superlative
    (re.compile(r'\bideal\s+for\b', re.IGNORECASE),
     'suitable for', '"ideal for" superlative'),

    # "Excellent" as unverifiable superlative
    (re.compile(r'\bexcellent\b', re.IGNORECASE),
     'strong', '"excellent" superlative'),

    # "Premium" — very commonly flagged by Amazon
    # Catch "premium quality" first to avoid "quality quality" double
    (re.compile(r'\bpremium[\s-]quality\b', re.IGNORECASE),
     'high quality', '"premium quality" superlative'),
    (re.compile(r'\bpremium\b', re.IGNORECASE),
     'quality', '"premium" superlative'),

    # "Superior" standalone (not just "superior quality/performance")
    (re.compile(r'\bsuperior\b', re.IGNORECASE),
     'high-quality', '"superior" superlative'),

    # "Unparalleled"
    (re.compile(r'\bunparalleled\b', re.IGNORECASE),
     'high-performance', 'unverified superlative'),

    # "Top of the line"
    (re.compile(r'\btop[\s-]of[\s-]the[\s-]line\b', re.IGNORECASE),
     'high-quality', '"top of the line" superlative'),

    # "Finest"
    (re.compile(r'\bfinest\b', re.IGNORECASE),
     'quality', '"finest" superlative'),

    # "Elite" / "luxury" / "deluxe"
    (re.compile(r'\belite\b', re.IGNORECASE),
     '', '"elite" superlative'),
    (re.compile(r'\bluxur(?:y|ious)\b', re.IGNORECASE),
     '', '"luxury" superlative'),
    (re.compile(r'\bdeluxe\b', re.IGNORECASE),
     '', '"deluxe" superlative'),

    # "Second to none" / "like no other"
    (re.compile(r'\bsecond\s+to\s+none\b', re.IGNORECASE),
     '', 'unverified superlative'),
    (re.compile(r'\blike\s+no\s+other\b', re.IGNORECASE),
     '', 'unverified superlative'),
    (re.compile(r'\bnothing\s+compares\b', re.IGNORECASE),
     '', 'unverified superlative'),

    # "Must-have" / "game-changer"
    (re.compile(r'\bmust[\s-]have\b', re.IGNORECASE),
     '', 'marketing hype'),
    (re.compile(r'\bgame[\s-]chang(?:er|ing)\b', re.IGNORECASE),
     '', 'marketing hype'),
    (re.compile(r'\blife[\s-]changing\b', re.IGNORECASE),
     '', 'marketing hype'),
    (re.compile(r'\bone[\s-]of[\s-]a[\s-]kind\b', re.IGNORECASE),
     '', 'unverified uniqueness claim'),

    # "Go-to" marketing language
    (re.compile(r'\bgo[\s-]to\s+(?:choice|option|solution|source|material|product)\b', re.IGNORECASE),
     'practical choice', '"go-to" marketing language'),

    # "Rigorous" — unverified quality claim
    (re.compile(r'\brigorous(?:ly)?\b', re.IGNORECASE),
     'thorough', '"rigorous" unverified claim'),

    # "Virtually" as vague intensifier
    (re.compile(r'\bvirtually\s+(?:zero|no|any|unlimited|indestructible|impervious)\b', re.IGNORECASE),
     'extremely low', '"virtually" vague claim'),

    # "Engineered for" — unverified engineering claim
    (re.compile(r'\bengineered\s+for\b', re.IGNORECASE),
     'designed for', '"engineered for" unverified claim'),

    # "Ensuring" as guarantee language
    (re.compile(r'\bensuring\b', re.IGNORECASE),
     'providing', '"ensuring" guarantee language'),

    # "Performs reliably" / "reliable quality/fit/performance"
    (re.compile(r'\bperforms?\s+reliably\b', re.IGNORECASE),
     'performs consistently', 'unverified reliability claim'),
    (re.compile(r'\breliable\s+(?:quality|fit|performance|results?|material|consistency)\b', re.IGNORECASE),
     'consistent', 'unverified reliability claim'),

    # "Optimum" / "optimal"
    (re.compile(r'\boptim(?:um|al)\b', re.IGNORECASE),
     'high', '"optimum/optimal" superlative'),

    # "Proven" standalone as marketing term
    (re.compile(r'\bproven\s+(?:performance|results?|quality|durability|strength)\b', re.IGNORECASE),
     'established', '"proven" unverified claim'),

    # Unverifiable emotional/marketing language
    (re.compile(r'\bincredible\b', re.IGNORECASE),
     '', 'unverifiable marketing claim'),
    (re.compile(r'\bamazing\b', re.IGNORECASE),
     '', 'unverifiable marketing claim'),
    (re.compile(r'\bfantastic\b', re.IGNORECASE),
     '', 'unverifiable marketing claim'),
    (re.compile(r'\bwonderful\b', re.IGNORECASE),
     '', 'unverifiable marketing claim'),
    (re.compile(r'\bspectacular\b', re.IGNORECASE),
     '', 'unverifiable marketing claim'),
    (re.compile(r'\bsuperb\b', re.IGNORECASE),
     '', 'unverifiable marketing claim'),
    (re.compile(r'\bwow\b', re.IGNORECASE),
     '', 'unverifiable marketing claim'),

    # "Popular" as unverified claim
    (re.compile(r'\bpopular\s+(?:choice|option|product|material|plastic)\b', re.IGNORECASE),
     'widely used', '"popular" unverified claim'),

    # Durability promises without data
    (re.compile(r'\bbuilt\s+to\s+last\b', re.IGNORECASE),
     'durable', '"built to last" unverified claim'),
    (re.compile(r'\blong[\s-]lasting\b', re.IGNORECASE),
     'durable', '"long-lasting" unverified claim'),
    (re.compile(r'\blasts\s+(?:longer|years|a\s+lifetime|forever)\b', re.IGNORECASE),
     'durable', 'durability promise'),
    (re.compile(r'\boutlasts\b', re.IGNORECASE),
     'outlasts', 'comparative durability claim'),

    # Appeal to emotion / peace of mind language
    (re.compile(r'\bpeace\s+of\s+mind\b', re.IGNORECASE),
     '', 'emotional appeal'),
    (re.compile(r'\bworry[\s-]free\b', re.IGNORECASE),
     '', 'unverified claim'),
    (re.compile(r'\bno[\s-]hassle\b', re.IGNORECASE),
     '', 'unverified claim'),
    (re.compile(r'\bhassle[\s-]free\b', re.IGNORECASE),
     '', 'unverified claim'),
    (re.compile(r'\beffortless(?:ly)?\b', re.IGNORECASE),
     '', 'unverified claim'),
    (re.compile(r'\bconfidence\s+(?:knowing|that|in|you)\b', re.IGNORECASE),
     '', 'emotional appeal'),

    # "You can rely/count/depend on" — trust without evidence
    (re.compile(r'\byou\s+can\s+(?:rely|count|depend)\s+on\b', re.IGNORECASE),
     '', 'unverified trust claim'),
    (re.compile(r'\byou\'?ll\s+(?:love|enjoy|appreciate)\b', re.IGNORECASE),
     '', 'marketing appeal to emotion'),

    # "Perfect" standalone as superlative
    (re.compile(r'\bperfect\b', re.IGNORECASE),
     'suitable', '"perfect" superlative'),

    # "Ideal" standalone (we catch "ideal for" already but not standalone)
    (re.compile(r'\bideal\b', re.IGNORECASE),
     'suitable', '"ideal" superlative'),

    # "Innovative" — we use as replacement but ban as original marketing word
    (re.compile(r'\binnovative\b', re.IGNORECASE),
     'functional', '"innovative" unverified claim'),

    # "Advanced" as vague marketing (when not describing a spec)
    (re.compile(r'\bmost\s+advanced\b', re.IGNORECASE),
     'highly functional', '"most advanced" superlative'),

    # "Heavy-duty" as unverifiable marketing claim
    (re.compile(r'\bheavy[\s-]duty\b', re.IGNORECASE),
     'industrial-grade', '"heavy-duty" unverified claim'),

    # "Commercial-grade" / "industrial-grade" without certification
    (re.compile(r'\bcommercial[\s-]grade\b', re.IGNORECASE),
     'commercial quality', 'unverified grade claim'),

    # "Exceed" expectations type claims
    (re.compile(r'\bexceeds?\s+(?:expectations?|standards?|requirements?)\b', re.IGNORECASE),
     'meets', 'unverifiable performance claim'),

    # "Specially designed/formulated/engineered"
    (re.compile(r'\bspecially\s+(?:designed|formulated|engineered|crafted|treated)\b', re.IGNORECASE),
     'designed', 'unverifiable process claim'),

    # "Preferred by" professionals — unverifiable endorsement
    (re.compile(r'\bpreferred\s+by\b', re.IGNORECASE),
     'used by', 'unverified preference claim'),

    # "Trusted by" — unverifiable
    (re.compile(r'\btrusted\s+by\b', re.IGNORECASE),
     'used by', 'unverified trust claim'),

    # "Widely recognized" — unverifiable
    (re.compile(r'\bwidely\s+(?:recognized|regarded|considered|known)\b', re.IGNORECASE),
     'commonly', 'unverifiable recognition claim'),

    # "The right choice" / "smart choice" — marketing
    (re.compile(r'\b(?:the\s+)?(?:right|smart|wise|obvious)\s+choice\b', re.IGNORECASE),
     'a practical choice', 'marketing claim'),

    # "Never" + performance claim (unverifiable absolute) — catch "never warps or cracks" as unit
    (re.compile(r'\bnever\s+(?:warps?|cracks?|fades?|rusts?|rots?|splinters?|chips?)(?:\s+or\s+(?:warps?|cracks?|fades?|rusts?|rots?|splinters?|chips?))*\b', re.IGNORECASE),
     'resists damage', 'unverifiable absolute performance claim'),
]

# ── Backend search term prohibited words ──────────────────────────────────────
# Word-level: matched against each whitespace-separated token (lowercased, punctuation stripped).
_BACKEND_PROHIBITED_WORDS = re.compile(
    r'^(?:'
    r'best|better|greatest|finest|ultimate|premier|superior|supreme'
    r'|unmatched|unrivaled|unsurpassed|unbeatable|unparalleled'
    r'|exceptional|outstanding|remarkable|incredible|amazing|fantastic'
    r'|wonderful|spectacular|superb|awesome|excellent|extraordinary|phenomenal'
    r'|free|cheap|cheaper|cheapest|sale|sales|discount|discounted|discounts'
    r'|clearance|bargain|deal|deals|offer|offers|wholesale|affordable|budget'
    r'|limited|exclusive|rare|must-have|hurry|urgent'
    r'|buy|order|shop|purchase|grab|click'
    r'|number-one|top-rated|award-winning|world-class|industry-leading'
    r'|guaranteed|guarantee|guarantees|warranty|warranted'
    r'|very|really|extremely|absolutely|totally|completely'
    r')$',
    re.IGNORECASE
)

# Competitor brand names — matched against each token using \b anchors
_COMPETITOR_BRANDS = re.compile(
    r'\b(?:'
    r'plexiglas(?:s)?|lucite|perspex|acrylite|optix|duoplex|polycast|altuglas'
    r'|altuclear|deglas|evocryl'
    r'|lexan|makrolon|tuffak|palglas|palsun|palgreen|hygard'
    r'|kydex|formica|corian|wilsonart'
    r'|stok|norstok'
    r'|palram|vivak|vekaplan|simona'
    r')\b',
    re.IGNORECASE
)

# Phrase-level patterns for backend terms (match against entire string, not per-word)
_BACKEND_PHRASE_PATTERNS = [
    (re.compile(r'\bhttps?://\S+', re.IGNORECASE),          'URL'),
    (re.compile(r'\bwww\.\S+', re.IGNORECASE),              'URL'),
    (re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'), 'email address'),
    (re.compile(r'\b\d{3}[\s.\-]?\d{3}[\s.\-]?\d{4}\b'),   'phone number'),
    (re.compile(r'[®™©]'),                                   'trademark symbol'),
    (re.compile(r'<[^>]+>'),                                 'HTML tag'),
    # ASIN patterns
    (re.compile(r'\bB0[A-Z0-9]{8}\b'),                       'ASIN reference'),
    # Competitor ASIN-like codes
    (re.compile(r'\b(?:number\s+one|#\s*1)\b', re.IGNORECASE), 'promotional claim'),
]


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

    # Normalize all-caps section headers to Title Case first
    def _title_header(m):
        return m.group(1).title() + m.group(2)
    new_clean = re.sub(r'([A-Z][A-Z\s&/\-]{3,}[A-Z])(\s*[—:\-])', _title_header, clean)
    if new_clean != clean:
        clean = new_clean
        violations.append(f'[SOFT] {field}: all-caps header — normalized to title case')

    for pattern, replacement, label in _HARD_PATTERNS:
        if pattern.search(clean):
            clean = pattern.sub(replacement, clean)
            violations.append(f'[HARD] {field}: {label} — removed')

    for pattern, replacement, label in _SOFT_PATTERNS:
        if pattern.search(clean):
            clean = pattern.sub(replacement, clean)
            violations.append(f'[SOFT] {field}: {label} — replaced with "{replacement}"')

    # Post-processing cleanup
    clean = re.sub(r'\b(\w+)\s+\1\b', r'\1', clean, flags=re.IGNORECASE)  # doubled words
    clean = re.sub(r'\band\s+(?:certified|compliant|approved)\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\s+,', ',', clean)          # space before comma
    clean = re.sub(r',\s*,+', ',', clean)        # double commas
    clean = re.sub(r',\s*\.', '.', clean)        # comma before period
    clean = re.sub(r'\(\s*\)', '', clean)        # empty parentheses
    clean = re.sub(r'  +', ' ', clean)           # multiple spaces
    clean = re.sub(r' \.', '.', clean)           # space before period
    clean = re.sub(r'\.\.+', '.', clean)          # double periods
    # Remove comma immediately after a sentence-opening word (orphaned from removed content)
    clean = re.sub(
        r'\b(This|The|A|An|Our|Your|Its|These|Those|It|When|For|In|At|By|With|As|And|But|Also)\s*,',
        r'\1', clean, flags=re.IGNORECASE)
    # Remove orphaned verb+pronoun before period ("gives you.", "helps us.", etc.)
    clean = re.sub(r'\b\w+\s+(?:you|them|us|it)\s*\.', '.', clean, flags=re.IGNORECASE)
    # Remove orphaned comma before a single word before period ("durable, products." → "durable products.")
    clean = re.sub(r',\s*(\w+)\.', r' \1.', clean)
    # Remove very short orphaned fragments between periods (". word." or ". word word.")
    clean = re.sub(r'\.\s+[A-Z]?[a-z]+(?:\s+[a-z]+)?\s*\.', '.', clean)
    # Remove orphaned conjunctions/prepositions before period
    clean = re.sub(r'\b(?:and|or|but|with|for|of|in|on|at|to|a|an|the)\s*\.', '.', clean, flags=re.IGNORECASE)
    clean = re.sub(r'  +', ' ', clean)           # final space collapse
    clean = re.sub(r' \.', '.', clean)           # catch any new space-before-period
    clean = re.sub(r'\.\.+', '.', clean)         # catch any new double periods
    clean = re.sub(r'\n{3,}', '\n\n', clean).strip()

    return clean, violations


def check_backend_terms(terms: str) -> tuple[str, list[str]]:
    """
    Scrub Amazon-prohibited words/phrases from backend search terms.
    Returns (clean_terms, violations).

    Strips:
    - Competitor brand names (Plexiglas, Lexan, STOK, etc.)
    - Promotional / marketing words (best, free, sale, discount, etc.)
    - Trademark symbols (® ™ ©)
    - Contact info (URLs, emails, phone numbers)
    - HTML tags
    - ASIN references
    - Duplicate tokens (case-insensitive)
    - Truncates to 249 bytes if needed
    """
    if not terms:
        return terms, []

    violations = []
    clean = terms

    # Phase 1: phrase-level patterns (apply to whole string before tokenizing)
    for pattern, label in _BACKEND_PHRASE_PATTERNS:
        if pattern.search(clean):
            clean = pattern.sub('', clean)
            violations.append(f'[HARD] backend_terms: {label} — removed')

    # Collapse extra whitespace from removals
    clean = re.sub(r'\s+', ' ', clean).strip()

    # Phase 2: token-level filtering
    seen_lower: set[str] = set()
    clean_words = []

    for token in clean.split():
        # Strip leading/trailing punctuation for the check, but keep original form
        bare = token.strip('.,;:!?()[]{}"\'-_/')
        bare_lower = bare.lower()

        if not bare_lower:
            continue

        # Deduplicate (case-insensitive)
        if bare_lower in seen_lower:
            violations.append(f'[SOFT] backend_terms: duplicate term "{token}" — removed')
            continue

        # Check against prohibited word list (exact token match)
        if _BACKEND_PROHIBITED_WORDS.fullmatch(bare_lower):
            violations.append(
                f'[HARD] backend_terms: prohibited word "{token}" — removed')
            continue

        # Check for competitor brand names embedded in the token
        if _COMPETITOR_BRANDS.search(bare_lower):
            violations.append(
                f'[HARD] backend_terms: competitor brand "{token}" — removed')
            continue

        # Drop lone single-letter tokens (noise), keep numbers and 2+ char words
        if len(bare_lower) == 1 and not bare_lower.isdigit():
            continue

        seen_lower.add(bare_lower)
        clean_words.append(token)

    clean = ' '.join(clean_words)

    # Phase 3: enforce 249-byte limit
    encoded = clean.encode('utf-8')
    if len(encoded) > 249:
        # Trim at byte boundary, then strip any incomplete token at the end
        truncated = encoded[:249].decode('utf-8', errors='ignore').rstrip()
        # Don't leave a trailing partial word
        last_space = truncated.rfind(' ')
        if last_space > 0:
            truncated = truncated[:last_space]
        clean = truncated
        violations.append(
            f'[HARD] backend_terms: exceeded 249 bytes — truncated to {len(clean.encode())} bytes')

    return clean, violations


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
