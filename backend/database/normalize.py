import re

def normalize_company_name(company_name):
    """Normalize company name for better duplicate detection."""
    if not company_name:
        return ""
    
    normalized = company_name.lower().strip()
    
    # Remove common business suffixes (more comprehensive list)
    business_suffixes = [
        r'\binc\.?$', r'\bincorporated$', r'\bcorp\.?$', r'\bcorporation$',
        r'\bltd\.?$', r'\blimited$', r'\bllc$', r'\bco\.?$', r'\bcompany$',
        r'\benterprises?$', r'\benterprise$', r'\bgroup$', r'\bholdings?$',
        r'\bassociates?$', r'\bpartners?$', r'\bsolutions?$', r'\bservices?$',
        r'\btechnologies$', r'\btechnology$', r'\btech$', r'\bsystems?$',
        r'\bindustries$', r'\bindustrial$', r'\bmanufacturing$', r'\bmfg$'
    ]

    changed = True
    while changed:
        old = normalized
        for suffix in business_suffixes:
            normalized = re.sub(suffix, '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        changed = (normalized != old)

    
    # Handle punctuation more carefully - preserve meaningful separators
    # Replace hyphens and underscores with spaces first
    normalized = re.sub(r'[-_]', ' ', normalized)
    
    # Remove other punctuation except dots (which might be meaningful in tech names)
    normalized = re.sub(r'[^\w\s\.]', ' ', normalized)
    
    # Clean up multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    normalized = re.sub(r'\.', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

def normalize_person_name(person_name):
    """Normalize person name for better duplicate detection."""
    if not person_name:
        return ""
    
    normalized = person_name.lower().strip()
    
    # Remove common person name prefixes and suffixes
    person_name_affixes = [
        r'^\s*mr\.?\s+', r'^\s*mrs\.?\s+', r'^\s*ms\.?\s+', r'^\s*dr\.?\s+',   # prefixes
        r'\s+phd\.?\s*$', r'\s+md\.?\s*$', r'\s+jr\.?\s*$', r'\s+sr\.?\s*$', r'\s+mba\.?\s*$'    # suffixes
    ]

    for affix in person_name_affixes:
        normalized = re.sub(affix, '', normalized)

    
    # Remove punctuation and extra spaces
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized