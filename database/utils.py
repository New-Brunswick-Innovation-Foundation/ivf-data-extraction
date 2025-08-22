import re

def extract_operating_name(company_name):
    """Extract the main operating name from complex company descriptions."""
    if not company_name:
        return company_name
    
    operating_patterns = [
        r'operating\s+business\s+name:\s*(.+?)(?:,|$)',
        r'dba\s*(.+?)(?:,|$)',
        r'doing\s+business\s+as\s*(.+?)(?:,|$)',
        r'operating\s+as\s*(.+?)(?:,|$)'
    ]
    
    for pattern in operating_patterns:
        match = re.search(pattern, company_name, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return company_name