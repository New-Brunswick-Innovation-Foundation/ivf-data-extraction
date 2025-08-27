from datetime import datetime

def map_selector_of_research(selector_of_research_task, sector_mapping):
    data = selector_of_research_task[0].get("data", {})

    for field in data.values():
        label = field.get("label")
        response = field.get("response")
        if response is not None and label in sector_mapping:
            sector_list = sector_mapping[label]
            if 0 <= response < len(sector_list):
                return sector_list[response]
    
    return None  #for when no valid mapping was found

def map_city_to_region(city, city_to_region_mapping):
    normalized_city = city.strip().title()  # Normalize formatting
    if normalized_city in city_to_region_mapping:
        return city_to_region_mapping[normalized_city]
    else:
        region = input(f"Enter region for city '{city}' (NE/NW/SE/SW): ").strip().upper()
        while region not in ["NE", "NW", "SE", "SW"]:
            region = input("Invalid input. Please enter NE, NW, SE, or SW: ").strip().upper()
        city_to_region_mapping[normalized_city] = region  # Save it for future use
        return region

def map_province(province_index, province_mapping, company_name):
    if province_index in province_mapping:
        return province_mapping[province_index]
    else:
        return input(f"Enter province for company '{company_name}' (NB, NS, etc.): ").strip().upper()

def map_fiscal_year(fiscal_year):
    if fiscal_year == '2024':
        return '2023-2024'
    elif fiscal_year == '2023':
        return '2022-2023'
    else:
        return fiscal_year
    
def map_decision_date(decision_date):
    if not decision_date or str(decision_date).strip() == "":
        return None
    
    decision_date = decision_date.strip()
    date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]
    
    for format in date_formats:
        try:
            return datetime.strptime(decision_date, format).replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(decision_date)
    except ValueError:
        return None