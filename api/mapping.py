from datetime import datetime
from api.utils import choose_region
import os
import json

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

def map_city_to_region(city):
    json_path = 'city_to_region_mapping.json' # Adjust path if necessary
    
    # 1. Load the current mapping from the file
    if os.path.exists(json_path):
        with open(json_path, 'r') as file:
            mapping = json.load(file)
    else:
        mapping = {}

    normalized_city = city.strip().title()

    # 2. Check if the city exists
    if normalized_city in mapping:
        return mapping[normalized_city]
    
    # 3. If it doesn't exist, ask the user and save it immediately!
    else:
        region = choose_region(city) # Your existing prompt function
        
        # Add the new city to our dictionary
        mapping[normalized_city] = region 
        
        # Write the updated dictionary back to the JSON file
        with open(json_path, 'w') as file:
            json.dump(mapping, file, indent=4)
            
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