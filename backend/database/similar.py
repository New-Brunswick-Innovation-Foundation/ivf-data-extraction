from difflib import SequenceMatcher
from database.normalize import normalize_company_name, normalize_person_name
from database.utils import extract_operating_name


def find_similar_companies(new_company, existing_df, similarity_threshold=0.8):
    """Find similar companies and return their IDs."""
    if existing_df.empty:
        return []
    
    new_operating = extract_operating_name(new_company['CompanyName'])
    new_normalized = normalize_company_name(new_operating)
    
    similar_companies = []
    
    for _, existing in existing_df.iterrows():
        existing_operating = extract_operating_name(existing['CompanyName'])
        existing_normalized = normalize_company_name(existing_operating)
        
        similarity = SequenceMatcher(None, new_normalized, existing_normalized).ratio()
        # if 'dot' in existing_normalized:
        #     print('New Normalized:' + new_normalized)
        #     print('Existing Normalized:' + existing_normalized)
        #     print('Similarity: ' + str(similarity))
        
        if similarity >= similarity_threshold:
            similar_companies.append({
                'company_id': existing['CompanyID'],
                'existing_company': existing['CompanyName'],
                'similarity': similarity,
                'address': existing.get('Address', ''),
                'city': existing.get('City', ''),
                'province': existing.get('Province', '')
            })
    
    return sorted(similar_companies, key=lambda x: x['similarity'], reverse=True)



def find_similar_people(new_person, existing_df, similarity_threshold=0.8):
    """Find similar people and return their IDs."""
    if existing_df.empty:
        return []
    
    new_email = (new_person.get('Email') or "").strip().lower()
    new_full_name = new_person['FirstName'] + new_person['LastName']
    new_normalized = normalize_person_name(new_full_name)

    similar_people = []
    
    for _, existing in existing_df.iterrows():
        existing_email = (existing.get('Email') or "").strip().lower()
        existing_full_name = existing['FirstName'] + existing['LastName']
        existing_normalized = normalize_person_name(existing_full_name)

        if new_email and existing_email and new_email == existing_email:
            similarity = 1.0
        else:
            similarity = SequenceMatcher(None, new_normalized, existing_normalized).ratio()
        
        if similarity >= similarity_threshold:
            similar_people.append({
                'person_id': existing['PersonID'],  # Store the ID!
                'existing_last_name': existing['LastName'],
                'existing_first_name': existing['FirstName'],
                'similarity': similarity,
                'email': existing.get('Email', ''),
            })
    
    return sorted(similar_people, key=lambda x: x['similarity'], reverse=True)