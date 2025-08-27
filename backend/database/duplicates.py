from database.similar import find_similar_companies, find_similar_people
import pandas as pd


def handle_company_duplicates(df, existing_df, interactive=True, similarity_threshold=0.8):
    """
    Handle potential company duplicates with corrected logic.
    
    Logic:
    - No matches above threshold → INSERT as new
    - Matches above threshold → Interactive review or auto-skip if non-interactive
    """
    insert_companies = []
    skip_companies = []
    update_companies = []
    
    for _, new_company in df.iterrows():
        similar = find_similar_companies(new_company, existing_df, similarity_threshold)
        
        if not similar:
            # No matches above threshold found - INSERT as new company
            print(f"✅ Auto-inserting (no matches above {similarity_threshold}): '{new_company['CompanyName']}'")
            insert_companies.append(new_company)
            continue
        
        # Found similar companies above threshold
        if interactive:
            # Show for interactive review
            print(f"\n🔍 Potential duplicate found for: '{new_company['CompanyName']}'")
            print(f"   Address: {new_company.get('Address', 'N/A')}, {new_company.get('City', 'N/A')}")
            
            for i, match in enumerate(similar[:3], 1):
                print(f"   {i}. '{match['existing_company']}' (similarity: {match['similarity']:.2f}) [ID: {match['company_id']}]")
                print(f"      Address: {match['address']}, {match['city']}")
            
            choice_made = False
            while not choice_made:
                choice = input("\nWhat would you like to do?\n"
                             "1. Insert as new company\n"
                             "2. Skip (it's a duplicate)\n"
                             "3. Update existing record\n"
                             "4. Show more details\n"
                             "Enter choice (1-4): ").strip()
                
                if choice == '1':
                    insert_companies.append(new_company)
                    choice_made = True
                elif choice == '2':
                    # User confirms it's a duplicate - skip and store reference
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nWhich existing company is this a duplicate of?")
                        for i, match in enumerate(similar[:3], 1):
                            print(f"   {i}. '{match['existing_company']}' [ID: {match['company_id']}]")
                        
                        while True:
                            try:
                                selection = int(input("Enter number (1-{}): ".format(min(3, len(similar)))).strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection-1]
                                    break
                                else:
                                    print("Invalid selection. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                    
                    skip_record = new_company.copy()
                    skip_record['_matched_existing_id'] = selected_match['company_id']
                    skip_record['_matched_existing_name'] = selected_match['existing_company']
                    skip_companies.append(skip_record)
                    choice_made = True
                elif choice == '3':
                    # Update existing record
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nSelect which existing company to update:")
                        for i, match in enumerate(similar[:3], 1):
                            print(f"   {i}. '{match['existing_company']}' [ID: {match['company_id']}]")
                        
                        while True:
                            try:
                                selection = int(input("Enter number (1-{}): ".format(min(3, len(similar)))).strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection-1]
                                    break
                                else:
                                    print("Invalid selection. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                    
                    update_record = new_company.copy()
                    update_record['_update_target_id'] = selected_match['company_id']
                    update_companies.append(update_record)
                    print(f"✅ Will update '{selected_match['existing_company']}' (ID: {selected_match['company_id']})")
                    choice_made = True
                elif choice == '4':
                    # Show more details
                    print(f"\nNew company details:")
                    for key, value in new_company.items():
                        print(f"  {key}: {value}")
                    print(f"\nExisting company details:")
                    for i, match in enumerate(similar[:3], 1):
                        print(f"\n  Match {i}: '{match['existing_company']}' [ID: {match['company_id']}]")
                        print(f"    Address: {match['address']}")
                        print(f"    City: {match['city']}")
                        print(f"    Province: {match['province']}")
                        print(f"    Similarity: {match['similarity']:.2f}")
                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")
        else:
            # Non-interactive mode: auto-skip potential duplicates
            print(f"⭐️ Auto-skipping potential duplicate: '{new_company['CompanyName']}' "
                  f"(similarity: {similar[0]['similarity']:.2f} with '{similar[0]['existing_company']}')")
            
            # Store reference to the most similar existing company
            skip_record = new_company.copy()
            skip_record['_matched_existing_id'] = similar[0]['company_id']
            skip_record['_matched_existing_name'] = similar[0]['existing_company']
            skip_companies.append(skip_record)
    
    return pd.DataFrame(insert_companies), pd.DataFrame(skip_companies), pd.DataFrame(update_companies)

def handle_person_duplicates(df, existing_df, interactive=True, similarity_threshold=0.8):
    """
    Handle potential person duplicates with ID-based updates.
    """
    insert_people = []
    skip_people = []
    update_people = []
    
    for _, new_person in df.iterrows():
        similar = find_similar_people(new_person, existing_df, similarity_threshold)
        
        if not similar:
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            print(f"✅ Auto-inserting (no matches above {similarity_threshold}): '{full_name}'")
            insert_people.append(new_person)
            continue

        
        # Found similar people above threshold - show for interactive review
        if interactive:
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            print(f"\n🔍 Potential duplicate found for: '{full_name}'")
            print(f"   Email: {new_person.get('Email', 'N/A')}")
            
            for i, match in enumerate(similar[:3], 1):
                existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                email_display = match['email'] if match['email'] else 'None'
                print(f"   {i}. '{existing_name}' (similarity: {match['similarity']:.2f}) [ID: {match['person_id']}]")
                print(f"      Email: {email_display}")
            
            choice_made = False
            while not choice_made:
                choice = input("\nWhat would you like to do?\n"
                             "1. Insert as new person\n"
                             "2. Skip (it's a duplicate)\n"
                             "3. Update existing record\n"
                             "4. Show more details\n"
                             "Enter choice (1-4): ").strip()
                
                if choice == '1':
                    insert_people.append(new_person)
                    choice_made = True
                elif choice == '2':
                    # Let user select which existing person this is a duplicate of
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nWhich existing person is this a duplicate of?")
                        for i, match in enumerate(similar[:3], 1):
                            existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                            email_display = match['email'] if match['email'] else 'None'
                            print(f"   {i}. '{existing_name}' ({email_display}) [ID: {match['person_id']}]")
                        
                        while True:
                            try:
                                selection = int(input("Enter number (1-{}): ".format(min(3, len(similar)))).strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection-1]
                                    break
                                else:
                                    print("Invalid selection. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                    
                    # Store the matched person info in the skip record
                    skip_record = new_person.copy()
                    skip_record['_matched_existing_id'] = selected_match['person_id']
                    skip_record['_matched_existing_email'] = selected_match['email']
                    skip_people.append(skip_record)
                    choice_made = True
                elif choice == '3':
                    # Let user select which existing person to update
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nSelect which existing person to update:")
                        for i, match in enumerate(similar[:3], 1):
                            existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                            email_display = match['email'] if match['email'] else 'None'
                            print(f"   {i}. '{existing_name}' ({email_display}) [ID: {match['person_id']}]")
                        
                        while True:
                            try:
                                selection = int(input("Enter number (1-{}): ".format(min(3, len(similar)))).strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection-1]
                                    break
                                else:
                                    print("Invalid selection. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                    
                    # Create update record with ID for targeting
                    update_record = new_person.copy()
                    update_record['_update_target_id'] = selected_match['person_id']  # Store PersonID
                    
                    update_people.append(update_record)
                    selected_name = f"{selected_match['existing_first_name']} {selected_match['existing_last_name']}"
                    print(f"✅ Will update '{selected_name}' (ID: {selected_match['person_id']}) with new information")
                    choice_made = True
                elif choice == '4':
                    print(f"\nNew person details:")
                    for key, value in new_person.items():
                        print(f"  {key}: {value}")
                    print(f"\nExisting person details:")
                    for i, match in enumerate(similar[:3], 1):
                        existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                        print(f"\n  Match {i}: '{existing_name}' [ID: {match['person_id']}]")
                        print(f"    Email: {match['email']}")
                        print(f"    Similarity: {match['similarity']:.2f}")
                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")
        else:
            # Non-interactive mode: automatically skip entries above threshold
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            existing_name = f"{similar[0]['existing_first_name']} {similar[0]['existing_last_name']}"
            print(f"⭐️ Auto-skipping potential duplicate: '{full_name}' "
                  f"(similarity: {similar[0]['similarity']:.2f} with '{existing_name}')")
            skip_people.append(new_person)
    
    return pd.DataFrame(insert_people), pd.DataFrame(skip_people), pd.DataFrame(update_people)