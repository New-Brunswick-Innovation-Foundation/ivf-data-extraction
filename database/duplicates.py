from database.similar import find_similar_companies, find_similar_people
import pandas as pd


def format_row(label, name, contact, id_val="", similarity=""):
    """Format a row into aligned columns for table display."""
    return (
        f"{label.ljust(10)} | "
        f"{name.ljust(30)} | "
        f"{contact.ljust(40)} | "
        f"ID: {str(id_val).ljust(6)} Sim: {similarity}"
    )


def print_person_duplicate(new_person, similar):
    """Print a table comparing new person to existing matches."""
    full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}".strip()
    email = new_person.get("Email", "N/A")

    print("\n" + "=" * 100)
    print(f"🔍 Potential duplicate found for person: {full_name}")
    print("-" * 100)

    # Header
    print(f"{'Label'.ljust(10)} | {'Name'.ljust(30)} | {'Email'.ljust(40)} | {'ID / Similarity'}")
    print("-" * 100)

    # NEW record
    print(format_row("NEW", full_name, email))

    # EXISTING matches
    for i, match in enumerate(similar[:3], 1):
        existing_name = f"{match['existing_first_name']} {match['existing_last_name']}".strip()
        email_display = match['email'] if match['email'] else "None"
        print(format_row(
            f"MATCH {i}",
            existing_name,
            email_display,
            id_val=match['person_id'],
            similarity=f"{match['similarity']:.2f}"
        ))

    print("-" * 100)


def print_company_duplicate(new_company, similar):
    """Print a table comparing new company to existing matches."""
    company_name = new_company.get("CompanyName", "N/A")
    address = f"{new_company.get('Address', 'N/A')}, {new_company.get('City', 'N/A')}"

    print("\n" + "=" * 100)
    print(f"🔍 Potential duplicate found for company: {company_name}")
    print("-" * 100)

    # Header
    print(f"{'Label'.ljust(10)} | {'Company'.ljust(30)} | {'Address'.ljust(40)} | {'ID / Similarity'}")
    print("-" * 100)

    # NEW record
    print(format_row("NEW", company_name, address))

    # EXISTING matches
    for i, match in enumerate(similar[:3], 1):
        existing_address = f"{match['address']}, {match['city']}"
        print(format_row(
            f"MATCH {i}",
            match['existing_company'],
            existing_address,
            id_val=match['company_id'],
            similarity=f"{match['similarity']:.2f}"
        ))

    print("-" * 100)


def handle_company_duplicates(df, existing_df, interactive=True, similarity_threshold=0.8):
    insert_companies, skip_companies, update_companies = [], [], []

    for _, new_company in df.iterrows():
        similar = find_similar_companies(new_company, existing_df, similarity_threshold)

        if not similar:
            print(f"✅ Auto-inserting (no matches above {similarity_threshold}): '{new_company['CompanyName']}'")
            insert_companies.append(new_company)
            continue

        if interactive:
            print_company_duplicate(new_company, similar)

            choice_made = False
            while not choice_made:
                choice = input(
                    "\nWhat would you like to do?\n"
                    "1. Insert as new company\n"
                    "2. Skip (it's a duplicate)\n"
                    "3. Update existing record\n"
                    "4. Show more details\n"
                    "Enter choice (1-4): "
                ).strip()

                if choice == '1':
                    insert_companies.append(new_company)
                    choice_made = True

                elif choice == '2':
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nWhich existing company is this a duplicate of?")
                        for i, match in enumerate(similar[:3], 1):
                            print(f"   {i}. '{match['existing_company']}' [ID: {match['company_id']}]")

                        while True:
                            try:
                                selection = int(input(f"Enter number (1-{min(3, len(similar))}): ").strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection - 1]
                                    break
                                else:
                                    print("Invalid selection. Try again.")
                            except ValueError:
                                print("Please enter a valid number.")

                    skip_record = new_company.copy()
                    skip_record['_matched_existing_id'] = selected_match['company_id']
                    skip_record['_matched_existing_name'] = selected_match['existing_company']
                    skip_companies.append(skip_record)
                    choice_made = True

                elif choice == '3':
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nSelect which existing company to update:")
                        for i, match in enumerate(similar[:3], 1):
                            print(f"   {i}. '{match['existing_company']}' [ID: {match['company_id']}]")

                        while True:
                            try:
                                selection = int(input(f"Enter number (1-{min(3, len(similar))}): ").strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection - 1]
                                    break
                                else:
                                    print("Invalid selection. Try again.")
                            except ValueError:
                                print("Please enter a valid number.")

                    update_record = new_company.copy()
                    update_record['_update_target_id'] = selected_match['company_id']
                    update_companies.append(update_record)
                    print(f"✅ Will update '{selected_match['existing_company']}' (ID: {selected_match['company_id']})")
                    choice_made = True

                elif choice == '4':
                    print("\nNew company details:")
                    for key, value in new_company.items():
                        print(f"  {key}: {value}")

                    print("\nExisting company details:")
                    for i, match in enumerate(similar[:3], 1):
                        print(f"\n  Match {i}: '{match['existing_company']}' [ID: {match['company_id']}]")
                        for key, value in match.items():
                            print(f"    {key}: {value}")
                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")
        else:
            print(f"⭐️ Auto-skipping potential duplicate: '{new_company['CompanyName']}' "
                  f"(similarity: {similar[0]['similarity']:.2f} with '{similar[0]['existing_company']}')")

            skip_record = new_company.copy()
            skip_record['_matched_existing_id'] = similar[0]['company_id']
            skip_record['_matched_existing_name'] = similar[0]['existing_company']
            skip_companies.append(skip_record)

    return pd.DataFrame(insert_companies), pd.DataFrame(skip_companies), pd.DataFrame(update_companies)


def handle_person_duplicates(df, existing_df, interactive=True, similarity_threshold=0.8):
    insert_people, skip_people, update_people = [], [], []

    for _, new_person in df.iterrows():
        similar = find_similar_people(new_person, existing_df, similarity_threshold)

        if not similar:
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            print(f"✅ Auto-inserting (no matches above {similarity_threshold}): '{full_name}'")
            insert_people.append(new_person)
            continue

        if interactive:
            print_person_duplicate(new_person, similar)

            choice_made = False
            while not choice_made:
                choice = input(
                    "\nWhat would you like to do?\n"
                    "1. Insert as new person\n"
                    "2. Skip (it's a duplicate)\n"
                    "3. Update existing record\n"
                    "4. Show more details\n"
                    "Enter choice (1-4): "
                ).strip()

                if choice == '1':
                    insert_people.append(new_person)
                    choice_made = True

                elif choice == '2':
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nWhich existing person is this a duplicate of?")
                        for i, match in enumerate(similar[:3], 1):
                            existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                            print(f"   {i}. '{existing_name}' ({match['email']}) [ID: {match['person_id']}]")

                        while True:
                            try:
                                selection = int(input(f"Enter number (1-{min(3, len(similar))}): ").strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection - 1]
                                    break
                                else:
                                    print("Invalid selection. Try again.")
                            except ValueError:
                                print("Please enter a valid number.")

                    skip_record = new_person.copy()
                    skip_record['_matched_existing_id'] = selected_match['person_id']
                    skip_record['_matched_existing_email'] = selected_match['email']
                    skip_people.append(skip_record)
                    choice_made = True

                elif choice == '3':
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nSelect which existing person to update:")
                        for i, match in enumerate(similar[:3], 1):
                            existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                            print(f"   {i}. '{existing_name}' ({match['email']}) [ID: {match['person_id']}]")

                        while True:
                            try:
                                selection = int(input(f"Enter number (1-{min(3, len(similar))}): ").strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection - 1]
                                    break
                                else:
                                    print("Invalid selection. Try again.")
                            except ValueError:
                                print("Please enter a valid number.")

                    update_record = new_person.copy()
                    update_record['_update_target_id'] = selected_match['person_id']
                    update_people.append(update_record)
                    print(f"✅ Will update '{selected_match['existing_first_name']} {selected_match['existing_last_name']}' "
                          f"(ID: {selected_match['person_id']})")
                    choice_made = True

                elif choice == '4':
                    print("\nNew person details:")
                    for key, value in new_person.items():
                        print(f"  {key}: {value}")

                    print("\nExisting person details:")
                    for i, match in enumerate(similar[:3], 1):
                        existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                        print(f"\n  Match {i}: '{existing_name}' [ID: {match['person_id']}]")
                        for key, value in match.items():
                            print(f"    {key}: {value}")
                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")
        else:
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            existing_name = f"{similar[0]['existing_first_name']} {similar[0]['existing_last_name']}"
            print(f"⭐️ Auto-skipping potential duplicate: '{full_name}' "
                  f"(similarity: {similar[0]['similarity']:.2f} with '{existing_name}')")
            skip_people.append(new_person)

    return pd.DataFrame(insert_people), pd.DataFrame(skip_people), pd.DataFrame(update_people)
