from database.get import get_company_id_by_name, get_person_id_by_email 
from database.insert import insert_into_company_asgmt, insert_into_project_asgmt
from database.connection import connect_to_db
from api.utils import assignment_exists, safe_int


def process_join_tables(investment_df,
                        people_insert_df, people_skip_df, people_update_df,
                        company_insert_df, company_skip_df, company_update_df):
    conn = connect_to_db(False)
    if not conn:
        print("Unable to connect to DB for join table inserts.")
        return

    try:
        linked_people = set()   # (refnum, person_id)
        linked_companies = set()  # (refnum, company_id)

        for _, investment in investment_df.iterrows():
            refnum = investment["RefNum"]

            # ---- Handle People ----
            email = str(investment.get("Email", "")).strip().lower()
            person_id = None

            # check skip df
            if not people_skip_df.empty and "_matched_existing_id" in people_skip_df.columns:
                match_skip = people_skip_df[people_skip_df.get("Email", "").str.lower() == email]
                if not match_skip.empty:
                    person_id = safe_int(match_skip["_matched_existing_id"].iloc[0])

            # check update df
            if person_id is None and not people_update_df.empty and "_update_target_id" in people_update_df.columns:
                match_update = people_update_df[people_update_df.get("Email", "").str.lower() == email]
                if not match_update.empty:
                    person_id = safe_int(match_update["_update_target_id"].iloc[0])

            # fallback lookup
            if person_id is None and email:
                pid = get_person_id_by_email(email, conn)
                if pid:
                    person_id = safe_int(pid)

            # insert only if not already linked
            if person_id is not None:
                key = (refnum, person_id)
                if key not in linked_people and not assignment_exists("ProjectAsgmt", refnum, person_id, conn):
                    insert_into_project_asgmt(refnum, person_id, conn)
                    linked_people.add(key)

            # ---- Handle Companies ----
            company_name = str(investment.get("CompanyName", "")).strip()
            company_id = None

            if not company_skip_df.empty and "_matched_existing_id" in company_skip_df.columns:
                match_skip = company_skip_df[company_skip_df.get("CompanyName", "") == company_name]
                if not match_skip.empty:
                    company_id = safe_int(match_skip["_matched_existing_id"].iloc[0])

            if company_id is None and not company_update_df.empty and "_update_target_id" in company_update_df.columns:
                match_update = company_update_df[company_update_df.get("CompanyName", "") == company_name]
                if not match_update.empty:
                    company_id = safe_int(match_update["_update_target_id"].iloc[0])

            if company_id is None and company_name:
                cid = get_company_id_by_name(company_name, conn)
                if cid:
                    company_id = safe_int(cid)

            if company_id is not None:
                key = (refnum, company_id)
                if key not in linked_companies and not assignment_exists("CompanyAsgmt", refnum, company_id, conn):
                    insert_into_company_asgmt(refnum, company_id, conn)
                    linked_companies.add(key)

    finally:
        conn.close()