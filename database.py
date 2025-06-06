import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()

db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_driver = os.getenv("DB_DRIVER")

def connect_to_db():
    try:
        return pyodbc.connect(
            driver=db_driver,
            server=db_host,
            database=db_name,
            uid=db_user,
            pwd=db_password
        )
    except pyodbc.Error as e:
        logging.error(f"Error connecting to database: {e}")
        return None

def fetch_sql(sql):
    conn = connect_to_db()
    if not conn:
        return None
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    except pyodbc.Error as e:
        logging.error(f"Error executing query: {e}")
        return None
    finally:
        conn.close()

def load_existing_records():
    sql = "SELECT RefNum FROM Investment"
    existing_records = fetch_sql(sql)
    return pd.DataFrame(existing_records)
