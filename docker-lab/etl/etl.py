import psycopg2
import time
from datetime import datetime

MAX_RETRIES = 10
RETRY_DELAY = 3

conn = None

for attempt in range(1, MAX_RETRIES + 1):
    try:
        print(f"Attempt {attempt}: Connecting to Postgres...")
        conn = psycopg2.connect(
            host="postgres",
            database="etl_db",
            user="etl_user",
            password="etl_pass"
        )
        print("Connected to Postgres!")
        break
    except psycopg2.OperationalError as e:
        print(f"Connection failed: {e}")
        time.sleep(RETRY_DELAY)

if conn is None:
    raise RuntimeError("Could not connect to Postgres after retries")

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS job_runs (
    id SERIAL PRIMARY KEY,
    run_time TIMESTAMP,
    status TEXT
)
""")

cur.execute("""
INSERT INTO job_runs (run_time, status)
VALUES (%s, %s)
""", (datetime.utcnow(), "SUCCESS"))

conn.commit()
cur.close()
conn.close()

print("ETL job completed successfully")

