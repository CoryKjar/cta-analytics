import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

database_url = os.getenv("DATABASE_URL")

with psycopg.connect(database_url) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]

        print("Connected to PostgreSQL!")
        print(version)