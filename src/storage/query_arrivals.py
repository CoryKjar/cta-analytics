import sqlite3
import pandas as pd

conn = sqlite3.connect("data/database/cta.db")

df = pd.read_sql("""
            SELECT * FROM arrivals limit 10
            """, conn)

print(df)

conn.close()