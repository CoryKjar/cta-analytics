import sqlite3
import pandas as pd

query = """
SELECT * FROM arrivals where station_id = '41220';
"""

conn = sqlite3.connect("data/database/cta.db")

df = pd.read_sql(query, conn)

print(df)

conn.close()