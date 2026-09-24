import os
import psycopg
from dotenv import load_dotenv

from src.config.stations import STATIONS


load_dotenv()

database_url = os.getenv("DATABASE_URL")

with psycopg.connect(database_url) as conn:
    with conn.cursor() as cursor:

        cursor.execute("""
            SELECT DISTINCT station_id
            FROM arrivals
            ORDER BY station_id;
        """)

        database_station_ids = {
            str(row[0])
            for row in cursor.fetchall()
        }


configured_station_ids = {
    str(map_id)
    for map_id in STATIONS.values()
}

missing_station_ids = configured_station_ids - database_station_ids


print(f"Configured station IDs: {len(configured_station_ids)}")
print(f"Database station IDs:   {len(database_station_ids)}")

print("\nMissing station IDs:")
for station_id in sorted(missing_station_ids):
    print(f"  - {station_id}")