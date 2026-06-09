import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
from datetime import datetime
import os
import requests

from config.stations import STATIONS

load_dotenv()

api_key = os.getenv("CTA_API_KEY")

url = "http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx"

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

for station_name, map_id in STATIONS.items():
    params = {
        "key": api_key,
        "mapid": map_id
    }

    response = requests.get(url, params=params)

    filename = (
        f"data/raw/arrivals/"
        f"{timestamp}_{station_name}.xml"
    )
    
    with open(filename, "w") as f:
        f.write(response.text)

    print(f"{station_name} : {timestamp}")