import sys
from pathlib import Path
import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.stations import STATIONS


def fetch_arrivals():
    load_dotenv()

    api_key = os.getenv("CTA_API_KEY")

    url = "http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx"

    for station_name, map_id in STATIONS.items():
        params = {
            "key": api_key,
            "mapid": map_id
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        # Get the CTA server timestamp from the XML
        root = ET.fromstring(response.text)
        tmst = root.find("tmst").text

        timestamp = datetime.strptime(
            tmst,
            "%Y%m%d %H:%M:%S"
        ).strftime("%Y%m%d_%H%M%S")

        filename = (
            f"data/raw/arrivals/"
            f"{timestamp}_{station_name}.xml"
        )

        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)

        print(f"Saved {station_name} -> {filename}")


if __name__ == "__main__":
    fetch_arrivals()