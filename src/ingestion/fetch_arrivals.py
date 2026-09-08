import sys
from pathlib import Path
import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.stations import STATIONS

from src.utils.logger import get_logger
logger = get_logger()

def fetch_arrivals():
    load_dotenv()

    api_key = os.getenv("CTA_API_KEY")

    url = "http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx"

    output_folder = Path("data/raw/arrivals")
    output_folder.mkdir(parents=True, exist_ok=True)

    for station_name, map_id in STATIONS.items():
        try:
            params = {
                "key": api_key,
                "mapid": map_id
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            # Get the CTA server timestamp from the XML
            root = ET.fromstring(response.text)
            tmst = root.find("tmst").text

            timestamp = datetime.strptime(
                tmst,
                "%Y%m%d %H:%M:%S"
            ).strftime("%Y%m%d_%H%M%S")

            filename = output_folder / f"{timestamp}_{station_name}.xml"

            with open(filename, "w", encoding="utf-8") as f:
                f.write(response.text)

            logger.info(f"Saved {station_name}")

        except Exception as e:
            logger.error(f"Failed to fetch arrivals for {station_name}: {e}")


if __name__ == "__main__":
    fetch_arrivals()