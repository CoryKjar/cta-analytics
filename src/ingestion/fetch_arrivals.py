import os
import sys
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

from dotenv import load_dotenv
from src.storage.object_storage import upload_file

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.stations import STATIONS

from src.utils.logger import get_logger

logger = get_logger()


def fetch_arrivals():
    """Fetch and upload station snapshots; return only successfully uploaded paths."""
    load_dotenv()
    api_key = os.getenv("CTA_API_KEY")
    if not api_key:
        raise ValueError("CTA_API_KEY is not configured.")

    url = "http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx"
    output_folder = Path("data/raw/arrivals")
    output_folder.mkdir(parents=True, exist_ok=True)

    uploaded_files = []
    failed = 0

    for station_name, map_id in STATIONS.items():
        local_path = None
        try:
            params = {"key": api_key, "mapid": map_id}
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            root = ET.fromstring(response.text)
            timestamp_node = root.find("tmst")
            if timestamp_node is None or not timestamp_node.text:
                raise ValueError("CTA response has no valid tmst timestamp.")

            timestamp = datetime.strptime(
                timestamp_node.text, "%Y%m%d %H:%M:%S"
            ).strftime("%Y%m%d_%H%M%S")

            local_path = output_folder / f"{timestamp}_{station_name}.xml"
            object_key = (
                f"raw/{timestamp[:4]}/{timestamp[4:6]}/"
                f"{timestamp[6:8]}/{local_path.name}"
            )

            # Write response locally, then upload. Only return it if upload succeeds.
            local_path.write_text(response.text, encoding="utf-8")
            upload_file(local_path, object_key)

            uploaded_files.append(local_path)
            logger.info(f"Uploaded {station_name}: {local_path.name}")

        except Exception as exc:
            failed += 1
            logger.error(
                f"Failed to fetch/upload arrivals for {station_name}: {exc}"
            )

    logger.info(
        f"Fetch arrivals completed: {len(uploaded_files)} successful, "
        f"{failed} failed"
    )
    return uploaded_files


if __name__ == "__main__":
    fetch_arrivals()
