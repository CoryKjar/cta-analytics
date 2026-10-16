import os
import sys
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import random
import time
from requests.exceptions import Timeout, ConnectionError, HTTPError

from dotenv import load_dotenv
from src.storage.object_storage import upload_file

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.stations import STATIONS

from src.utils.logger import get_logger

logger = get_logger()

def fetch_cta_response(url, params, station_name, max_attempts=3):
    """Fetch a station response with bounded retries and safe logging."""
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=(5, 20),
            )

            if response.status_code in (429, 500, 502, 503, 504):
                if attempt == max_attempts:
                    response.raise_for_status()

                retry_after = response.headers.get("Retry-After")
                try:
                    delay = min(float(retry_after), 30) if retry_after else 0
                except ValueError:
                    delay = 0

                if delay <= 0:
                    delay = min(2 ** (attempt - 1) + random.uniform(0, 1), 15)

                logger.warning(
                    "CTA API returned HTTP %s for %s; "
                    "retry %s/%s in %.1fs",
                    response.status_code,
                    station_name,
                    attempt,
                    max_attempts - 1,
                    delay,
                )
                time.sleep(delay)
                continue

            response.raise_for_status()
            return response

        except (Timeout, ConnectionError):
            if attempt == max_attempts:
                logger.error(
                    "CTA API request failed for %s after %s attempts "
                    "(timeout or connection error)",
                    station_name,
                    max_attempts,
                )
                raise

            delay = min(2 ** (attempt - 1) + random.uniform(0, 1), 15)
            logger.warning(
                "CTA API timeout/connection failure for %s; "
                "retry %s/%s in %.1fs",
                station_name,
                attempt,
                max_attempts - 1,
                delay,
            )
            time.sleep(delay)

    raise RuntimeError(f"CTA request failed for {station_name}")

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
            response = fetch_cta_response(url, params, station_name)

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
            "Failed to fetch/upload arrivals for %s (%s)",
            station_name,
            type(exc).__name__,
        )

    logger.info(
        f"Fetch arrivals completed: {len(uploaded_files)} successful, "
        f"{failed} failed"
    )
    return uploaded_files


if __name__ == "__main__":
    fetch_arrivals()
