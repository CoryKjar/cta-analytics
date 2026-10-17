import os
import sys
import random
import time
from pathlib import Path
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
from requests.exceptions import Timeout, ConnectionError, HTTPError

from dotenv import load_dotenv
from src.storage.object_storage import storage_error_details, upload_file
from src.transform.parse_arrivals import validate_cta_response

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.stations import STATIONS
from src.utils.logger import get_logger

logger = get_logger(__name__)


def fetch_cta_response(url, params, station_name, max_attempts=3):
    """Fetch a station response with bounded retries and safe logging."""
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=(5, 20))
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
                    "event=cta_retry station=%s http_status=%s attempt=%s/%s delay_seconds=%.1f",
                    station_name, response.status_code, attempt, max_attempts, delay,
                )
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response
        except (Timeout, ConnectionError) as exc:
            if attempt == max_attempts:
                logger.error(
                    "event=cta_request_failed station=%s attempts=%s error_type=%s",
                    station_name, max_attempts, type(exc).__name__,
                )
                raise
            delay = min(2 ** (attempt - 1) + random.uniform(0, 1), 15)
            logger.warning(
                "event=cta_retry station=%s error_type=%s attempt=%s/%s delay_seconds=%.1f",
                station_name, type(exc).__name__, attempt, max_attempts, delay,
            )
            time.sleep(delay)
    raise RuntimeError(f"CTA request failed for {station_name}")


def fetch_arrivals():
    """Fetch and upload station snapshots; return only successfully uploaded paths."""
    started = time.monotonic()
    load_dotenv()
    api_key = os.getenv("CTA_API_KEY")
    if not api_key:
        logger.critical("event=configuration_error missing_environment_variable=CTA_API_KEY")
        raise ValueError("CTA_API_KEY is not configured.")

    url = "http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx"
    output_folder = Path("data/raw/arrivals")
    output_folder.mkdir(parents=True, exist_ok=True)
    uploaded_files = []
    failed = 0
    logger.info("event=collection_started station_count=%s", len(STATIONS))

    for index, (station_name, map_id) in enumerate(STATIONS.items(), start=1):
        station_started = time.monotonic()
        try:
            params = {"key": api_key, "mapid": map_id}
            response = fetch_cta_response(url, params, station_name)
            root = ET.fromstring(response.text)
            validate_cta_response(root)
            timestamp_node = root.find("tmst")
            if timestamp_node is None or not timestamp_node.text:
                raise ValueError("CTA response has no valid tmst timestamp.")
            timestamp = datetime.strptime(timestamp_node.text, "%Y%m%d %H:%M:%S").strftime("%Y%m%d_%H%M%S")
            local_path = output_folder / f"{timestamp}_{station_name}.xml"
            object_key = f"raw/{timestamp[:4]}/{timestamp[4:6]}/{timestamp[6:8]}/{local_path.name}"
            local_path.write_text(response.text, encoding="utf-8")
            upload_file(local_path, object_key)
            uploaded_files.append(local_path)
            logger.info(
                "event=station_upload_succeeded station=%s station_index=%s/%s http_status=%s duration_seconds=%.2f file=%s",
                station_name, index, len(STATIONS), response.status_code,
                time.monotonic() - station_started, local_path.name,
            )
        except Exception as exc:
            failed += 1
            # Do not log exception text or request URLs: requests exceptions can contain the API key.
            status = getattr(getattr(exc, "response", None), "status_code", None)
            storage_code, storage_status = storage_error_details(exc)
            logger.error(
                "event=station_collection_failed station=%s station_index=%s/%s error_type=%s http_status=%s storage_error_code=%s storage_http_status=%s duration_seconds=%.2f",
                station_name, index, len(STATIONS), type(exc).__name__, status,
                storage_code, storage_status,
                time.monotonic() - station_started,
            )

    logger.info(
        "event=collection_completed successful=%s failed=%s total=%s duration_seconds=%.2f",
        len(uploaded_files), failed, len(STATIONS), time.monotonic() - started,
    )
    return uploaded_files


if __name__ == "__main__":
    fetch_arrivals()
