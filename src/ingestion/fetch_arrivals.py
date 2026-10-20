import os
import random
import time
from pathlib import Path
from datetime import datetime, timezone
import requests
import xml.etree.ElementTree as ET
from requests.exceptions import Timeout, ConnectionError

from dotenv import load_dotenv
from src.config.stations import STATIONS
from src.storage.object_storage import storage_error_details, upload_file
from src.storage.collection_ledger import CollectionAttemptLedger
from src.transform.parse_arrivals import validate_cta_response

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _record_request_attempt(ledger, station_id, counter, attempt):
    counter[0] = attempt
    if attempt > 1:
        ledger.record_request_attempt(station_id, attempt)


def fetch_cta_response(url, params, station_name, max_attempts=3, *, run_id=None, on_attempt=None, on_retry=None):
    """Fetch a station response with bounded retries and safe logging."""
    for attempt in range(1, max_attempts + 1):
        if on_attempt is not None:
            on_attempt(attempt)
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
                if on_retry is not None:
                    on_retry(http_status=response.status_code, error_type="HTTPError")
                logger.warning(
                    "event=cta_retry run_id=%s station=%s http_status=%s attempt=%s/%s delay_seconds=%.1f",
                    run_id, station_name, response.status_code, attempt, max_attempts, delay,
                )
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response
        except (Timeout, ConnectionError) as exc:
            if attempt == max_attempts:
                logger.error(
                    "event=cta_request_failed run_id=%s station=%s attempts=%s error_type=%s",
                    run_id, station_name, attempt, type(exc).__name__,
                )
                raise
            delay = min(2 ** (attempt - 1) + random.uniform(0, 1), 15)
            if on_retry is not None:
                on_retry(http_status=None, error_type=type(exc).__name__)
            logger.warning(
                "event=cta_retry run_id=%s station=%s error_type=%s attempt=%s/%s delay_seconds=%.1f",
                run_id, station_name, type(exc).__name__, attempt, max_attempts, delay,
            )
            time.sleep(delay)
    raise RuntimeError(f"CTA request failed for {station_name}")


def fetch_arrivals():
    """Fetch and upload station snapshots; return only successfully uploaded paths."""
    started = time.monotonic()
    load_dotenv()
    ledger = CollectionAttemptLedger(STATIONS, logger)
    uploaded_files = []
    failed = 0
    fatal_error_type = None
    logger.info("event=collection_started run_id=%s station_count=%s", ledger.run_id, len(STATIONS))
    try:
        api_key = os.getenv("CTA_API_KEY")
        if not api_key:
            logger.critical("event=configuration_error missing_environment_variable=CTA_API_KEY")
            raise ValueError("CTA_API_KEY is not configured.")

        url = "http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx"
        output_folder = Path("data/raw/arrivals")
        output_folder.mkdir(parents=True, exist_ok=True)

        for index, (station_name, map_id) in enumerate(STATIONS.items(), start=1):
            station_started = time.monotonic()
            request_attempts = [0]
            stage = "cta_request"
            http_status = None
            cta_error_code = None
            local_path = None
            object_key = None
            ledger.begin_station(map_id, started_at=datetime.now(timezone.utc))
            try:
                params = {"key": api_key, "mapid": map_id}
                response = fetch_cta_response(
                    url, params, station_name, run_id=ledger.run_id,
                    on_attempt=lambda attempt: _record_request_attempt(
                        ledger, map_id, request_attempts, attempt,
                    ),
                    on_retry=lambda **details: ledger.record_retry_failure(map_id, **details),
                )
                http_status = response.status_code
                stage = "response_parse"
                ledger.set_stage(map_id, stage, http_status=http_status)
                root = ET.fromstring(response.text)
                stage = "response_validation"
                raw_error_code = root.findtext("errCd")
                cta_error_code = (
                    raw_error_code.strip()
                    if raw_error_code and raw_error_code.strip().isdigit()
                    and len(raw_error_code.strip()) <= 5
                    else None
                )
                ledger.set_stage(
                    map_id, stage, http_status=http_status,
                    cta_error_code=cta_error_code,
                )
                validate_cta_response(root)
                stage = "timestamp_validation"
                ledger.set_stage(
                    map_id, stage, http_status=http_status,
                    cta_error_code=cta_error_code,
                )
                timestamp_node = root.find("tmst")
                if timestamp_node is None or not timestamp_node.text:
                    raise ValueError("CTA response has no valid tmst timestamp.")
                timestamp = datetime.strptime(timestamp_node.text, "%Y%m%d %H:%M:%S").strftime("%Y%m%d_%H%M%S")
                local_path = output_folder / f"{timestamp}_{station_name}.xml"
                object_key = f"raw/{timestamp[:4]}/{timestamp[4:6]}/{timestamp[6:8]}/{local_path.name}"
                stage = "local_write"
                ledger.set_stage(
                    map_id, stage, http_status=http_status,
                    cta_error_code=cta_error_code,
                )
                local_path.write_text(response.text, encoding="utf-8")
                stage = "bucket_upload"
                ledger.set_stage(
                    map_id, stage, http_status=http_status,
                    cta_error_code=cta_error_code,
                )
                upload_file(local_path, object_key)
                uploaded_files.append(local_path)
                duration = time.monotonic() - station_started
                ledger.station_uploaded(
                    map_id, attempt_count=request_attempts[0], http_status=http_status,
                    cta_error_code=cta_error_code, source_file=local_path.name,
                    object_key=object_key, duration_seconds=duration,
                )
                logger.info(
                    "event=station_upload_succeeded run_id=%s station=%s station_index=%s/%s http_status=%s request_attempts=%s duration_seconds=%.2f file=%s",
                    ledger.run_id, station_name, index, len(STATIONS), http_status,
                    request_attempts[0], duration, local_path.name,
                )
            except Exception as exc:
                failed += 1
                # Never log exception text or request URLs; they can contain secrets.
                response_status = getattr(getattr(exc, "response", None), "status_code", None)
                http_status = http_status or response_status
                storage_code, storage_status = storage_error_details(exc)
                duration = time.monotonic() - station_started
                ledger.station_failed(
                    map_id, stage=stage, attempt_count=request_attempts[0],
                    error_type=type(exc).__name__, http_status=http_status,
                    cta_error_code=cta_error_code, storage_error_code=storage_code,
                    storage_http_status=storage_status, duration_seconds=duration,
                    source_file=local_path.name if local_path is not None else None,
                    object_key=object_key,
                )
                logger.error(
                    "event=station_collection_failed run_id=%s station=%s station_index=%s/%s stage=%s error_type=%s http_status=%s cta_error_code=%s storage_error_code=%s storage_http_status=%s request_attempts=%s duration_seconds=%.2f",
                    ledger.run_id, station_name, index, len(STATIONS), stage,
                    type(exc).__name__, http_status, cta_error_code, storage_code,
                    storage_status, request_attempts[0], duration,
                )

        logger.info(
            "event=collection_completed run_id=%s successful=%s failed=%s total=%s duration_seconds=%.2f",
            ledger.run_id, len(uploaded_files), failed, len(STATIONS),
            time.monotonic() - started,
        )
        return uploaded_files
    except Exception as exc:
        fatal_error_type = type(exc).__name__
        raise
    finally:
        ledger.finish(
            uploaded_count=len(uploaded_files), failed_count=failed,
            fatal_error_type=fatal_error_type,
        )
        ledger.close()


if __name__ == "__main__":
    fetch_arrivals()
