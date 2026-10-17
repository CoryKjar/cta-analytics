import xml.etree.ElementTree as ET
import re
import pandas as pd
from pathlib import Path


def parse_cta_boolean(value):
    """Parse CTA's binary flag without treating the nonempty string '0' as true."""
    if isinstance(value, str):
        value = value.strip()
    if pd.isna(value) or value == "":
        return None
    if value in (False, "0"):
        return False
    if value in (True, "1"):
        return True
    raise ValueError(f"Invalid CTA boolean flag: {value!r}")


def parse_arrivals(xml_file):
    return parse_arrivals_xml(Path(xml_file).read_bytes(), Path(xml_file).name)


def parse_cta_run_number(value):
    """Preserve the API train identity rather than inventing one for missing data."""
    if pd.isna(value) or not re.fullmatch(r"[0-9]+", str(value).strip()):
        raise ValueError("CTA prediction has no valid train run number.")
    return str(value).strip()


def parse_arrivals_xml(xml_bytes, source_file):
    """Parse the same source representation for ingestion and historical restore."""
    root = ET.fromstring(xml_bytes)

    etas = root.findall("eta")

    records = []

    filename = Path(source_file).stem
    parts = filename.split("_")

    collection_timestamp = pd.to_datetime("_".join(parts[:2]),format="%Y%m%d_%H%M%S")

    for eta in etas:
        station_id = eta.find("staId").text
        station = eta.find("staNm").text
        stop_id = eta.find("stpId").text
        route = eta.find("rt").text
        run_number = parse_cta_run_number(eta.findtext("rn"))
        destination = eta.find("destNm").text
        direction = eta.find("trDr").text

        prediction_time = pd.to_datetime(
            eta.find("prdt").text,
            format="%Y%m%d %H:%M:%S"
        )

        arrival_time = pd.to_datetime(
            eta.find("arrT").text,
            format="%Y%m%d %H:%M:%S"
        )

        is_approaching = parse_cta_boolean(eta.find("isApp").text)
        is_delayed = parse_cta_boolean(eta.find("isDly").text)
        latitude = eta.find("lat").text
        longitude = eta.find("lon").text
        heading = eta.find("heading").text

        wait_time = (
            arrival_time - prediction_time
        ).total_seconds() / 60

        records.append({
            "collection_timestamp": collection_timestamp,
            "station_id": station_id,
            "station": station,
            "stop_id": stop_id,
            "route": route,
            "run_number": run_number,
            "destination": destination,
            "direction": direction,
            "prediction_time": prediction_time,
            "arrival_time": arrival_time,
            "is_approaching": is_approaching,
            "is_delayed": is_delayed,
            "latitude": latitude,
            "longitude": longitude,
            "heading": heading,
            "wait_time": wait_time,
            "source_file": Path(source_file).name
        })


    return pd.DataFrame(records)