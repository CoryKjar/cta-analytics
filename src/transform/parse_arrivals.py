import xml.etree.ElementTree as ET
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
    tree = ET.parse(xml_file)
    root = tree.getroot()

    etas = root.findall("eta")

    records = []

    filename = Path(xml_file).stem
    parts = filename.split("_")

    collection_timestamp = pd.to_datetime("_".join(parts[:2]),format="%Y%m%d_%H%M%S")

    for eta in etas:
        station_id = eta.find("staId").text
        station = eta.find("staNm").text
        stop_id = eta.find("stpId").text
        route = eta.find("rt").text
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
            "source_file": Path(xml_file).name
        })


    return pd.DataFrame(records)