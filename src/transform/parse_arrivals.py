import xml.etree.ElementTree as ET
import gzip
import re
from datetime import datetime
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


def validate_cta_response(root):
    if root.findtext("errCd", "0").strip() != "0":
        raise ValueError("CTA response reports an API error.")


def parse_arrivals_xml(xml_bytes, source_file):
    """Parse the same source representation for ingestion and historical restore."""
    root = ET.fromstring(xml_bytes)
    validate_cta_response(root)
    source_xml = gzip.compress(xml_bytes)

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
        is_scheduled = parse_cta_boolean(eta.findtext("isSch"))
        is_fault = parse_cta_boolean(eta.findtext("isFlt"))
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
            "is_scheduled": is_scheduled,
            "is_fault": is_fault,
            "latitude": latitude,
            "longitude": longitude,
            "heading": heading,
            "wait_time": wait_time,
            "source_file": Path(source_file).name,
            "_source_xml_gzip": source_xml,
        })


    return pd.DataFrame(records)


def source_prediction_records(xml_bytes, file_record, routes, destinations):
    """Verify source metadata and normalize rows for audited ingestion/replay."""
    file_id, filename, collected, row_count = file_record
    root = ET.fromstring(xml_bytes)
    if root.findtext("errCd", "0") != "0" or root.findtext("tmst") != collected.strftime("%Y%m%d %H:%M:%S"):
        raise ValueError("Source API timestamp/error does not match the manifest.")
    if filename[:15] != collected.strftime("%Y%m%d_%H%M%S") or "/" in filename or "\\" in filename:
        raise ValueError("Source filename is not a valid manifest basename.")
    filename_time = datetime.strptime(
        "_".join(Path(filename).stem.split("_")[:2]), "%Y%m%d_%H%M%S",
    )
    if filename_time != collected:
        raise ValueError("Source filename timestamp does not match the manifest.")

    records = []
    for eta in root.findall("eta"):
        station_id = int(eta.find("staId").text)
        stop_id = int(eta.find("stpId").text)
        route = eta.find("rt").text
        destination = str(eta.find("destNm").text).strip()
        if route not in routes or destination not in destinations:
            raise ValueError("Source route/destination is absent from the database dimensions.")

        prediction_time = datetime.strptime(eta.find("prdt").text, "%Y%m%d %H:%M:%S")
        arrival_time = datetime.strptime(eta.find("arrT").text, "%Y%m%d %H:%M:%S")
        latitude = eta.find("lat").text
        longitude = eta.find("lon").text
        heading = eta.find("heading").text
        values = (
            collected, station_id, stop_id, routes[route], destinations[destination], file_id,
            eta.find("trDr").text, prediction_time, arrival_time,
            parse_cta_boolean(eta.find("isApp").text),
            parse_cta_boolean(eta.find("isDly").text),
            parse_cta_boolean(eta.findtext("isSch")),
            parse_cta_boolean(eta.findtext("isFlt")),
            float(latitude) if latitude is not None else None,
            float(longitude) if longitude is not None else None,
            int(heading) if heading is not None else None,
            (arrival_time - prediction_time).total_seconds() / 60,
            parse_cta_run_number(eta.findtext("rn")),
        )
        records.append(values)
    return records