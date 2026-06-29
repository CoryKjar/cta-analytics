import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
from datetime import datetime
import shutil

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

        is_approaching = eta.find("isApp").text
        is_delayed = eta.find("isDly").text
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

def parse_all_arrivals(folder):
    all_dfs = []
    processed_files = []

    xml_files = list(Path(folder).glob("*.xml"))

    print(f"Found {len(xml_files)} XML files to parse.")

    for xml_file in xml_files:
        df = parse_arrivals(xml_file)
        all_dfs.append(df)
        processed_files.append(xml_file)

        print(f"Parsed {xml_file.name}: {len(df)} arrivals")

    combined_df = pd.concat(all_dfs, ignore_index=True)

    print(
        f"Finished parsing {len(xml_files)} files. "
        f"Total arrivals: {len(combined_df)}"
    )

    return combined_df, processed_files


if __name__ == "__main__":
    df = parse_all_arrivals("data/raw/arrivals")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")