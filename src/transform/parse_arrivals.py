import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path

def parse_arrivals(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    etas = root.findall("eta")

    records = []

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
            "wait_time": wait_time
        })

    return pd.DataFrame(records)

def parse_all_arrivals(folder):
    all_dfs = []

    for xml_file in Path(folder).glob("*.xml"):
        df = parse_arrivals(xml_file)
        all_dfs.append(df)

    return pd.concat(all_dfs, ignore_index=True)



xml_file = "data/raw/arrivals/20260608_183325.xml"

df = parse_all_arrivals("data/raw/arrivals")


df.to_csv(
    "data/processed/arrivals/all_arrivals_20260608_183325.csv",
    index=False
)