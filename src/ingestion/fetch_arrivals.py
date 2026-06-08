from dotenv import load_dotenv
from datetime import datetime
import os
import requests

load_dotenv()

api_key = os.getenv("CTA_API_KEY")

url = "http://lapi.transitchicago.com/api/1.0/ttarrivals.aspx"

params = {
    "key": api_key,
    "mapid": 40380
}

response = requests.get(url, params=params)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

filename = f"data/raw/arrivals/{timestamp}.xml"

with open(filename, "w", encoding="utf-8") as f:
    f.write(response.text)

print(f"Saved to {filename}")