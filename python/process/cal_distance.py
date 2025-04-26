import pandas as pd
import math
import json

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

# Load your parquet file
df = pd.read_parquet("parquet/3.parquet")

# Fix single quotes to double quotes and convert to dict
def parse_json(s):
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s.replace("'", '"'))
    except Exception:
        return {}

df["json_v"] = df["json_v"].apply(parse_json)

# Extract lat/lon
df["latitude"] = df["json_v"].apply(lambda x: x.get("latitude"))
df["longitude"] = df["json_v"].apply(lambda x: x.get("longitude"))

# Sort by timestamp
df = df.sort_values("ts").reset_index(drop=True)

# Compute total distance
total_distance = 0.0
for i in range(1, len(df)):
    lat1, lon1 = df.loc[i - 1, ["latitude", "longitude"]]
    lat2, lon2 = df.loc[i, ["latitude", "longitude"]]
    if None not in (lat1, lon1, lat2, lon2):
        total_distance += haversine(lat1, lon1, lat2, lon2)

print(f"Total distance traveled: {total_distance:.2f} km")
