import os

from dotenv import load_dotenv
from typing import Literal

import clickhouse_connect
from openai import OpenAI
from pydantic import BaseModel

load_dotenv()

class VesselAIEnrichment(BaseModel):
    activity_class: Literal[
        "mostly_stationary",
        "continuous_movement",
        "mixed_activity",
        "insufficient_data",
    ]

    navigation_status_quality: Literal[
        "high",
        "medium",
        "low",
        "unknown",
    ]

    summary: str
    notable_behavior: str
    data_quality_note: str | None

clickhouse = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD"),
)

query = """
SELECT
    mmsi AS mmsi,
    activity_date AS activity_date,
    vessel_name AS vessel_name,
    ship_type_name AS ship_type_name,
    ship_category AS ship_category,
    ais_points AS ais_points,
    round(toFloat64(observation_hours), 2) AS observation_hours,
    round(toFloat64(avg_speed_kn), 2) AS avg_speed_kn,
    round(toFloat64(max_speed_kn), 2) AS max_speed_kn,
    round(toFloat64(stationary_observation_pct), 1) AS stationary_observation_pct,
    dominant_navigational_status_name AS dominant_navigational_status_name,
    round(toFloat64(dominant_status_pct), 1) AS dominant_status_pct,
    round(toFloat64(nav_status_change_rate_pct), 1) AS nav_status_change_rate_pct
FROM analytics.vessel_daily_features
WHERE mmsi = 259014700 -- 273332880, 992572500, 259014700
  AND activity_date = toDate('2026-09-22')
LIMIT 1
"""

result = clickhouse.query(query)

if not result.result_rows:
    raise RuntimeError("No vessel-day row found")

columns = result.column_names
values = result.result_rows[0]

vessel = dict(zip(columns, values))

print("Input:")
print(vessel)


prompt = f"""
Analyze this vessel's daily AIS-derived features.

Use only the supplied data.
Do not invent vessel activity that cannot be supported by the features.

Important interpretation rules:
- stationary_observation_pct is the percentage of valid AIS speed
  observations below the stationary threshold; it is not elapsed
  stationary time.
- dominant_status_pct measures how strongly one navigational status
  dominates the day's observations.
- nav_status_change_rate_pct is the percentage of comparable adjacent
  AIS observations whose reported navigational status changed.
- A high navigation-status change rate combined with a low dominant
  status percentage should be treated as unreliable or inconsistent
  navigational-status data.
- Do not interpret every reported status change as a real operational
  vessel-state transition.

Vessel features:

MMSI: {vessel["mmsi"]}
Date: {vessel["activity_date"]}
Vessel name: {vessel["vessel_name"]}
Ship type: {vessel["ship_type_name"]}
Ship category: {vessel["ship_category"]}

AIS observations: {vessel["ais_points"]}
Observation hours: {vessel["observation_hours"]}
Average speed: {vessel["avg_speed_kn"]} knots
Maximum speed: {vessel["max_speed_kn"]} knots
Stationary observations: {vessel["stationary_observation_pct"]}%

Dominant navigational status:
{vessel["dominant_navigational_status_name"]}

Dominant status share:
{vessel["dominant_status_pct"]}%

Navigation-status change rate:
{vessel["nav_status_change_rate_pct"]}%
"""


openai_client = OpenAI()

response = openai_client.responses.parse(
    model="gpt-6-luna",
    input=[
        {
            "role": "system",
            "content": (
                "You analyze engineered AIS vessel features. "
                "Return concise, conservative analytics. "
                "Distinguish observed movement characteristics from "
                "uncertain or inconsistent AIS metadata."
            ),
        },
        {
            "role": "user",
            "content": prompt,
        },
    ],
    text_format=VesselAIEnrichment,
)

enrichment = response.output_parsed

print("\nAI enrichment:")
print(enrichment.model_dump_json(indent=2))