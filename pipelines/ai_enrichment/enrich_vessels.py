import argparse
import hashlib
import json
import os

from datetime import date, datetime, timedelta, timezone
from typing import Literal

import clickhouse_connect

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel


load_dotenv()


MODEL = os.getenv("OPENAI_MODEL", "gpt-6-luna")
PROMPT_VERSION = "vessel_daily_v1"
DEFAULT_LIMIT = int(
    os.getenv("AI_ENRICHMENT_LIMIT", "100")
)


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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Enrich daily vessel features using the OpenAI API."
    )

    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        help=(
            "Activity date in YYYY-MM-DD format. "
            "Defaults to yesterday UTC."
        ),
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Maximum number of new vessel-days to enrich (default: AI_ENRICHMENT_LIMIT or 100).",
    )

    args = parser.parse_args()

    if args.limit <= 0:
        parser.error("--limit must be greater than 0")

    return args


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
    )


def get_vessels(
    clickhouse,
    activity_date: date,
) -> list[dict]:
    query = """
        SELECT
            mmsi AS mmsi,
            activity_date AS activity_date,
            vessel_name AS vessel_name,
            ship_type_name AS ship_type_name,
            ship_category AS ship_category,

            ais_points AS ais_points,

            round(
                toFloat64(observation_hours),
                2
            ) AS observation_hours,

            round(
                toFloat64(avg_speed_kn),
                2
            ) AS avg_speed_kn,

            round(
                toFloat64(max_speed_kn),
                2
            ) AS max_speed_kn,

            round(
                toFloat64(stationary_observation_pct),
                1
            ) AS stationary_observation_pct,

            dominant_navigational_status_name
                AS dominant_navigational_status_name,

            round(
                toFloat64(dominant_status_pct),
                1
            ) AS dominant_status_pct,

            round(
                toFloat64(nav_status_change_rate_pct),
                1
            ) AS nav_status_change_rate_pct

        FROM analytics.vessel_daily_features

        WHERE activity_date = {activity_date:Date}

        ORDER BY
            mmsi ASC
    """

    result = clickhouse.query(
        query,
        parameters={
            "activity_date": activity_date,
        },
    )

    return [
        dict(zip(result.column_names, row))
        for row in result.result_rows
    ]


def calculate_input_hash(vessel: dict) -> str:
    payload = json.dumps(
        vessel,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


def get_existing_hashes(
    clickhouse,
    activity_date: date,
) -> set[tuple[int, str]]:
    query = """
        SELECT
            mmsi AS mmsi,
            input_hash AS input_hash

        FROM analytics.vessel_ai_enrichment

        WHERE activity_date = {activity_date:Date}
          AND model = {model:String}
          AND prompt_version = {prompt_version:String}
    """

    result = clickhouse.query(
        query,
        parameters={
            "activity_date": activity_date,
            "model": MODEL,
            "prompt_version": PROMPT_VERSION,
        },
    )

    return {
        (
            row[0],
            (
                row[1].decode("utf-8")
                if isinstance(row[1], bytes)
                else row[1]
            ),
        )
        for row in result.result_rows
    }


def build_prompt(vessel: dict) -> str:
    vessel_json = json.dumps(
        vessel,
        indent=2,
        default=str,
    )

    return f"""
Analyze this vessel's daily AIS-derived features.

Use only the supplied data.
Do not invent vessel activity that cannot be supported by the features.

Interpretation rules:

- stationary_observation_pct is the percentage of valid AIS
  speed observations below the stationary threshold. It is
  not elapsed stationary time.

- dominant_status_pct measures how strongly one reported
  navigational status dominates the day's observations.

- nav_status_change_rate_pct is the percentage of comparable
  adjacent AIS observations whose reported navigational
  status changed.

- A high navigation-status change rate combined with a low
  dominant-status percentage should be treated as unreliable
  or inconsistent navigational-status data.

- Do not interpret every reported status change as a real
  operational vessel-state transition.

- When describing movement, refer to AIS speed observations
  or the observed period.

- Do not claim continuous movement between AIS observations.

Vessel features:

{vessel_json}
""".strip()


def enrich_vessel(
    openai_client: OpenAI,
    vessel: dict,
) -> tuple[VesselAIEnrichment, object]:
    response = openai_client.responses.parse(
        model=MODEL,
        input=[
            {
                "role": "system",
                "content": (
                    "You analyze engineered AIS vessel features. "
                    "Return concise, conservative analytics. "
                    "Distinguish observed movement characteristics "
                    "from uncertain or inconsistent AIS metadata."
                ),
            },
            {
                "role": "user",
                "content": build_prompt(vessel),
            },
        ],
        text_format=VesselAIEnrichment,
    )

    if response.output_parsed is None:
        raise RuntimeError(
            f"No parsed output returned for MMSI {vessel['mmsi']}"
        )

    return response.output_parsed, response


def insert_enrichment(
    clickhouse,
    vessel: dict,
    input_hash: str,
    enrichment: VesselAIEnrichment,
    response,
) -> None:
    usage = response.usage

    input_tokens = (
        usage.input_tokens
        if usage is not None
        else 0
    )

    output_tokens = (
        usage.output_tokens
        if usage is not None
        else 0
    )

    row = [
        vessel["mmsi"],
        vessel["activity_date"],

        enrichment.activity_class,
        enrichment.navigation_status_quality,

        enrichment.summary,
        enrichment.notable_behavior,
        enrichment.data_quality_note,

        MODEL,
        PROMPT_VERSION,
        input_hash,

        response.id,
        input_tokens,
        output_tokens,

        datetime.now(timezone.utc),
    ]

    clickhouse.insert(
        "analytics.vessel_ai_enrichment",
        [row],
        column_names=[
            "mmsi",
            "activity_date",

            "activity_class",
            "navigation_status_quality",

            "summary",
            "notable_behavior",
            "data_quality_note",

            "model",
            "prompt_version",
            "input_hash",

            "openai_response_id",
            "input_tokens",
            "output_tokens",

            "created_at",
        ],
    )


def main():
    args = parse_args()

    target_date = (
        args.date
        if args.date
        else datetime.now(timezone.utc).date() - timedelta(days=1)
    )

    print(f"AI enrichment date: {target_date}")
    print(f"Model: {MODEL}")
    print(f"Prompt version: {PROMPT_VERSION}")
    print(f"Limit: {args.limit}")

    clickhouse = get_clickhouse_client()
    openai_client = OpenAI()

    vessels = get_vessels(
        clickhouse=clickhouse,
        activity_date=target_date,
    )

    existing_hashes = get_existing_hashes(
        clickhouse=clickhouse,
        activity_date=target_date,
    )

    pending_vessels = []
    already_enriched = 0

    for vessel in vessels:
        input_hash = calculate_input_hash(vessel)

        key = (
            vessel["mmsi"],
            input_hash,
        )

        if key in existing_hashes:
            already_enriched += 1
            continue

        pending_vessels.append(
            {
                "vessel": vessel,
                "input_hash": input_hash,
            }
        )

    pending_vessels = pending_vessels[: args.limit]

    print(f"Vessel-days found: {len(vessels)}")
    print(f"Already enriched: {already_enriched}")
    print(f"Selected for enrichment: {len(pending_vessels)}")
    print()

    processed = 0
    failed = 0
    interrupted = False

    for item in pending_vessels:
        vessel = item["vessel"]
        input_hash = item["input_hash"]

        try:
            enrichment, response = enrich_vessel(
                openai_client=openai_client,
                vessel=vessel,
            )

            insert_enrichment(
                clickhouse=clickhouse,
                vessel=vessel,
                input_hash=input_hash,
                enrichment=enrichment,
                response=response,
            )

            processed += 1

            print(
                "OK  "
                f"{vessel['mmsi']} "
                f"{vessel['vessel_name']} "
                f"-> {enrichment.activity_class}"
            )

        except KeyboardInterrupt:
            print()
            print("Interrupted by user.")
            interrupted = True
            break

        except Exception as exc:
            failed += 1

            print(
                "ERROR "
                f"{vessel['mmsi']} "
                f"{vessel['vessel_name']}: "
                f"{exc}"
            )

    print()
    print("Completed")
    print(f"Processed: {processed}")
    print(f"Failed:    {failed}")

    if interrupted or failed:
        raise SystemExit(130 if interrupted else 1)


if __name__ == "__main__":
    main()
