"""Full-batch raw-to-curated HAIS transformation; no source-file ingestion."""
import json
from decimal import Decimal

from pyspark.sql import functions as F
from spark_session import create_spark

SOURCE = "iceberg.hais_raw.ais_positions"
TARGET = "iceberg.hais_curated.ais_positions"
REQUIRED = (
    "date_time", "mmsi", "longitude", "latitude", "navigational_status",
    "maneuver", "data_source", "ais_class", "message_type", "geometry",
)
CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    date_time TIMESTAMP NOT NULL,
    mmsi INT NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    latitude DECIMAL(8,6) NOT NULL,
    navigational_status INT NOT NULL,
    course_over_ground DECIMAL(5,2),
    true_heading INT,
    speed_over_ground DECIMAL(5,2),
    rate_of_turn INT,
    maneuver INT NOT NULL,
    data_source STRING NOT NULL,
    ais_class STRING NOT NULL,
    message_type INT NOT NULL,
    geometry BINARY NOT NULL
) USING iceberg
PARTITIONED BY (days(date_time))
TBLPROPERTIES ('format-version'='2')
"""


def transform(raw):
    cog = F.col("course_over_ground")
    return raw.select(
        F.col("date_time_utc").alias("date_time"),
        F.col("mmsi"),
        F.col("longitude").cast("decimal(9,6)").alias("longitude"),
        F.col("latitude").cast("decimal(8,6)").alias("latitude"),
        F.col("status").alias("navigational_status"),
        F.when((cog >= 0) & (cog < 360), cog)
            .cast("decimal(5,2)").alias("course_over_ground"),
        F.when(F.col("true_heading") != 511, F.col("true_heading")).alias("true_heading"),
        F.when(F.col("speed_over_ground") != F.lit(Decimal("102.30")), F.col("speed_over_ground"))
            .cast("decimal(5,2)").alias("speed_over_ground"),
        F.when(F.col("rate_of_turn") != -128, F.col("rate_of_turn")).alias("rate_of_turn"),
        F.col("maneuvre").alias("maneuver"),
        F.col("data_source"), F.col("ais_class"),
        F.col("msg_type").alias("message_type"), F.col("geometry"),
    )


def main():
    spark = create_spark("HAIS curated Iceberg transformation")
    # Decimal overflow must fail, rather than silently add unrequested NULLs.
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        raw = spark.table(SOURCE)
        source_count = raw.count()
        curated = transform(raw)
        curated.printSchema()
        nulls = curated.agg(*[
            F.count(F.when(F.col(name).isNull(), 1)).alias(name) for name in REQUIRED
        ]).first().asDict()
        if any(nulls.values()):
            raise ValueError("Required curated fields contain NULLs: " + json.dumps(nulls))
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.hais_curated "
                  "LOCATION 's3://warehouse/hais_curated/'")
        spark.sql(CREATE_TABLE)
        # Required fields are checked above. The projection still carries
        # nullable Spark schema flags; retain actual Iceberg NOT NULL fields.
        curated.writeTo(TARGET).option("check-nullability", "false").overwrite(F.lit(True))
        result_count = spark.table(TARGET).count()
        if result_count != source_count:
            raise RuntimeError(f"Curated count {result_count} != raw count {source_count}")
        spark.sql(f"SHOW CREATE TABLE {TARGET}").show(truncate=False)
        print("CURATED_RESULT=" + json.dumps({
            "source": SOURCE, "target": TARGET,
            "raw_count": source_count, "curated_count": result_count,
        }), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
