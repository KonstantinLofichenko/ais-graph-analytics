mkdir -p neo4j/cypher

cat > neo4j/cypher/clickhouse_jdbc_diagnostics.cypher <<'EOF'
// Neo4j -> ClickHouse JDBC diagnostics
//
// Diagnostic/admin use only.
// Normal ingestion remains Kafka/Airflow based.
//
// In Neo4j Browser set:
//
// :param clickhouse_user => 'default';
// :param clickhouse_password => '...';
//
// Never commit a real password.

// -----------------------------------------------------------------------------
// 1. Count distinct vessels in ClickHouse
// -----------------------------------------------------------------------------

CALL apoc.load.jdbc(
  'clickhouse',
  'SELECT count(DISTINCT mmsi) AS unique_mmsi FROM raw.ais_positions FINAL',
  [],
  {
    credentials: {
      user: $clickhouse_user,
      password: $clickhouse_password
    }
  }
)
YIELD row
RETURN row.unique_mmsi;


// -----------------------------------------------------------------------------
// 2. List ClickHouse vessels missing in Neo4j
// -----------------------------------------------------------------------------

CALL apoc.load.jdbc(
  'clickhouse',
  'SELECT DISTINCT mmsi FROM raw.ais_positions FINAL',
  [],
  {
    credentials: {
      user: $clickhouse_user,
      password: $clickhouse_password
    }
  }
)
YIELD row
WITH toInteger(row.mmsi) AS mmsi
OPTIONAL MATCH (v:Vessel {mmsi: mmsi})
WITH mmsi, v
WHERE v IS NULL
RETURN mmsi
ORDER BY mmsi;


// -----------------------------------------------------------------------------
// 3. Count ClickHouse vessels missing in Neo4j
// -----------------------------------------------------------------------------

CALL apoc.load.jdbc(
  'clickhouse',
  'SELECT DISTINCT mmsi FROM raw.ais_positions FINAL',
  [],
  {
    credentials: {
      user: $clickhouse_user,
      password: $clickhouse_password
    }
  }
)
YIELD row
WITH toInteger(row.mmsi) AS mmsi
OPTIONAL MATCH (v:Vessel {mmsi: mmsi})
WITH v
WHERE v IS NULL
RETURN count(*) AS missing_vessels;


// -----------------------------------------------------------------------------
// 4. List Neo4j vessels missing in ClickHouse
// -----------------------------------------------------------------------------

CALL apoc.load.jdbc(
  'clickhouse',
  'SELECT DISTINCT mmsi FROM raw.ais_positions FINAL',
  [],
  {
    credentials: {
      user: $clickhouse_user,
      password: $clickhouse_password
    }
  }
)
YIELD row
WITH collect(toInteger(row.mmsi)) AS clickhouse_mmsis
MATCH (v:Vessel)
WHERE NOT v.mmsi IN clickhouse_mmsis
RETURN
  v.mmsi AS mmsi,
  v.name AS name,
  v.lastSeen AS lastSeen
ORDER BY mmsi;


// -----------------------------------------------------------------------------
// 5. Count Neo4j vessels missing in ClickHouse
// -----------------------------------------------------------------------------

CALL apoc.load.jdbc(
  'clickhouse',
  'SELECT DISTINCT mmsi FROM raw.ais_positions FINAL',
  [],
  {
    credentials: {
      user: $clickhouse_user,
      password: $clickhouse_password
    }
  }
)
YIELD row
WITH collect(toInteger(row.mmsi)) AS clickhouse_mmsis
MATCH (v:Vessel)
WHERE NOT v.mmsi IN clickhouse_mmsis
RETURN count(v) AS neo4j_only_vessels;


// -----------------------------------------------------------------------------
// 6. Rare reconciliation: create vessels missing from Neo4j
//
// WARNING:
// This query WRITES to Neo4j.
// Use only for rare reconciliation/recovery.
// Normal ingestion must remain Kafka/Airflow based.
// -----------------------------------------------------------------------------

CALL apoc.load.jdbc(
  'clickhouse',
  '
  SELECT
      mmsi,
      argMax(name, msgtime) AS name,
      argMax(ship_type, msgtime) AS ship_type,
      argMax(latitude, msgtime) AS latitude,
      argMax(longitude, msgtime) AS longitude,
      argMax(speed_over_ground, msgtime) AS speed_over_ground,
      argMax(course_over_ground, msgtime) AS course_over_ground,
      argMax(true_heading, msgtime) AS true_heading,
      argMax(navigational_status, msgtime) AS navigational_status,
      argMax(stream, msgtime) AS stream,
      max(msgtime) AS last_seen
  FROM raw.ais_positions FINAL
  GROUP BY mmsi
  ',
  [],
  {
    credentials: {
      user: $clickhouse_user,
      password: $clickhouse_password
    }
  }
)
YIELD row
WITH
  toInteger(row.mmsi) AS mmsi,
  row.name AS name,
  row.ship_type AS shipType,
  row.latitude AS latitude,
  row.longitude AS longitude,
  row.speed_over_ground AS speedOverGround,
  row.course_over_ground AS courseOverGround,
  row.true_heading AS trueHeading,
  row.navigational_status AS navigationalStatus,
  row.stream AS stream,
  row.last_seen AS lastSeen

OPTIONAL MATCH (existing:Vessel {mmsi: mmsi})
WITH *, existing
WHERE existing IS NULL

MERGE (v:Vessel {mmsi: mmsi})
ON CREATE SET
  v.createdAt = datetime(),
  v.name = name,
  v.shipType = shipType,
  v.lastLatitude = latitude,
  v.lastLongitude = longitude,
  v.lastSpeedOverGround = speedOverGround,
  v.lastCourseOverGround = courseOverGround,
  v.lastTrueHeading = trueHeading,
  v.lastNavigationalStatus = navigationalStatus,
  v.lastStream = stream,
  v.lastSeen = datetime(toString(lastSeen)),
  v.updatedAt = datetime()

RETURN count(v) AS vessels_created;
EOF