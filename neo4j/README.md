# Neo4j

Neo4j is used as the graph layer for vessel, port, route, area, and encounter relationships.

## ClickHouse JDBC diagnostics

Neo4j can query ClickHouse through APOC Extended and the ClickHouse JDBC driver.

This JDBC bridge is intended only for:

- diagnostics
- cross-system validation
- rare reconciliation or recovery tasks

It is **not** part of the regular ingestion pipeline.

Normal ingestion remains:

```text
Live AIS        -> Kafka   -> Neo4j
Historical AIS  -> Airflow -> ClickHouse
Derived graph   -> Airflow -> Neo4j
```

For local installation, follow the [fresh-clone quickstart](../README.md#fresh-clone-quickstart).
