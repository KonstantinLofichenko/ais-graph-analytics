# Neo4j

Neo4j complements ClickHouse as the current graph and relationship layer; it is
not the system of record for raw AIS history. Kafka Connect maintains Vessel
nodes from `ais.positions`. The analytical port-visit workflow adds Port nodes and
`VISITED`/`CONNECTED_TO` relationships; GDS PageRank and Louvain derive metrics and
Community nodes linked by `MEMBER_OF`. Graph snapshots are exported to ClickHouse
and dbt models for Metabase and downstream analysis.

See the [canonical architecture](../docs/README.md#2-architecture) and
[graph metrics workflow](../pipelines/graph_metrics/README.md).

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

For local installation, follow the [fresh-clone quickstart](../README.md#fresh-clone-quick-start).
