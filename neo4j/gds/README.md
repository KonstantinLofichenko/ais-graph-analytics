# Port network analytics with Neo4j GDS

Manual Cypher workflow for the current published Port network. Compose already
enables the `graph-data-science` plugin; these scripts require no image rebuild.
Run statements in Neo4j Browser (http://localhost:7474) using the same user and
the `neo4j` database throughout. Check the installed GDS version with:

```cypher
RETURN gds.version() AS gdsVersion;
```

## Run order

| File | Purpose |
| --- | --- |
| [01_project_graphs.cypher](01_project_graphs.cypher) | Recreate directed and undirected in-memory graphs. |
| [02_pagerank.cypher](02_pagerank.cypher) | Stream directed PageRank weighted by `movementCount`. |
| [03_louvain.cypher](03_louvain.cypher) | Stream undirected Louvain communities with the same weight. |
| [04_louvain_stats.cypher](04_louvain_stats.cypher) | Report community count, modularity, levels and community-size distribution. |
| [05_write_back.cypher](05_write_back.cypher) | Clear previous scores from all Ports, write current `pageRank` and `communityId`, then inspect them. |

Execute each semicolon-delimited statement in order. Steps 02–04 require step 01;
step 05 first removes `pageRank` and `communityId` from all Port nodes, then
recomputes the algorithms with the same settings and writes the current results
on active Ports. The cleanup and two write calls commit separately; a failure can
leave cleared or partially written results. Rerun step 05 after resolving the
failure. Only step 05 changes database properties.

## Input and interpretation

Both projections include only active `:Port` nodes that are a source or destination
of a `CONNECTED_TO` relationship managed by `port-connections-v1`. Isolated Ports
and retained Ports without managed connections are excluded. This selects the
Airflow publisher's current snapshot. Manual connections and other relationship
types are excluded. Each relationship supplies its stored
positive `movementCount`; no substitute weight is assigned to a missing value.

PageRank follows the stored source-to-destination direction. Its weighted score
reflects incoming connections and the scores of their source Ports. The scripts
use a damping factor of 0.85 and at most 20 iterations; the write result reports
`didConverge` so the iteration limit can be revisited when needed.

Louvain uses a separate undirected projection. If both A → B and B → A exist,
both movement weights contribute. GDS represents each undirected relationship in
both directions in memory, so its relationship count is twice the directed count.
Community IDs are cluster labels, not ranks or stable business identifiers.
Stream, stats and write are separate executions; do not rely on identical numeric
community IDs across executions or changed snapshots. All algorithm calls use concurrency 1.

`CONNECTED_TO` means the next **detected** port visit was the destination. It does
not establish that no unobserved port was visited between the two detections.
These scores and communities describe the observed snapshot, not complete traffic.

## Refresh and cleanup

Finish Airflow connection publishing before step 01 and keep that snapshot stable
while running the workflow. GDS graphs are in-memory snapshots, not live views:
after another publication, rerun step 01 and then the analyses/write-back. The
publisher does not refresh `pageRank` or `communityId`; step 05 clears both
properties from all Ports before writing current active-port results. Ports no
longer in the snapshot retain neither property. If no managed connections exist,
run only the cleanup statement in step 05; there is no active network to analyze
or write back.

To release only this workflow's in-memory graphs:

```cypher
CALL gds.graph.drop('ais-port-connections-directed', false) YIELD graphName;
CALL gds.graph.drop('ais-port-connections-undirected', false) YIELD graphName;
```

Dropping a projection leaves stored nodes, relationships and written properties intact.

## GDS references

- [Cypher projection](https://neo4j.com/docs/graph-data-science/current/management-ops/graph-creation/graph-project-cypher-projection/)
- [PageRank](https://neo4j.com/docs/graph-data-science/current/algorithms/page-rank/)
- [Louvain](https://neo4j.com/docs/graph-data-science/current/algorithms/louvain/)
