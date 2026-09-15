// Rebuild these two in-memory snapshots after CONNECTED_TO publishing completes.
// Dropping a GDS graph does not delete stored nodes, relationships or properties.
CALL gds.graph.drop('ais-port-connections-directed', false) YIELD graphName;
CALL gds.graph.drop('ais-port-connections-undirected', false) YIELD graphName;

// PageRank follows the stored source -> destination direction.
// Project only active Ports participating in managed connections.
MATCH (source:Port)-[r:CONNECTED_TO {managedBy: 'port-connections-v1'}]->(target:Port)
WITH gds.graph.project(
  'ais-port-connections-directed',
  source,
  target,
  {
    sourceNodeLabels: ['Port'],
    targetNodeLabels: ['Port'],
    relationshipType: 'CONNECTED_TO',
    relationshipProperties: r { .movementCount }
  }
) AS graph
RETURN graph.graphName AS graphName, graph.nodeCount AS nodeCount,
       graph.relationshipCount AS relationshipCount;

// Louvain treats each stored movement relationship as undirected.
// Reciprocal routes retain both weights; neither direction is discarded.
MATCH (source:Port)-[r:CONNECTED_TO {managedBy: 'port-connections-v1'}]->(target:Port)
WITH gds.graph.project(
  'ais-port-connections-undirected',
  source,
  target,
  {
    sourceNodeLabels: ['Port'],
    targetNodeLabels: ['Port'],
    relationshipType: 'CONNECTED_TO',
    relationshipProperties: r { .movementCount }
  },
  {undirectedRelationshipTypes: ['CONNECTED_TO']}
) AS graph
RETURN graph.graphName AS graphName, graph.nodeCount AS nodeCount,
       graph.relationshipCount AS relationshipCount;
