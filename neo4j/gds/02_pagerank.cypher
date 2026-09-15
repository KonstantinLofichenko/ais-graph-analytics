// Directed weighted PageRank: preview results without writing node properties.
CALL gds.pageRank.stream('ais-port-connections-directed', {
  relationshipWeightProperty: 'movementCount',
  maxIterations: 20,
  dampingFactor: 0.85,
  concurrency: 1
})
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS port, score
RETURN port.portId AS portId, port.name AS name, score AS pageRank
ORDER BY pageRank DESC, portId;
