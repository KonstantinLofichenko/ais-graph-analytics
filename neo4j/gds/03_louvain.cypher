// Undirected weighted Louvain: preview each Port's community assignment.
CALL gds.louvain.stream('ais-port-connections-undirected', {
  relationshipWeightProperty: 'movementCount',
  concurrency: 1
})
YIELD nodeId, communityId
WITH gds.util.asNode(nodeId) AS port, communityId
RETURN communityId, port.portId AS portId, port.name AS name
ORDER BY communityId, portId;
