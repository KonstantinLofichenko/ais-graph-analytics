// Clear the previous snapshot's GDS results, including Ports no longer active.
MATCH (port:Port)
REMOVE port.pageRank, port.communityId;

// Persist results on active Port nodes. Reuses the two projected snapshots.
CALL gds.pageRank.write('ais-port-connections-directed', {
  relationshipWeightProperty: 'movementCount',
  maxIterations: 20,
  dampingFactor: 0.85,
  concurrency: 1,
  writeProperty: 'pageRank'
})
YIELD nodePropertiesWritten, ranIterations, didConverge
RETURN nodePropertiesWritten, ranIterations, didConverge;

CALL gds.louvain.write('ais-port-connections-undirected', {
  relationshipWeightProperty: 'movementCount',
  concurrency: 1,
  writeProperty: 'communityId'
})
YIELD nodePropertiesWritten, communityCount, modularity, communityDistribution
RETURN nodePropertiesWritten, communityCount, modularity, communityDistribution;

// Inspect the properties persisted by the two calls above.
MATCH (port:Port)
RETURN port.portId AS portId, port.name AS name,
       port.pageRank AS pageRank, port.communityId AS communityId
ORDER BY pageRank DESC, portId;
