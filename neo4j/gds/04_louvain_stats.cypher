// Same graph and weighting as the Louvain preview and write-back.
CALL gds.louvain.stats('ais-port-connections-undirected', {
  relationshipWeightProperty: 'movementCount',
  concurrency: 1
})
YIELD communityCount, modularity, modularities, ranLevels, communityDistribution
RETURN communityCount, modularity, modularities, ranLevels, communityDistribution;
