CREATE CONSTRAINT vessel_mmsi_unique IF NOT EXISTS
FOR (v:Vessel)
REQUIRE v.mmsi IS UNIQUE;

CREATE CONSTRAINT connection_snapshot_managed_by_unique IF NOT EXISTS
FOR (s:ConnectionSnapshot)
REQUIRE s.managedBy IS UNIQUE;
