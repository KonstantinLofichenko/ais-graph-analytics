CREATE CONSTRAINT vessel_mmsi_unique IF NOT EXISTS
FOR (v:Vessel)
REQUIRE v.mmsi IS UNIQUE;
