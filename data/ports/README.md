# Bergen pilot port reference

`bergen.json` contains seven World Port Index (NGA) port locations, retrieved
2026-09-12 from the UN OCHA-hosted `global_world_seaport_index_202511` snapshot:

https://gis.unocha.org/server/rest/services/Hosted/global_world_seaport_index_202511/FeatureServer/0

Source publication: https://msi.nga.mil/Publications/WPI

Query: `country='NO' AND latitude >= 60 AND latitude <= 61 AND longitude >= 4 AND longitude <= 6`.
Fields: `index_no,port_name,country,latitude,longitude`; geometry omitted.
`port_id` is `WPI:` plus the stable index number. Names/coordinates are preserved.
The source returned seven records and `exceededTransferLimit=false`.
This is a dated pilot extract, not a claim to current or complete port coverage.

`radius_m=1500` is our provisional detection parameter, NOT an official port boundary.
These approximate point locations/radii need review against actual AIS trajectories
before interpreting results as confirmed port calls. Nearby anchorages and passing
traffic can otherwise be misclassified. Detection results are inferred observations,
not official arrival/departure records.

A repeatable downloader now exists at `pipelines/port_visits/download_ports.py`.
The Airflow `download_ports` task uses it without overwriting this checked-in sample.
