# Architecture diagram assets

[`architecture.png`](architecture.png) is the single primary architecture diagram
embedded in the root and detailed READMEs. [`architecture.svg`](architecture.svg)
is the matching zoomable vector version. Both represent the same layout and
content, defined in [`render_architecture.py`](render_architecture.py).

The diagram distinguishes raw AIS history, two PyFlink derived streams and the
Neo4j/GDS path. Metabase reads their persistent ClickHouse outputs. The supporting
recovery band uses dotted links for checkpoint/restore operations, not AIS record
flow. The existing HAIS-to-ClickHouse batch path is separate from the amber
**planned, undeployed** PySpark/Iceberg/Trino historical lakehouse path.

This is a high-level data/state-flow diagram: Compose plumbing, optional country
reference ingestion, individual daily DAG dependencies and infrastructure tools
are described in the [detailed documentation](../README.md#2-architecture) and
component guides rather than expanded into more diagram nodes.

## Regeneration

Use Python 3.9+ with Pillow and either Arial (macOS) or DejaVu Sans (Linux):

```sh
python3 -m venv /tmp/ais-diagram-venv
/tmp/ais-diagram-venv/bin/pip install pillow
/tmp/ais-diagram-venv/bin/python docs/assets/render_architecture.py
```

The renderer works from any directory and writes only the two architecture assets
beside the script. Edit the renderer's shared labels/layout, regenerate both files
and visually review the PNG. Do not update the PNG or SVG independently: a later
regeneration would overwrite manual edits. The renderer checks body-text widths;
review arrows, line spacing and clipping visually after layout changes.

`neo4j_graph.png` and `metabase.png` are illustrative runtime screenshots, not
alternate architecture diagrams. The English/Russian PDFs one directory above embed this same diagram and are
regenerated from the current documentation sources. See
[PDF regeneration](../README.md#pdf-regeneration).
