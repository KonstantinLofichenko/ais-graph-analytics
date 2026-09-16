-- Convenient analytical source for Metabase, joining metrics to their dimensions.
-- analytics.port_graph_metrics contains snapshot-specific GDS metrics exported from Neo4j.
-- analytics.ports supplies descriptive port attributes and geographic coordinates.
-- analytics.countries supplies country reference information.
-- Keep these descriptive dimensions separate; do not duplicate them into analytics.port_graph_metrics.
-- Apply after the ports table and migrations 002_countries.sql / 003_port_graph_metrics.sql.
CREATE OR REPLACE VIEW analytics.port_graph_metrics_enriched AS
SELECT
    g.run_id,
    g.window_start,
    g.window_end,
    g.snapshot_date,

    g.port_id AS port_id,
    p.name AS port_name,

    p.country AS country_code,
    c.country_name,
    c.iso3,
    c.numeric_code,

    p.latitude,
    p.longitude,

    g.page_rank,
    g.community_id,

    g.exported_at
FROM
(
    SELECT *
    FROM analytics.port_graph_metrics FINAL
) AS g
LEFT JOIN analytics.ports AS p FINAL
    ON g.port_id = p.port_id
LEFT JOIN analytics.countries AS c FINAL
    ON p.country = c.country_code;
