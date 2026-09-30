-- Additive lookup only; never rewrite or delete vessel_ai_enrichment history.
-- input_hash remains the existing Python SHA-256 of sorted compact JSON.
-- input_values is the exact typed-value representation from the shared dbt view.
-- Repeated mappings are identical; FINAL exposes one mapping per input value set.
CREATE TABLE IF NOT EXISTS analytics.vessel_ai_input_hashes
(
    input_values String,
    input_hash FixedString(64)
)
ENGINE = ReplacingMergeTree
ORDER BY input_values;
