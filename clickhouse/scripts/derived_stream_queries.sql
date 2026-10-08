-- Query persistent tables only; these queries never consume Kafka Engine tables.
SELECT mmsi, name, window_minutes, window_start, window_end,
       position_count, avg_speed, min_speed, max_speed
FROM analytics.ais_vessel_features
ORDER BY window_end DESC
LIMIT 20;

SELECT *
FROM analytics.ais_vessel_gap_events
ORDER BY gap_detected_at DESC
LIMIT 20;

SELECT *
FROM analytics.ais_vessel_gap_events
WHERE event_type = 'AIS_GAP_ENDED'
  AND gap_duration_seconds >= 1800
ORDER BY gap_duration_seconds DESC
LIMIT 20;

SELECT window_minutes, count()
FROM analytics.ais_vessel_features
GROUP BY window_minutes
ORDER BY window_minutes;

SELECT event_type, count()
FROM analytics.ais_vessel_gap_events
GROUP BY event_type
ORDER BY event_type;

-- One matching lifecycle; detection-to-recovery time is NOT gap_duration_seconds.
WITH
    (SELECT (mmsi, gap_detected_at)
     FROM analytics.ais_vessel_gap_events
     GROUP BY mmsi, gap_detected_at
     HAVING countIf(event_type = 'AIS_GAP_DETECTED') > 0
        AND countIf(event_type = 'AIS_GAP_ENDED') > 0
     ORDER BY gap_detected_at DESC
     LIMIT 1) AS lifecycle
SELECT *
FROM analytics.ais_vessel_gap_events
WHERE (mmsi, gap_detected_at) = lifecycle
ORDER BY coalesce(gap_ended_at, gap_detected_at), event_type;

-- Stored Kafka/MV exceptions remain visible even after recovery. Check their times
-- and committed offsets before deciding that a consumer is currently stalled.
SELECT database, table, num_messages_read, num_commits, last_commit_time,
       exceptions.time, exceptions.text
FROM system.kafka_consumers
WHERE database = 'raw'
  AND table IN ('ais_vessel_features_kafka', 'ais_vessel_gap_events_kafka');
