"""Opt-in Kafka -> MV -> MergeTree test using disposable topics and database.

Uses running Compose containers by default; never writes fixtures to live AIS topics.
Override CLICKHOUSE_TEST_CONTAINER, KAFKA_TEST_CONTAINER and KAFKA_TEST_BROKER
for isolated CI. Requires only Python stdlib and Docker CLI.
"""
from datetime import datetime, timezone
import json
import os
import re
from pathlib import Path
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
CH = os.getenv('CLICKHOUSE_TEST_CONTAINER', 'ais-clickhouse')
KAFKA = os.getenv('KAFKA_TEST_CONTAINER', 'ais-kafka')
BROKER = os.getenv('KAFKA_TEST_BROKER', 'ais-kafka:29092')


def command(args, payload=None):
    result = subprocess.run(args, input=payload, text=True, capture_output=True, timeout=60)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout


def query(sql):
    return command(['docker', 'exec', '-i', CH, 'sh', '-c',
                    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery'], sql)


def kafka(script, *args, payload=None):
    return command(['docker', 'exec', '-i', KAFKA, '/opt/kafka/bin/' + script,
                    '--bootstrap-server', BROKER, *args], payload)


def wait_for(check, label, seconds=60):
    deadline = time.monotonic() + seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except RuntimeError as error:
            last_error = str(error).splitlines()[0]
        time.sleep(1)
    raise AssertionError('Timed out: ' + label + (' (' + last_error + ')' if last_error else ''))


def rows(sql):
    return [json.loads(line) for line in query(sql + ' FORMAT JSONEachRow').splitlines()]


def timestamp(value):
    if value is None:
        return None
    # Python handles six digits; fixture source timestamps below separately verify all nine.
    value = re.sub(r'(\.\d{6})\d+', r'\1', value)
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def assert_payload(payload, row):
    for field, value in payload.items():
        if field.endswith('_at') or field.endswith('_msgtime') or field in ('window_start', 'window_end'):
            assert timestamp(value) == timestamp(row[field]), (field, value, row[field])
        else:
            assert value == row[field], (field, value, row[field])


def fixtures():
    features = []
    for minutes in (5, 15, 30, 60):
        features.append(dict(mmsi=999999991, name="Fixture vessel", window_minutes=minutes,
                             window_start='2026-10-07T12:00:00+03:00',
                             window_end=f'2026-10-07T09:{minutes:02}:00Z' if minutes < 60 else '2026-10-07T10:00:00Z',
                             position_count=42, avg_speed=3.535185185185185,
                             min_speed=0.0, max_speed=18.9876543210123,
                             ship_type=70, ship_type_name='Cargo', ship_category='Cargo',
                             last_navigation_status=0, last_navigation_status_name='Under way using engine'))
    # Explicit nulls and omitted enrichment fields must both survive as NULL.
    features.append(dict(mmsi=999999992, window_minutes=5,
                         window_start='2026-10-07T09:00:00Z', window_end='2026-10-07T09:05:00Z',
                         position_count=1, name=None, avg_speed=None, min_speed=None, max_speed=None))
    detected = dict(event_type='AIS_GAP_DETECTED', mmsi=999999991, name=None,
                    last_event_msgtime='2026-10-07T11:59:00.123456789+03:00',
                    last_latitude=60.123456789, last_longitude=5.987654321,
                    gap_detected_at='2026-10-06T23:59:00Z', gap_timeout_seconds=600,
                    ship_type=None, ship_type_name=None, ship_category=None,
                    navigation_status=1, navigation_status_name='At anchor')
    ended = dict(event_type='AIS_GAP_ENDED', mmsi=999999991, name='Fixture',
                 previous_event_msgtime=detected['last_event_msgtime'],
                 resumed_event_msgtime='2026-10-07T12:10:00.987654321+03:00',
                 gap_detected_at=detected['gap_detected_at'], gap_ended_at='2026-10-07T09:10:00Z',
                 gap_duration_seconds=660.987, ship_type=70, ship_type_name='Cargo', ship_category='Cargo',
                 previous_navigation_status=1, previous_navigation_status_name='At anchor',
                 resumed_navigation_status=None, resumed_navigation_status_name=None)
    null_detected = dict(event_type='AIS_GAP_DETECTED', mmsi=999999992,
                         last_event_msgtime=None, gap_detected_at='2026-10-07T09:09:00Z', gap_timeout_seconds=600)
    return features, [detected, ended, null_detected]


def main(verify_live_bootstrap=True):
    suffix = uuid.uuid4().hex
    database = 'derived_raw_test_' + suffix
    analytics = 'derived_analytics_test_' + suffix
    topics = ['ais.test.derived.' + suffix + '.features', 'ais.test.derived.' + suffix + '.gaps']
    created_topics = []
    wait_for(lambda: query('SELECT 1').strip() == '1', 'ClickHouse readiness')
    wait_for(lambda: kafka('kafka-broker-api-versions.sh'), 'Kafka readiness')
    if verify_live_bootstrap:
        objects_sql = "SELECT count() FROM system.tables WHERE (database='raw' AND name IN ('ais_vessel_features_kafka','ais_vessel_features_mv','ais_vessel_gap_events_kafka','ais_vessel_gap_events_mv')) OR (database='analytics' AND name IN ('ais_vessel_features','ais_vessel_gap_events'))"
        wait_for(lambda: query(objects_sql).strip() == '6', 'automatic derived bootstrap')
        assert query("SELECT count() FROM system.tables WHERE database='raw' AND name IN ('ais_vessel_features','ais_vessel_gap_events')").strip() == '0'
        print('PASS: automatic bootstrap separates raw transport and analytics destinations')
    try:
        for topic in topics:
            kafka('kafka-topics.sh', '--create', '--topic', topic, '--partitions', '1', '--replication-factor', '1')
            created_topics.append(topic)
        sql = (ROOT / 'clickhouse/init/04_flink_derived.sql').read_text()
        sql = sql.replace('raw.', database + '.').replace('analytics.', analytics + '.')
        sql = sql.replace('DATABASE IF NOT EXISTS raw;', 'DATABASE IF NOT EXISTS ' + database + ';')
        sql = sql.replace('DATABASE IF NOT EXISTS analytics;', 'DATABASE IF NOT EXISTS ' + analytics + ';')
        sql = sql.replace('ais-kafka:29092', BROKER)
        sql = sql.replace("'ais.vessel.features'", "'" + topics[0] + "'").replace("'ais.vessel.gaps'", "'" + topics[1] + "'")
        sql = sql.replace('clickhouse_ais_vessel_features', 'test_features_' + suffix).replace('clickhouse_ais_vessel_gaps', 'test_gaps_' + suffix)
        # Reproduce the prior deployment: persistent destinations and views in raw,
        # feature transport without name. Seed real Kafka history before migration.
        legacy_sql = sql.replace(analytics + '.', database + '.')
        legacy_sql = re.sub(r'^    activity_date .*\n', '', legacy_sql, flags=re.M)
        legacy_sql = re.sub(r'ALTER TABLE [^\n]*ADD COLUMN IF NOT EXISTS activity_date [^;]* FIRST;', '', legacy_sql, flags=re.S)
        legacy_sql = re.sub(r'ALTER TABLE .*?ADD COLUMN IF NOT EXISTS name Nullable\(String\) AFTER mmsi;', '', legacy_sql)
        legacy_sql = re.sub(r'(CREATE TABLE IF NOT EXISTS \w+\.ais_vessel_features(?:_kafka)?\n\()(.*?)(\n\)\nENGINE)',
                            lambda match: match[1] + match[2].replace('    name Nullable(String),\n', '') + match[3], legacy_sql, flags=re.S)
        legacy_sql = legacy_sql.replace('    mmsi,\n    name,\n    window_minutes,', '    mmsi,\n    window_minutes,')
        query(legacy_sql)
        features, gaps = fixtures()
        old_feature = {k: v for k, v in features[0].items() if k != 'name'}
        old_feature['mmsi'] = 999999993
        old_gap = dict(gaps[0], mmsi=999999993)
        for topic, payload in zip(topics, (old_feature, old_gap)):
            kafka('kafka-console-producer.sh', '--topic', topic, payload=json.dumps(payload) + '\n')
        for table in ('ais_vessel_features', 'ais_vessel_gap_events'):
            wait_for(lambda: query('SELECT count() FROM ' + database + '.' + table).strip() == '1', 'legacy history')
        wait_for(lambda: query("SELECT count() FROM system.kafka_consumers WHERE database='" + database + "' AND num_commits>0").strip() == '2', 'legacy offset commits')
        identities = rows("SELECT name,toString(uuid) AS uuid FROM system.tables WHERE database='" + database + "' AND engine='MergeTree' ORDER BY name")
        def migrate():
            command(['docker', 'exec', '-e', 'DERIVED_RAW_DATABASE=' + database,
                     '-e', 'DERIVED_ANALYTICS_DATABASE=' + analytics, CH, 'sh',
                     '/docker-entrypoint-initdb.d/04_flink_derived.sh', '--prepare-only'])
            query(sql)
        migrate()
        # An ambiguous pre-existing destination must fail before any data/view loss.
        query('CREATE TABLE ' + database + '.ais_vessel_features AS ' + analytics + '.ais_vessel_features')
        query('INSERT INTO ' + database + '.ais_vessel_features SELECT * FROM ' + analytics + '.ais_vessel_features')
        try:
            migrate()
        except RuntimeError as error:
            assert 'refusing to merge or drop data' in str(error), error
        else:
            raise AssertionError('Migration accepted conflicting histories')
        assert query('SELECT count() FROM ' + database + '.ais_vessel_features').strip() == '1'
        assert query('SELECT count() FROM ' + analytics + '.ais_vessel_features').strip() == '1'
        query('DROP TABLE ' + database + '.ais_vessel_features SYNC')
        migrate()
        objects = rows("SELECT database,name,engine,create_table_query FROM system.tables WHERE database IN ('" + database + "','" + analytics + "') ORDER BY name")
        assert len(objects) == 6, objects
        assert sorted(row['engine'] for row in objects) == ['Kafka', 'Kafka', 'MaterializedView', 'MaterializedView', 'MergeTree', 'MergeTree']
        for obj in objects:
            assert obj['database'] == (analytics if obj['engine'] == 'MergeTree' else database)
            if obj['engine'] == 'MaterializedView':
                assert 'TO ' + analytics + '.' in obj['create_table_query']
        assert rows("SELECT name,toString(uuid) AS uuid FROM system.tables WHERE database='" + analytics + "' ORDER BY name") == identities
        for table in ('ais_vessel_features', 'ais_vessel_gap_events'):
            assert query('SELECT count() FROM ' + analytics + '.' + table).strip() == '1'
        assert query('SELECT isNull(name) FROM ' + analytics + '.ais_vessel_features').strip() == '1'
        print('PASS: migration preserves UUIDs/history; conflicting histories fail safely; raw destinations retired; second migration copies no rows; MVs target analytics')
        for topic, payloads in zip(topics, (features, gaps)):
            kafka('kafka-console-producer.sh', '--topic', topic, payload=''.join(json.dumps(p) + '\n' for p in payloads))
        for table, expected in (('ais_vessel_features', 6), ('ais_vessel_gap_events', 4)):
            wait_for(lambda: int(query('SELECT count() FROM ' + analytics + '.' + table).strip()) == expected, table + ' delivery')
        feature_rows = rows('SELECT * FROM ' + analytics + '.ais_vessel_features WHERE mmsi<999999993 ORDER BY mmsi,window_minutes')
        for payload, row in zip(features, feature_rows):
            assert_payload(payload, row)
            assert row['ingested_at'] is not None
            assert row['activity_date'] == timestamp(payload['window_end']).date().isoformat()
        assert {row['window_minutes'] for row in feature_rows} == {5, 15, 30, 60}
        for field in ('name', 'avg_speed', 'min_speed', 'max_speed', 'ship_type', 'ship_type_name', 'ship_category', 'last_navigation_status', 'last_navigation_status_name'):
            assert feature_rows[-1][field] is None, field
        gap_rows = rows('SELECT * FROM ' + analytics + '.ais_vessel_gap_events WHERE mmsi<999999993 ORDER BY mmsi,event_type')
        for payload, row in zip(gaps, gap_rows):
            assert_payload(payload, row)
            date_field = 'gap_ended_at' if payload['event_type'] == 'AIS_GAP_ENDED' else 'gap_detected_at'
            assert row['activity_date'] == timestamp(payload[date_field]).date().isoformat()
        detected, ended, null_detected = gap_rows
        assert detected['activity_date'] != ended['activity_date']
        for table in ('ais_vessel_features', 'ais_vessel_gap_events'):
            assert query("SELECT name FROM system.columns WHERE database='" + analytics + "' AND table='" + table + "' ORDER BY position LIMIT 1").strip() == 'activity_date'
            assert rows('SELECT activity_date FROM ' + analytics + '.' + table + ' WHERE mmsi=999999993')[0]['activity_date'] is not None
        print('PASS: activity_date is first; historical/new rows derive UTC dates, including gaps spanning midnight')
        assert all(detected[f] is None for f in ('previous_event_msgtime', 'resumed_event_msgtime', 'gap_ended_at', 'gap_duration_seconds', 'previous_navigation_status', 'resumed_navigation_status'))
        assert all(ended[f] is None for f in ('last_event_msgtime', 'last_latitude', 'last_longitude', 'gap_timeout_seconds', 'navigation_status'))
        assert null_detected['last_event_msgtime'] is None
        assert ended['gap_duration_seconds'] == 660.987  # Not the 60s detection-to-recovery interval.
        nanos = query("SELECT toString(last_event_msgtime) FROM " + analytics + ".ais_vessel_gap_events WHERE mmsi=999999991 AND event_type='AIS_GAP_DETECTED'").strip()
        assert nanos == '2026-10-07 08:59:00.123456789', nanos
        types = rows("SELECT name,type FROM system.columns WHERE database='" + analytics + "' AND table IN ('ais_vessel_features','ais_vessel_gap_events') AND type LIKE '%DateTime%'")
        assert all("'UTC'" in row['type'] for row in types)
        print('PASS: feature 5/15/30/60, Float64 precision, null/missing fields, DETECTED/ENDED, UTC and nanosecond timestamps')
        # Poison only disposable topics; failures must remain visible and uncommitted.
        kafka('kafka-console-producer.sh', '--topic', topics[0], payload='{malformed\n')
        invalid = dict(gaps[0], last_event_msgtime='not-a-timestamp')
        kafka('kafka-console-producer.sh', '--topic', topics[1], payload=json.dumps(invalid) + '\n')
        wait_for(lambda: int(query("SELECT count() FROM system.kafka_consumers WHERE database='" + database + "' AND length(exceptions.text)>0").strip()) == 2, 'visible parsing failures')
        assert query('SELECT count() FROM ' + analytics + '.ais_vessel_features').strip() == '6'
        assert query('SELECT count() FROM ' + analytics + '.ais_vessel_gap_events').strip() == '4'
        print('PASS: malformed JSON and invalid non-null timestamps are visible; bad rows are not persisted')
    finally:
        query('DROP DATABASE IF EXISTS ' + database + ' SYNC')
        query('DROP DATABASE IF EXISTS ' + analytics + ' SYNC')
        for topic in created_topics:
            kafka('kafka-topics.sh', '--delete', '--topic', topic)
        print('Cleaned disposable database and topics; live AIS topics untouched')


if __name__ == '__main__':
    main()
