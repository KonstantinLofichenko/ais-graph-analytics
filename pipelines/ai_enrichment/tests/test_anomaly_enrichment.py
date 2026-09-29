"""Offline regression tests; OpenAI and ClickHouse writes are mocked."""
from contextlib import ExitStack, redirect_stdout
from datetime import date, datetime, timezone
from io import StringIO
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from pipelines.ai_enrichment import enrich_vessels as enrichment


DAY = date(2026, 9, 28)


def vessel(rank):
    return dict(
        mmsi=257000000 + rank, activity_date=DAY, vessel_name=f'Vessel {rank}',
        ship_type_name='Cargo', ship_category='Cargo', ais_points=1000,
        observation_hours=23.5, avg_speed_kn=8.0, max_speed_kn=10.0,
        stationary_observation_pct=5.0, dominant_navigational_status_name='Moored',
        dominant_status_pct=90.0, nav_status_change_rate_pct=3.0, baseline_days=6,
        baseline_avg_speed_kn=1.0, baseline_sd_speed_kn=0.5, speed_deviation_kn=7.0,
        speed_anomaly_threshold_kn=3.0, speed_anomaly=1, baseline_stationary_pct=95.0,
        baseline_sd_stationary_pct=2.0, stationary_deviation_pct=90.0,
        stationary_anomaly_threshold_pct=30.0, stationary_anomaly=1,
        status_speed_mismatch=1, navigation_status_inconsistent=0,
        anomaly_reason='speed_and_stationary', anomaly_score=5.583, anomaly_rank=rank,
    )


class AnomalyEnrichmentTests(unittest.TestCase):
    def setUp(self):
        # Execute the production SELECT over >100 rows, rather than returning a
        # prefiltered mock. SQLite supports this query with one cast shim and a
        # parameter-placeholder translation; real ClickHouse is checked separately.
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.create_function('toFloat64', 1, lambda x: float(x) if x is not None else None)
        self.db.execute("ATTACH DATABASE ':memory:' AS analytics")
        self.columns = list(vessel(1))
        self.db.execute('CREATE TABLE analytics.vessel_daily_anomalies (' + ','.join(self.columns) + ')')
        rows = [vessel(rank) for rank in range(150, 0, -1)]
        rows.append(dict(vessel(1), activity_date=date(2026, 9, 27)))
        self.db.executemany(
            'INSERT INTO analytics.vessel_daily_anomalies VALUES (' + ','.join('?' for _ in self.columns) + ')',
            [[v.isoformat() if isinstance(v, date) else v for v in row.values()] for row in rows],
        )
        self.client = Mock()
        self.client.query.side_effect = self.query

    def query(self, query, parameters):
        self.assertIn('FROM analytics.vessel_daily_anomalies', query)
        cursor = self.db.execute(query.replace('{activity_date:Date}', ':activity_date'),
                                 {'activity_date': parameters['activity_date'].isoformat()})
        names = [col[0] for col in cursor.description]
        return SimpleNamespace(column_names=names, result_rows=cursor.fetchall())

    def candidates(self):
        return enrichment.get_vessels(self.client, DAY)

    def run_job(self, limit=5, existing=None, effects=None, explicit_date=True):
        existing = existing if existing is not None else set()
        events = []
        def call_ai(**kwargs):
            row = kwargs['vessel']
            events.append(('call', row['anomaly_rank']))
            if effects:
                effect = effects.get(row['anomaly_rank'])
                if effect:
                    raise effect
            return Mock(activity_class='mixed_activity'), Mock()
        def insert(**kwargs):
            row = kwargs['vessel']
            events.append(('insert', row['anomaly_rank']))
            existing.add((row['mmsi'], kwargs['input_hash']))
        argv = ['enrich_vessels', '--limit', str(limit)]
        if explicit_date:
            argv += ['--date', DAY.isoformat()]
        output = StringIO()
        with ExitStack() as stack:
            stack.enter_context(patch.object(sys, 'argv', argv))
            stack.enter_context(patch.object(enrichment, 'get_clickhouse_client', return_value=self.client))
            stack.enter_context(patch.object(enrichment, 'OpenAI'))
            stack.enter_context(patch.object(enrichment, 'get_existing_hashes', return_value=existing))
            stack.enter_context(patch.object(enrichment, 'enrich_vessel', side_effect=call_ai))
            stack.enter_context(patch.object(enrichment, 'insert_enrichment', side_effect=insert))
            stack.enter_context(redirect_stdout(output))
            try:
                enrichment.main()
                code = 0
            except SystemExit as exc:
                code = exc.code
        return code, events, output.getvalue()

    def test_query_selects_only_top_100_for_date_in_rank_order(self):
        rows = self.candidates()
        self.assertEqual([r['anomaly_rank'] for r in rows], list(range(1, 101)))
        self.assertEqual({r['activity_date'] for r in rows}, {DAY.isoformat()})
        self.assertEqual(set(rows[0]), set(vessel(1)))
        query = self.client.query.call_args.args[0]
        for field in self.columns:
            self.assertIn('AS ' + field, query)

    def test_skipping_first_five_still_makes_only_five_calls_and_inserts_immediately(self):
        existing = {(r['mmsi'], enrichment.calculate_input_hash(r)) for r in self.candidates()[:5]}
        code, events, output = self.run_job(existing=existing)
        self.assertEqual(code, 0)
        self.assertEqual(events, [(op, rank) for rank in range(6, 11) for op in ('call', 'insert')])
        self.assertIn('OpenAI calls attempted: 5', output)

    def test_rerun_top_100_never_backfills_from_rank_101(self):
        existing = set()
        first = self.run_job(limit=250, existing=existing)
        second = self.run_job(limit=250, existing=existing)
        self.assertEqual(first[0], 0)
        self.assertEqual([rank for op, rank in first[1] if op == 'call'], list(range(1, 101)))
        self.assertEqual(second[0], 0)
        self.assertEqual(second[1], [])
        self.assertIn('Already enriched: 100', second[2])

    def test_failure_consumes_limit_and_failed_row_can_be_retried(self):
        existing = set()
        first = self.run_job(existing=existing, effects={2: RuntimeError('test failure')})
        self.assertEqual(first[0], 1)
        self.assertEqual([r for op, r in first[1] if op == 'call'], [1, 2, 3, 4, 5])
        self.assertEqual([r for op, r in first[1] if op == 'insert'], [1, 3, 4, 5])
        second = self.run_job(limit=1, existing=existing)
        self.assertEqual(second[1], [('call', 2), ('insert', 2)])

    def test_keyboard_interrupt_keeps_prior_insert_and_stops(self):
        code, events, _ = self.run_job(effects={2: KeyboardInterrupt()})
        self.assertEqual(code, 130)
        self.assertEqual(events, [('call', 1), ('insert', 1), ('call', 2)])

    def test_default_date_is_previous_completed_utc_day(self):
        with patch.object(enrichment, 'datetime') as clock:
            clock.now.return_value = datetime(2026, 9, 29, 0, 1, tzinfo=timezone.utc)
            code, _, output = self.run_job(limit=1, explicit_date=False)
        clock.now.assert_called_once_with(timezone.utc)
        self.assertEqual(code, 0)
        self.assertIn('AI enrichment date: 2026-09-28', output)
        self.assertEqual(self.client.query.call_args.kwargs['parameters']['activity_date'], DAY)

    def test_prompt_payload_and_hash_include_anomaly_context(self):
        row = vessel(1)
        original = enrichment.calculate_input_hash(row)
        self.assertEqual(original, enrichment.calculate_input_hash(dict(reversed(list(row.items())))))
        prompt = enrichment.build_prompt(row)
        payload = json.loads(prompt.split('Vessel features and anomaly context:\n\n')[1])
        self.assertEqual(enrichment.calculate_input_hash(payload), original)
        for key in ('baseline_avg_speed_kn', 'stationary_deviation_pct', 'anomaly_score', 'anomaly_rank'):
            with self.subTest(field=key):
                changed = dict(row, **{key: row[key] + 1})
                self.assertNotEqual(enrichment.calculate_input_hash(changed), original)
                self.assertNotEqual(enrichment.build_prompt(changed), prompt)
        self.assertIn('not a probability', prompt)
        self.assertIn('Do not claim continuous movement', prompt)

    def test_explicit_date_does_not_follow_execution_date(self):
        with patch.object(enrichment, 'datetime') as clock:
            clock.now.return_value = datetime(2026, 10, 1, tzinfo=timezone.utc)
            code, _, _ = self.run_job(limit=1)
        self.assertEqual(code, 0)
        clock.now.assert_not_called()
        self.assertEqual(self.client.query.call_args.kwargs['parameters']['activity_date'], DAY)

    def test_skip_query_scopes_date_model_and_prompt_version(self):
        client = Mock()
        client.query.return_value.result_rows = [(123, b'hash')]
        self.assertEqual(enrichment.get_existing_hashes(client, DAY), {(123, 'hash')})
        self.assertEqual(client.query.call_args.kwargs['parameters'], dict(
            activity_date=DAY, model=enrichment.MODEL, prompt_version='vessel_anomaly_v1'))
        for field in ('activity_date', 'model', 'prompt_version'):
            self.assertIn(field + ' = {' + field, client.query.call_args.args[0])
        project = Path('dbt/dbt_project.yml').read_text()
        self.assertIn("ai_prompt_version: 'vessel_anomaly_v1'", project)

    def test_insert_retains_schema_provenance_and_usage(self):
        result = enrichment.VesselAIEnrichment(activity_class='mixed_activity',
            navigation_status_quality='low', summary='Baseline deviation',
            notable_behavior='Observed speed differs', data_quality_note=None)
        response = SimpleNamespace(id='resp_test', usage=SimpleNamespace(input_tokens=123, output_tokens=45))
        client = Mock()
        enrichment.insert_enrichment(client, vessel(1), 'hash', result, response)
        self.assertEqual(set(enrichment.VesselAIEnrichment.model_fields), {'activity_class', 'navigation_status_quality',
                         'summary', 'notable_behavior', 'data_quality_note'})
        call = client.insert.call_args
        stored = dict(zip(call.kwargs['column_names'], call.args[1][0]))
        self.assertEqual(call.args[0], 'analytics.vessel_ai_enrichment')
        for field, expected in dict(model=enrichment.MODEL, prompt_version='vessel_anomaly_v1',
                                    input_hash='hash', openai_response_id='resp_test',
                                    input_tokens=123, output_tokens=45).items():
            self.assertEqual(stored[field], expected)


if __name__ == '__main__':
    unittest.main()
