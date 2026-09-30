"""Execute the production presentation SQL over deterministic historical fixtures."""
from datetime import date
from pathlib import Path
import re
import sqlite3
import sys
import unittest
from unittest.mock import Mock, patch
from jinja2 import Environment

from pipelines.ai_enrichment import enrich_vessels as enrichment


class PresentationTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.create_function('toNullable', 1, lambda x: x)
        self.db.create_function('toUInt8', 1, lambda x: int(x or 0))
        self.db.execute("ATTACH DATABASE ':memory:' AS analytics")
        template = Path('dbt/models/marts/vessel_daily_enriched.sql').read_text()
        fields = list(dict.fromkeys(re.findall(r'f\.(\w+)', template)))
        self.db.execute('CREATE TABLE analytics.vessel_daily_features (' + ','.join(fields) + ')')
        self.db.execute('INSERT INTO analytics.vessel_daily_features (' + ','.join(fields) + ') VALUES (' + ','.join('?' for _ in fields) + ')',
                        [257000000 if k == 'mmsi' else '2026-09-27' if k == 'activity_date' else 0 for k in fields])
        self.db.execute('CREATE TABLE analytics.int_vessel_ai_current_inputs (mmsi, activity_date, anomaly_rank, current_ai_input_hash)')
        self.db.execute("INSERT INTO analytics.int_vessel_ai_current_inputs VALUES (257000000,'2026-09-27',1,'B')")
        self.db.execute('''CREATE TABLE analytics.vessel_ai_enrichment (
            mmsi,activity_date,activity_class,navigation_status_quality,summary,notable_behavior,
            data_quality_note,model,prompt_version,input_hash,openai_response_id,input_tokens,output_tokens,created_at)''')
        self.sql = Environment().from_string(template).render(config=lambda **k: '',
            ref=lambda name: 'analytics.'+name, source=lambda schema,name: schema+'.'+name,
            var=lambda name: {'ai_model':'test-model','ai_prompt_version':'vessel_anomaly_v1'}[name])

    def add(self, hash='B', model='test-model', prompt='vessel_anomaly_v1', created=1, note=None):
        self.db.execute('INSERT INTO analytics.vessel_ai_enrichment VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (257000000,'2026-09-27','mixed_activity','low','response '+hash,'behavior',note,
             model,prompt,hash,'resp_'+hash,10,5,created))

    def current(self):
        return dict(self.db.execute(self.sql).fetchone())

    def assert_hidden(self):
        row = self.current()
        self.assertEqual(row['has_ai_enrichment'], 0)
        for key in ('activity_class','navigation_status_quality','ai_summary','ai_notable_behavior',
                    'ai_data_quality_note','ai_model','ai_prompt_version','ai_input_hash',
                    'openai_response_id','ai_input_tokens','ai_output_tokens','ai_created_at'):
            self.assertIsNone(row[key], key)

    def test_exact_match_is_shown(self):
        self.add()
        self.assertEqual(self.current()['has_ai_enrichment'], 1)
        self.assertEqual(self.current()['ai_input_hash'], 'B')

    def test_stale_is_hidden_and_history_retained(self):
        self.add('A')
        self.assert_hidden()
        self.assertEqual(self.db.execute('SELECT count(*) FROM analytics.vessel_ai_enrichment').fetchone()[0],1)

    def test_current_hash_wins_over_newest_other_hash(self):
        for hash,created in [('A',1),('B',2),('C',3)]: self.add(hash,created=created)
        self.assertEqual(self.current()['ai_summary'],'response B')

    def test_wrong_model_and_prompt_hidden(self):
        self.add(model='other')
        self.add(prompt='old')
        self.assert_hidden()

    def test_no_response_hidden(self):
        self.assert_hidden()

    def test_former_top100_hidden(self):
        self.add()
        self.db.execute('UPDATE analytics.int_vessel_ai_current_inputs SET anomaly_rank=101')
        self.assert_hidden()

    def test_top100_boundary_shown(self):
        self.add()
        self.db.execute('UPDATE analytics.int_vessel_ai_current_inputs SET anomaly_rank=100')
        self.assertEqual(self.current()['has_ai_enrichment'],1)

    def test_current_input_lookup_requires_exact_values(self):
        self.db.execute('CREATE TABLE analytics.int_vessel_ai_candidates (mmsi,activity_date,anomaly_rank,input_values)')
        self.db.execute('CREATE TABLE analytics.vessel_ai_input_hashes (input_values,input_hash)')
        self.db.execute("INSERT INTO analytics.int_vessel_ai_candidates VALUES (257000000,'2026-09-27',1,'values B')")
        self.db.execute("INSERT INTO analytics.vessel_ai_input_hashes VALUES ('values A','A')")
        sql = Environment().from_string(Path('dbt/models/intermediate/int_vessel_ai_current_inputs.sql').read_text()).render(
            config=lambda **k: '', ref=lambda name: 'analytics.'+name,
            source=lambda schema,name: schema+'.'+name).replace('AS h FINAL','AS h')
        self.assertEqual(self.db.execute(sql).fetchall(), [])
        self.db.execute("INSERT INTO analytics.vessel_ai_input_hashes VALUES ('values B','B')")
        self.assertEqual(self.db.execute(sql).fetchone()['current_ai_input_hash'], 'B')

    def test_missing_current_mapping_fails_closed(self):
        self.add()
        self.db.execute('DELETE FROM analytics.int_vessel_ai_current_inputs')
        self.assert_hidden()

    def test_duplicate_exact_key_selects_one_coherent_response_with_null_note(self):
        self.add(created=1,note='old note')
        self.add(created=2,note=None)
        row=self.current()
        self.assertEqual(row['ai_created_at'],2)
        self.assertIsNone(row['ai_data_quality_note'])
        self.assertEqual(len(self.db.execute(self.sql).fetchall()),1)


class CanonicalHashTests(unittest.TestCase):
    def test_lookup_uses_existing_python_hash_without_payload_field(self):
        vessel=dict(mmsi=257000000,activity_date=date(2026,9,27),avg_speed_kn=8.0,anomaly_rank=1)
        ch=Mock()
        ch.query.return_value.column_names=[*vessel,'input_values']
        ch.query.return_value.result_rows=[(*vessel.values(),'typed bytes')]
        self.assertEqual(enrichment.refresh_input_hashes(ch),1)
        self.assertEqual(ch.insert.call_args.args[1],[['typed bytes',enrichment.calculate_input_hash(vessel)]])
        self.assertEqual(ch.insert.call_args.args[0],'analytics.vessel_ai_input_hashes')

    def test_refresh_only_never_constructs_openai_or_writes_responses(self):
        with patch.object(sys,'argv',['enrich','--refresh-input-hashes-only']), \
                patch.object(enrichment,'get_clickhouse_client') as client, \
                patch.object(enrichment,'refresh_input_hashes',return_value=4), \
                patch.object(enrichment,'OpenAI') as api, \
                patch.object(enrichment,'insert_enrichment') as write:
            enrichment.main()
        api.assert_not_called()
        write.assert_not_called()
        client.return_value.close.assert_called_once()


if __name__ == '__main__': unittest.main()
