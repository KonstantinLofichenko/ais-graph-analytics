"""Manual DAG wiring and fail-closed validator/summary behavior."""
from pathlib import Path
import json
import unittest
from unittest.mock import Mock, patch

from airflow.dag_processing.dagbag import DagBag
from airflow.exceptions import AirflowFailException


class LakehouseDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag()
        cls.bag.process_file(str(Path(__file__).resolve().parents[1] /
                                 'dags/ais_hais_lakehouse_load.py'), safe_mode=False)
        cls.dag = cls.bag.dags['ais_hais_lakehouse_load']

    def test_manual_serial_pipeline(self):
        self.assertFalse(self.bag.import_errors)
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        ids = ['check_source_files', 'ingest_raw_iceberg', 'validate_raw',
               'curate_hais', 'validate_curated', 'final_summary']
        self.assertEqual(set(self.dag.task_ids), set(ids))
        for index, task_id in enumerate(ids):
            task = self.dag.get_task(task_id)
            self.assertEqual(task.upstream_task_ids, set(ids[index-1:index]) if index else set())
            self.assertEqual(task.trigger_rule, 'all_success')
        for task_id in ['validate_raw', 'validate_curated', 'final_summary']:
            self.assertEqual(self.dag.get_task(task_id).retries, 0)

    def test_spark_container_has_no_upload_or_host_mount(self):
        for task_id in ['check_source_files', 'ingest_raw_iceberg', 'curate_hais']:
            task = self.dag.get_task(task_id)
            self.assertFalse(task.mount_tmp_dir)
            self.assertFalse(task.mounts)
            self.assertEqual(task.auto_remove, 'force')
            self.assertNotIn('upload_hais', str(task.command))
            self.assertIn('/opt/spark/bin/spark-submit', str(task.command))
            self.assertIn('tail -n 40', str(task.command))
            self.assertFalse(task.xcom_all)

    def test_validation_error_is_not_swallowed(self):
        task = self.dag.get_task('validate_raw')
        ti = Mock()
        ti.xcom_pull.return_value = 'RAW_INGEST_RESULT=' + json.dumps({'source_count': 42})
        with patch('lakehouse.validate_raw.validate', side_effect=ValueError('bad raw data')):
            with self.assertRaisesRegex(ValueError, 'bad raw data'):
                task.python_callable(ti)
        with patch('lakehouse.validate_curated.validate', side_effect=AssertionError('bad NULL counts')):
            with self.assertRaisesRegex(AssertionError, 'bad NULL counts'):
                self.dag.get_task('validate_curated').python_callable()

    def test_missing_success_marker_fails(self):
        ti = Mock(); ti.xcom_pull.return_value = 'Spark failed'
        with self.assertRaises(AirflowFailException):
            self.dag.get_task('validate_raw').python_callable(ti)

    def test_summary_is_small_and_uses_validated_counts(self):
        ti = Mock()
        values = {'check_source_files': 'SOURCE_RESULT={"files":30}',
                  'validate_raw': {'trino_count': 23295797},
                  'validate_curated': {'counts': {'curated': 23295797}}}
        ti.xcom_pull.side_effect = lambda task_ids: values[task_ids]
        self.assertEqual(self.dag.get_task('final_summary').python_callable(ti),
                         {'source_files': 30, 'raw_rows': 23295797,
                          'curated_rows': 23295797, 'validation': 'passed'})


class RawValidationTests(unittest.TestCase):
    def test_empty_raw_fails(self):
        from lakehouse.validate_raw import validate
        with patch('lakehouse.validate_raw.query', return_value=[[0]]):
            with self.assertRaisesRegex(ValueError, 'empty'):
                validate({'source_count': 0, 'iceberg_count': 0})

    def test_count_mismatch_fails(self):
        from lakehouse.validate_raw import validate
        with patch('lakehouse.validate_raw.query', return_value=[[42]]):
            with self.assertRaisesRegex(RuntimeError, 'differs'):
                validate({'source_count': 43, 'iceberg_count': 43})

    def test_missing_raw_column_fails(self):
        from lakehouse.validate_raw import validate
        columns = ['date_time_utc', 'mmsi', 'longitude', 'latitude', 'status',
                   'course_over_ground', 'true_heading', 'speed_over_ground',
                   'rate_of_turn', 'maneuvre', 'data_source', 'ais_class', 'msg_type']
        with patch('lakehouse.validate_raw.query', side_effect=[
            [[42]], [['s3://warehouse/raw/data.parquet']], [[x] for x in columns]
        ]):
            with self.assertRaisesRegex(ValueError, 'geometry'):
                validate({'source_count': 42, 'iceberg_count': 42})


if __name__ == '__main__':
    unittest.main()
