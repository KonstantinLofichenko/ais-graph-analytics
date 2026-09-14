import json
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from client import HistoricAISClient
from run import batches, load_settings, normalize_record, parse_timestamp


class HistoricAISLogicTests(unittest.TestCase):
    def test_parse_timestamp(self):
        self.assertEqual(parse_timestamp('2026-09-01T00:00:00Z').tzinfo, timezone.utc)
        with self.assertRaises(ValueError):
            parse_timestamp('2026-09-01T00:00:00')

    def test_normalize_record(self):
        row = normalize_record(259989000, {'msgtime': '2026-09-01T00:00:00Z', 'latitude': 60.4,
                                           'longitude': 5.3, 'speedOverGround': 2.0, 'name': 'TEST'})
        self.assertEqual(row['speed_over_ground'], 2.0)
        self.assertNotIn('ingested_at', row)

    def test_batches(self):
        self.assertEqual(list(batches([{'n': n} for n in range(5)], 2)),
                         [[{'n': 0}, {'n': 1}], [{'n': 2}, {'n': 3}], [{'n': 4}]])

    def test_mmsis_in_area_uses_official_payload_fields(self):
        session = Mock()
        token_response = Mock()
        token_response.json.return_value = {'access_token': 'test-token', 'expires_in': 300}
        area_response = Mock()
        area_response.json.return_value = [259989000]
        session.post.return_value = token_response
        session.request.return_value = area_response
        client = HistoricAISClient(
            'https://token', 'https://historic', 'client-id', 'client-secret', 'ais', 60,
            session=session,
        )
        start = datetime(2026, 9, 1, tzinfo=timezone.utc)
        end = datetime(2026, 9, 2, tzinfo=timezone.utc)

        self.assertEqual(client.mmsis_in_area({'type': 'Polygon'}, start, end), [259989000])
        request_kwargs = session.request.call_args.kwargs
        self.assertEqual(request_kwargs['json'], {
            'msgtimefrom': start.isoformat(),
            'msgtimeto': end.isoformat(),
            'polygon': {'type': 'Polygon'},
        })

    def test_api_requests_reuse_cached_token(self):
        session = Mock()
        token_response = Mock()
        token_response.json.return_value = {'access_token': 'test-token', 'expires_in': 300}
        area_response = Mock()
        area_response.json.return_value = [259989000]
        track_response = Mock()
        track_response.json.return_value = []
        session.post.return_value = token_response
        session.request.side_effect = [area_response, track_response]
        client = HistoricAISClient(
            'https://token', 'https://historic', 'client-id', 'client-secret', 'ais', 60,
            session=session,
        )

        client.mmsis_in_area({'type': 'Polygon'}, datetime(2026, 9, 1, tzinfo=timezone.utc),
                             datetime(2026, 9, 2, tzinfo=timezone.utc))
        client.tracks(259989000, datetime(2026, 9, 1, tzinfo=timezone.utc),
                      datetime(2026, 9, 2, tzinfo=timezone.utc))

        self.assertEqual(session.post.call_count, 1)

    def test_expired_token_is_refreshed(self):
        session = Mock()
        first_token = Mock()
        first_token.json.return_value = {'access_token': 'first-token', 'expires_in': 300}
        second_token = Mock()
        second_token.json.return_value = {'access_token': 'second-token', 'expires_in': 300}
        session.post.side_effect = [first_token, second_token]
        client = HistoricAISClient(
            'https://token', 'https://historic', 'client-id', 'client-secret', 'ais', 60,
            session=session,
        )

        with patch('client.time.monotonic', side_effect=[100.0, 100.0, 400.0, 400.0]):
            self.assertEqual(client._token(), 'first-token')
            self.assertEqual(client._token(), 'second-token')

        self.assertEqual(session.post.call_count, 2)

    def test_malformed_token_response_raises_error(self):
        session = Mock()
        token_response = Mock()
        token_response.json.return_value = {'expires_in': 300}
        session.post.return_value = token_response
        client = HistoricAISClient(
            'https://token', 'https://historic', 'client-id', 'client-secret', 'ais', 60,
            session=session,
        )

        with self.assertRaisesRegex(RuntimeError, 'did not contain a token'):
            client._token()

    def test_load_settings_polygon(self):
        values = {'BW_AIS_CLIENT_ID': 'id', 'BW_AIS_CLIENT_SECRET': 'secret', 'BW_TOKEN_URL': 'https://token',
                  'BW_HISTORIC_BASE_URL': 'https://historic', 'BW_HISTORIC_SCOPE': 'ais',
                  'BW_HISTORIC_WINDOW_HOURS': '24', 'BW_HISTORIC_BATCH_SIZE': '5000',
                  'BW_HISTORIC_REQUEST_TIMEOUT_SECONDS': '60',
                  'BW_HISTORIC_POLYGON_JSON': json.dumps({'type': 'Polygon', 'coordinates': []}),
                  'CLICKHOUSE_URL': 'http://clickhouse', 'CLICKHOUSE_USER': 'default', 'CLICKHOUSE_PASSWORD': 'password'}
        old = os.environ.copy()
        try:
            os.environ.update(values)
            self.assertEqual(load_settings()['polygon']['type'], 'Polygon')
        finally:
            os.environ.clear(); os.environ.update(old)