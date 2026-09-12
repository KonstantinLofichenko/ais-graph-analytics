import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch
import signal

spec = importlib.util.spec_from_file_location('ais_producer', Path(__file__).parents[1] / 'producer.py')
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


class ProducerTests(unittest.TestCase):
    def test_limits(self):
        for value in (None, '', ' '):
            self.assertIsNone(p.message_limit(value))
        self.assertEqual(p.message_limit('20'), 20)
        for value in ('0', '-1', 'bad'):
            with self.assertRaises(RuntimeError):
                p.message_limit(value)

    def test_token_uses_arguments_and_expiry(self):
        session = MagicMock()
        response = session.post.return_value.__enter__.return_value
        response.status_code = 200
        response.json.return_value = {'access_token': 'test-token', 'expires_in': 100}
        config = {'BW_TOKEN_URL': 'https://example.test/token', 'BW_AIS_CLIENT_ID': 'id', 'BW_AIS_CLIENT_SECRET': 'secret'}
        with patch.object(p.time, 'monotonic', return_value=10):
            self.assertEqual(p.request_access_token(session, config), ('test-token', 100))
        self.assertEqual(session.post.call_args.kwargs['data']['client_id'], 'id')
        response.json.return_value = {'access_token': 'test-token', 'expires_in': 'nan'}
        with self.assertRaises(RuntimeError):
            p.request_access_token(session, config)

    def test_http_classification(self):
        response = MagicMock(status_code=429, headers={'Retry-After': '12'})
        with self.assertRaises(p.Retryable) as caught:
            p.check_response(response, 'test')
        self.assertEqual(caught.exception.delay, 12)
        response.status_code = 403
        with self.assertRaises(RuntimeError):
            p.check_response(response, 'test')

    def lifecycle(self, limit='20', lines=None, statuses=(200,), token_deadline=10**20):
        env = {name: 'test' for name in ('BW_TOKEN_URL','BW_AIS_URL','BW_AIS_CLIENT_ID','BW_AIS_CLIENT_SECRET','KAFKA_BOOTSTRAP_SERVERS','KAFKA_TOPIC')}
        env['AIS_MESSAGE_LIMIT'] = limit
        producer = MagicMock()
        producer.flush.return_value = 0
        producer.produce.side_effect = lambda *a, **kw: kw['on_delivery'](None, None)
        session = MagicMock()
        responses = []
        for status in statuses:
            response = MagicMock(status_code=status)
            response.iter_lines.return_value = lines if lines is not None else [b'bad', b'[]', b'{}'] + [json.dumps({'mmsi': n}).encode() for n in range(1, 30)]
            context = MagicMock()
            context.__enter__.return_value = response
            responses.append(context)
        session.get.side_effect = responses
        with patch.dict(p.os.environ, env), patch.object(p, 'load_dotenv'), patch.object(p, 'Producer', return_value=producer), patch.object(p.requests, 'Session') as sessions, patch.object(p, 'request_access_token', return_value=('token', token_deadline)) as token, patch.object(p.time, 'sleep'), patch.object(p.signal, 'setitimer'):
            sessions.return_value.__enter__.return_value = session
            result = p.run()
        return result, producer, token

    def test_exactly_twenty_and_flush(self):
        result, producer, _ = self.lifecycle()
        self.assertEqual(result, 20)
        self.assertEqual(producer.produce.call_count, 20)
        producer.flush.assert_called_once_with(35)

    def test_401_refresh(self):
        result, _, token = self.lifecycle(statuses=(401, 200))
        self.assertEqual(result, 20)
        self.assertEqual(token.call_count, 2)

    def test_eof_reconnect(self):
        result, _, _ = self.lifecycle(lines=[b'{"mmsi":1}'], limit='2', statuses=(200, 200))
        self.assertEqual(result, 2)

    def test_continuous_signals(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            def lines():
                for _ in range(25):
                    yield b'{"mmsi":1}'
                signal.raise_signal(sig)
            result, producer, _ = self.lifecycle(limit='', lines=lines())
            self.assertEqual(result, 25)
            producer.flush.assert_called_once_with(35)

    def test_refresh_interrupts_stream(self):
        calls = 0
        def lines():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise p.RefreshToken()
            yield b'{"mmsi":1}'
        class Stream:
            def __iter__(self):
                return lines()
        result, _, token = self.lifecycle(limit='1', lines=Stream(), statuses=(200, 200))
        self.assertEqual(result, 1)
        self.assertEqual(token.call_count, 2)

    def test_repeated_401_fails(self):
        with self.assertRaisesRegex(RuntimeError, 'fresh OAuth token'):
            self.lifecycle(statuses=(401, 401))

    def test_delivery_failure(self):
        producer = MagicMock()
        producer.produce.side_effect = lambda *a, **kw: kw['on_delivery']('failed', None)
        with self.assertRaises(RuntimeError):
            p.publish_event(producer, 'topic', {'mmsi': 1})


if __name__ == '__main__':
    unittest.main()
