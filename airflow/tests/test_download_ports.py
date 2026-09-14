from pathlib import Path
import sys
import unittest
from unittest.mock import patch, MagicMock
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'pipelines/port_visits'))
from download_ports import download


class DownloadTests(unittest.TestCase):
    def fetch(self, body):
        response = MagicMock()
        response.json.return_value = body
        with patch('download_ports.requests.get', return_value=response):
            return download()

    def test_normalization(self):
        ports = self.fetch({'features':[{'attributes':dict(index_no=23160,port_name='BERGEN',country='NO',latitude=60.4,longitude=5.316667)}]})
        self.assertEqual(ports[0]['port_id'],'WPI:23160')
        self.assertEqual(ports[0]['radius_m'],1500)

    def test_refuses_partial_error_empty_or_duplicate(self):
        for body in ({'error':{'code':500}}, {'exceededTransferLimit':True}, {'features':[]}):
            with self.assertRaises((RuntimeError,ValueError)):
                self.fetch(body)
        f={'attributes':dict(index_no=1,port_name='TEST',country='NO',latitude=60,longitude=5)}
        with self.assertRaises(ValueError):
            self.fetch({'features':[f,f]})
