"""Offline tests for dashboard export and import, including API call order."""
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import export_metabase as exporter
import import_metabase as importer


def example_objects():
    dashboard = {
        "id": 9, "name": "Port network", "creator": {"email": "private@example.com"},
        "parameters": [{"id": "country", "name": "Country"}],
        "tabs": [{"id": 8, "name": "Overview", "position": 0}],
        "dashcards": [{"id": 77, "card_id": 1, "row": 0, "col": 2, "size_x": 8,
                       "size_y": 4, "dashboard_tab_id": 8,
                       "parameter_mappings": [{"parameter_id": "country", "card_id": 1,
                                               "target": ["dimension", ["field", 10, None]]}],
                       "series": [{"id": 4, "card_id": 4}],
                       "visualization_settings": {"graph.show_values": True}},
                      {"id": 78, "card_id": None, "row": 4, "col": 0, "size_x": 6,
                       "size_y": 1, "card": {"display": "text", "name": "Intro"}}],
    }
    cards = {
        1: {"id": 1, "name": "Main", "display": "line", "type": "question",
            "creator": {"email": "private@example.com"},
            "dataset_query": {"query": {"source-table": "card__2"}}},
        2: {"id": 2, "name": "SQL", "display": "table", "type": "question",
            "dataset_query": {"native": {"query": "SELECT * FROM {{#3-base}}"}}},
        3: {"id": 3, "name": "Base", "display": "table", "type": "model",
            "dataset_query": {"query": {"source-table": 42}}},
        4: {"id": 4, "name": "Series", "display": "line", "type": "question",
            "dataset_query": {"query": {"source-table": 42}}},
    }
    return dashboard, cards


class FakeExportAPI:
    def __init__(self, dashboard, cards):
        self.dashboard, self.cards = dashboard, cards
        self.calls = []

    def get(self, path):
        self.calls.append(path)
        if path == "/api/dashboard/9":
            return self.dashboard
        return self.cards[int(path.rsplit("/", 1)[1])]


class FakeImportAPI:
    def __init__(self):
        self.calls = []

    def post(self, path, payload):
        self.calls.append(("POST", path, payload))
        return {"id": 100 + len(self.calls)}

    def put(self, path, payload):
        self.calls.append(("PUT", path, payload))
        return {}


class MetabaseTransferTests(unittest.TestCase):
    def test_tracked_norway_snapshot_validates_offline(self):
        root = Path(__file__).resolve().parents[1] / "exports" / "norway-port-graph-analytics"
        with patch.object(sys, "argv", ["import_metabase.py", str(root), "--dry-run"]), \
             patch.object(importer, "Metabase") as api, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(importer.main(), 0)
            api.assert_not_called()

    def test_export_recursively_fetches_query_and_sql_dependencies(self):
        dashboard, cards = example_objects()
        api = FakeExportAPI(dashboard, cards)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "export"
            with patch.object(exporter, "Metabase", return_value=api), \
                 patch.object(sys, "argv", ["export_metabase.py", "9", "--output", str(output)]), \
                 patch.dict(os.environ, {"METABASE_URL": "http://example", "METABASE_API_KEY": "test"}), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(exporter.main(), 0)
            self.assertEqual(set(api.calls), {"/api/dashboard/9", "/api/card/1", "/api/card/2",
                                              "/api/card/3", "/api/card/4"})
            self.assertEqual(json.loads((output / "manifest.json").read_text())["all_card_ids"],
                             [1, 2, 3, 4])
            self.assertEqual(json.loads((output / "manifest.json").read_text())["card_dependencies"],
                             {"1": [2], "2": [3], "3": [], "4": []})
            self.assertEqual(len(list((output / "cards").glob("*.json"))), 4)
            self.assertTrue((output / "cards" / "1-main.json").exists())
            self.assertNotIn("creator", json.loads((output / "cards" / "1-main.json").read_text()))
            self.assertNotIn("creator", json.loads((output / "dashboard.json").read_text()))

    def test_import_orders_cards_and_restores_layout_and_filters(self):
        dashboard, cards = example_objects()
        api = FakeImportAPI()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "cards").mkdir()
            (root / "dashboard.json").write_text(json.dumps(dashboard))
            for cid, card in cards.items():
                (root / "cards" / f"{cid}.json").write_text(json.dumps(card))
            with patch.object(importer, "Metabase", return_value=api), \
                 patch.object(sys, "argv", ["import_metabase.py", str(root), "--collection-id", "7"]), \
                 patch.dict(os.environ, {"METABASE_URL": "http://example", "METABASE_API_KEY": "test"}), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(importer.main(), 0)
            posts = [call for call in api.calls if call[0] == "POST"]
            self.assertEqual([call[2]["name"] for call in posts],
                             ["Base", "SQL", "Main", "Series", "Port network"])
            self.assertEqual(posts[1][2]["dataset_query"]["native"]["query"],
                             "SELECT * FROM {{#101-base}}")
            self.assertEqual(posts[2][2]["dataset_query"]["query"]["source-table"], "card__102")
            self.assertEqual(posts[-1][2]["parameters"], dashboard["parameters"])
            self.assertTrue(all(call[2]["collection_id"] == 7 for call in posts))
            method, path, layout = api.calls[-1]
            self.assertEqual((method, path), ("PUT", "/api/dashboard/105/cards"))
            self.assertEqual(layout["ordered_tabs"][0]["name"], "Overview")
            self.assertEqual(layout["cards"][0]["card_id"], 103)
            self.assertEqual(layout["cards"][0]["parameter_mappings"][0]["card_id"], 103)
            self.assertEqual(layout["cards"][0]["series"][0]["card_id"], 104)
            self.assertEqual(layout["cards"][0]["dashboard_tab_id"], -1)
            self.assertEqual(layout["cards"][1]["card_id"], None)
            mapping = json.loads((root / "import_id_map.json").read_text())
            self.assertEqual(mapping["cards"], {"1": 103, "2": 102, "3": 101, "4": 104})

    def test_dry_run_rejects_missing_dependency_without_api_or_writes(self):
        dashboard, cards = example_objects()
        cards.pop(3)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "cards").mkdir()
            (root / "dashboard.json").write_text(json.dumps(dashboard))
            for cid, card in cards.items():
                (root / "cards" / f"{cid}.json").write_text(json.dumps(card))
            with patch.object(sys, "argv", ["import_metabase.py", str(root), "--dry-run"]), \
                 patch.object(importer, "Metabase") as api, \
                 contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "not present in export"):
                    importer.main()
                api.assert_not_called()
            self.assertFalse((root / "import_id_map.json").exists())

    def test_missing_filter_card_fails_before_api_writes(self):
        dashboard, cards = example_objects()
        dashboard["dashcards"][0]["parameter_mappings"][0]["card_id"] = 999
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "cards").mkdir()
            (root / "dashboard.json").write_text(json.dumps(dashboard))
            for cid, card in cards.items():
                (root / "cards" / f"{cid}.json").write_text(json.dumps(card))
            with patch.object(sys, "argv", ["import_metabase.py", str(root), "--dry-run"]), \
                 patch.object(importer, "Metabase") as api, \
                 contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "Dashboard references card 999"):
                    importer.main()
                api.assert_not_called()

    def test_export_refuses_nonempty_directory_so_stale_cards_cannot_leak_in(self):
        dashboard, cards = example_objects()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "old.json").write_text("{}")
            with patch.object(sys, "argv", ["export_metabase.py", "9", "--output", str(root)]), \
                 patch.dict(os.environ, {"METABASE_URL": "http://example", "METABASE_API_KEY": "test"}), \
                 patch.object(exporter, "Metabase", return_value=FakeExportAPI(dashboard, cards)), \
                 contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    exporter.main()
            self.assertTrue((root / "old.json").exists())


if __name__ == "__main__":
    unittest.main()
