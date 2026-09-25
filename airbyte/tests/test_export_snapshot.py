"""Offline validation of the versionable Airbyte country pipeline snapshot."""
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT = ROOT / "airbyte/exports/countries-to-clickhouse"


class AirbyteSnapshotTests(unittest.TestCase):
    def test_manifest_and_named_files_are_the_single_configuration_copy(self):
        manifest = json.loads((SNAPSHOT / "manifest.json").read_text())
        self.assertEqual(manifest["format"], "airbyte-countries-to-clickhouse-config-export")
        self.assertTrue(manifest["live_api_verified"])
        expected = {
            "countries-api-connector.yaml",
            "countries-api-source.json",
            "clickhouse-destination.json",
            "countries-to-clickhouse-connection.json",
        }
        self.assertEqual(set(manifest["files"].values()), expected)
        self.assertEqual({p.name for p in SNAPSHOT.iterdir()}, expected | {"manifest.json"})
        for name in expected:
            self.assertTrue((SNAPSHOT / name).is_file())
        for redundant in (
            "countries_api.yaml",
            "source-rest-countries.example.json",
            "destination-clickhouse.example.json",
            "connection-countries-to-clickhouse.example.json",
        ):
            self.assertFalse((ROOT / "airbyte" / redundant).exists())

    def test_objects_and_placeholders_form_one_country_sync(self):
        source = json.loads((SNAPSHOT / "countries-api-source.json").read_text())
        destination = json.loads((SNAPSHOT / "clickhouse-destination.json").read_text())
        connection = json.loads((SNAPSHOT / "countries-to-clickhouse-connection.json").read_text())
        manifest = json.loads((SNAPSHOT / "manifest.json").read_text())
        connector = (SNAPSHOT / "countries-api-connector.yaml").read_text()

        self.assertEqual(source["name"], "Countries API")
        self.assertEqual(destination["name"], "ClickHouse")
        self.assertEqual(destination["configuration"]["database"], "raw")
        self.assertEqual(connection["configurations"]["streams"][0]["name"], "countries")
        self.assertEqual(connection["configurations"]["streams"][0]["syncMode"],
                         "full_refresh_overwrite")
        self.assertEqual(connection["sourceId"], "REPLACE_WITH_CREATED_SOURCE_ID")
        self.assertEqual(connection["destinationId"], "REPLACE_WITH_CREATED_DESTINATION_ID")
        self.assertIn("name: countries", connector)
        self.assertIn("airbyte_secret: true", connector)
        text = "\n".join(path.read_text() for path in SNAPSHOT.iterdir() if path.is_file())
        for placeholder in manifest["required_placeholders"]:
            self.assertIn(placeholder, text)
        self.assertNotRegex(text, r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")

    def test_no_local_credentials_appear_when_env_file_exists(self):
        env_path = ROOT / ".env"
        if not env_path.exists():
            self.skipTest("No local .env file")
        values = {}
        for line in env_path.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
        text = "\n".join(path.read_text() for path in SNAPSHOT.iterdir() if path.is_file())
        for key in ("AIRBYTE_API_KEY", "AIRBYTE_ACCESS_TOKEN", "REST_COUNTRIES_API_KEY",
                    "CLICKHOUSE_PASSWORD"):
            value = values.get(key, "")
            if len(value) >= 8 and not value.startswith("your_"):
                self.assertNotIn(value, text, f"{key} appears in versionable Airbyte export")


if __name__ == "__main__":
    unittest.main()
