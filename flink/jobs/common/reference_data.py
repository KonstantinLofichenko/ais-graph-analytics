"""Read the existing dbt seeds; never maintain a second set of AIS mappings."""

import csv
from pathlib import Path
import re


def load_reference(path, key, attributes):
    with Path(path).open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if not {key, *attributes}.issubset(reader.fieldnames or []):
            raise ValueError(f"Missing required reference columns in {path}")
        reference = {}
        for row in reader:
            if any(row.get(name) is None for name in (key, *attributes)) or None in row:
                raise ValueError(f"Invalid reference row in {path} at line {reader.line_num}")
            code = row[key].strip()
            if not re.fullmatch(r"[0-9]+", code):
                raise ValueError(f"Empty or invalid reference code in {path}: {code!r}")
            if code in reference:
                raise ValueError(f"Duplicate reference code in {path}: {code!r}")
            reference[code] = {name: row[name].strip() or None for name in attributes}
        return reference


class ReferenceData:
    """Construct once in an operator's open(); lookups never perform file I/O."""

    def __init__(self, directory):
        directory = Path(directory)
        self.ship_types = load_reference(directory / "ais_ship_types.csv", "ship_type",
                                         ("ship_type_name", "ship_category"))
        self.navigation_statuses = load_reference(
            directory / "ais_navigational_status.csv", "navigational_status",
            ("navigational_status_name",),
        )

    def ship(self, code):
        row = self.ship_types.get(str(code).strip(), {})
        return {"ship_type": code, "ship_type_name": row.get("ship_type_name"),
                "ship_category": row.get("ship_category")}

    def navigation_name(self, code):
        return self.navigation_statuses.get(str(code).strip(), {}).get("navigational_status_name")
