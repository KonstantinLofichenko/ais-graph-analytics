#!/usr/bin/env python3
"""
Import a JSON export produced by export_metabase.py.

The importer:
  1. creates saved questions/cards in dependency order,
  2. remaps references like card__123 -> card__NEW_ID,
  3. creates the dashboard,
  4. restores tabs, card layout, filter mappings and visualization settings.

Environment variables:
  METABASE_URL
  METABASE_API_KEY     preferred when available
  METABASE_SESSION
  METABASE_USER
  METABASE_PASSWORD

Example:
  export METABASE_URL=http://localhost:3000
  export METABASE_API_KEY=...
  python import_metabase.py metabase_export --collection-id 4

Use --dry-run to validate/read the export without changing Metabase.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

import requests
from export_metabase import CARD_REF_RE, SQL_CARD_REF_RE, card_dependencies, setting


class Metabase:
    def __init__(self, base_url: str, api_key: str | None, session_token: str | None,
                 username: str | None, password: str | None, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

        if api_key:
            self.session.headers["X-API-Key"] = api_key
        elif session_token:
            self.session.headers["X-Metabase-Session"] = session_token
        elif username and password:
            r = self.session.post(
                f"{self.base_url}/api/session",
                json={"username": username, "password": password},
                timeout=self.timeout,
            )
            self._raise(r)
            self.session.headers["X-Metabase-Session"] = r.json()["id"]
        else:
            raise SystemExit(
                "Authentication required: set METABASE_API_KEY, METABASE_SESSION, "
                "or METABASE_USER + METABASE_PASSWORD."
            )

    @staticmethod
    def _raise(r: requests.Response) -> None:
        if not r.ok:
            raise RuntimeError(f"Metabase API returned HTTP {r.status_code}")

    def post(self, path: str, payload: dict[str, Any]) -> Any:
        r = self.session.post(
            f"{self.base_url}{path}", json=payload, timeout=self.timeout
        )
        self._raise(r)
        return r.json() if r.content else None

    def put(self, path: str, payload: dict[str, Any]) -> Any:
        r = self.session.put(
            f"{self.base_url}{path}", json=payload, timeout=self.timeout
        )
        self._raise(r)
        return r.json() if r.content else None


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def walk_replace_card_refs(obj: Any, id_map: dict[int, int]) -> Any:
    """
    Recursively replace Metabase MBQL saved-question references:
        card__OLD_ID -> card__NEW_ID
    """
    if isinstance(obj, dict):
        return {k: walk_replace_card_refs(v, id_map) for k, v in obj.items()}
    if isinstance(obj, list):
        return [walk_replace_card_refs(v, id_map) for v in obj]
    if isinstance(obj, str):
        m = CARD_REF_RE.match(obj)
        if m:
            old_id = int(m.group(1))
            if old_id not in id_map:
                raise ValueError(
                    f"Card depends on saved question {old_id}, which has not been imported."
                )
            return f"card__{id_map[old_id]}"
        def replace_sql(match):
            old_id = int(match.group(1))
            if old_id not in id_map:
                raise ValueError(f"Card depends on saved question {old_id}, which has not been imported.")
            return match.group(0).replace(f"#{old_id}", f"#{id_map[old_id]}", 1)
        return SQL_CARD_REF_RE.sub(replace_sql, obj)
    return obj


def source_dependencies(card: dict[str, Any]) -> set[int]:
    return card_dependencies(card)


CARD_CREATE_KEYS = {
    "name",
    "description",
    "dataset_query",
    "display",
    "visualization_settings",
    "collection_id",
    "collection_position",
    "type",
    "parameters",
    "parameter_mappings",
    "result_metadata",
    "cache_ttl",
    "enable_embedding",
    "embedding_params",
}


def card_create_payload(
    card: dict[str, Any],
    id_map: dict[int, int],
    collection_id: int | None,
) -> dict[str, Any]:
    payload = {
        k: copy.deepcopy(v)
        for k, v in card.items()
        if k in CARD_CREATE_KEYS
    }

    payload["dataset_query"] = walk_replace_card_refs(
        payload.get("dataset_query", {}), id_map
    )

    # Destination collection IDs are installation-specific.
    payload["collection_id"] = collection_id

    # Current Metabase uses type=question/model/metric rather than dataset=true/false.
    if not payload.get("type"):
        payload["type"] = "question"

    # Do not send null/obsolete metadata if it is absent from the source semantics.
    payload = {k: v for k, v in payload.items() if v is not None or k == "collection_id"}

    required = ("name", "dataset_query", "display")
    missing = [k for k in required if k not in payload]
    if missing:
        raise ValueError(f"Card is missing required field(s): {', '.join(missing)}")

    return payload


DASHBOARD_CREATE_KEYS = {
    "name",
    "description",
    "parameters",
    "width",
    "auto_apply_filters",
    "cache_ttl",
}


def dashboard_create_payload(
    dashboard: dict[str, Any], collection_id: int | None, name_suffix: str
) -> dict[str, Any]:
    payload = {
        k: copy.deepcopy(v)
        for k, v in dashboard.items()
        if k in DASHBOARD_CREATE_KEYS
    }
    payload["name"] = f"{dashboard.get('name', 'Imported dashboard')}{name_suffix}"
    payload["collection_id"] = collection_id
    return {k: v for k, v in payload.items() if v is not None or k == "collection_id"}


def get_dashcards(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    cards = (
        dashboard.get("dashcards")
        or dashboard.get("ordered_cards")
        or dashboard.get("cards")
        or []
    )
    return [c for c in cards if isinstance(c, dict)]


def get_tabs(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    tabs = dashboard.get("tabs") or dashboard.get("ordered_tabs") or []
    return [t for t in tabs if isinstance(t, dict)]


def old_card_id(dc: dict[str, Any]) -> int | None:
    cid = dc.get("card_id")
    if isinstance(cid, int) and cid > 0:
        return cid
    card = dc.get("card")
    if isinstance(card, dict):
        cid = card.get("id")
        if isinstance(cid, int) and cid > 0:
            return cid
    return None


def remap_card_id(old_id: Any, card_id_map: dict[int, int]) -> Any:
    if old_id is None:
        return None
    if not isinstance(old_id, int) or old_id not in card_id_map:
        raise ValueError(f"Dashboard references card {old_id}, but it was not imported.")
    return card_id_map[old_id]


def make_layout_payload(
    dashboard: dict[str, Any],
    card_id_map: dict[int, int],
) -> dict[str, Any]:
    source_tabs = get_tabs(dashboard)
    tab_id_map: dict[int, int] = {}
    ordered_tabs: list[dict[str, Any]] = []

    # Negative IDs tell Metabase these are new objects.
    for index, tab in enumerate(source_tabs, start=1):
        old_id = tab.get("id")
        new_temp_id = -index
        if isinstance(old_id, int):
            tab_id_map[old_id] = new_temp_id

        new_tab = {
            "id": new_temp_id,
            "name": tab.get("name") or f"Tab {index}",
        }
        if "position" in tab:
            new_tab["position"] = tab["position"]
        ordered_tabs.append(new_tab)

    cards: list[dict[str, Any]] = []
    next_negative = -1

    for dc in get_dashcards(dashboard):
        new_dc: dict[str, Any] = {}
        new_dc["id"] = next_negative
        next_negative -= 1

        source_card_id = old_card_id(dc)
        if source_card_id is None:
            # Text/heading/action/virtual dashcard.
            new_dc["card_id"] = None
        else:
            new_dc["card_id"] = remap_card_id(source_card_id, card_id_map)

        # Preserve fields accepted by the dashboard-card bulk update API.
        for key in (
            "row",
            "col",
            "size_x",
            "size_y",
            "parameter_mappings",
            "inline_parameters",
            "visualization_settings",
            "action_id",
            "series",
        ):
            if key in dc:
                new_dc[key] = copy.deepcopy(dc[key])

        if isinstance(new_dc.get("parameter_mappings"), list):
            for mapping in new_dc["parameter_mappings"]:
                if isinstance(mapping, dict) and "card_id" in mapping:
                    mapping["card_id"] = remap_card_id(mapping["card_id"], card_id_map)

        # Hydrated virtual cards may carry the virtual card definition.
        if source_card_id is None and isinstance(dc.get("card"), dict):
            virtual = copy.deepcopy(dc["card"])
            virtual.pop("id", None)
            new_dc["card"] = virtual

        old_tab_id = dc.get("dashboard_tab_id")
        if old_tab_id is not None:
            if old_tab_id not in tab_id_map:
                raise ValueError(f"Dashboard references missing tab {old_tab_id}")
            new_dc["dashboard_tab_id"] = tab_id_map[old_tab_id]

        # Remap IDs in series definitions.
        if isinstance(new_dc.get("series"), list):
            remapped_series = []
            for item in new_dc["series"]:
                if isinstance(item, dict):
                    item = copy.deepcopy(item)
                    sid = item.get("id") or item.get("card_id")
                    if sid is not None:
                        new_id = remap_card_id(sid, card_id_map)
                        if "id" in item:
                            item["id"] = new_id
                        if "card_id" in item:
                            item["card_id"] = new_id
                remapped_series.append(item)
            new_dc["series"] = remapped_series

        cards.append(new_dc)

    payload: dict[str, Any] = {"cards": cards}
    if ordered_tabs:
        payload["ordered_tabs"] = ordered_tabs
    return payload


def main() -> int:
    p = argparse.ArgumentParser(
        description="Import a Metabase dashboard/cards JSON export."
    )
    p.add_argument("export_dir")
    p.add_argument("--collection-id", type=int, default=None,
                   help="Destination Metabase collection ID; default: root collection.")
    p.add_argument("--name-suffix", default="",
                   help="Optional suffix appended to imported dashboard name.")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--url", default=setting("URL"))
    p.add_argument("--timeout", type=int, default=30)
    args = p.parse_args()

    root = Path(args.export_dir)
    dashboard_path = root / "dashboard.json"
    cards_dir = root / "cards"

    if not dashboard_path.exists():
        p.error(f"Missing {dashboard_path}")
    if not cards_dir.is_dir():
        p.error(f"Missing {cards_dir}")

    dashboard = load_json(dashboard_path)
    if not isinstance(dashboard, dict):
        raise ValueError("dashboard.json must contain an object")
    source_cards: dict[int, dict[str, Any]] = {}
    source_files: dict[str, str] = {}
    for path in sorted(cards_dir.glob("*.json")):
        card = load_json(path)
        if not isinstance(card, dict):
            raise ValueError(f"{path} must contain a card object")
        cid = card.get("id")
        if (not isinstance(cid, int) or cid <= 0
                or not (path.stem == str(cid) or path.stem.startswith(f"{cid}-"))):
            raise ValueError(f"{path} filename must start with its positive card ID")
        if cid in source_cards:
            raise ValueError(f"Duplicate card ID {cid}")
        source_cards[cid] = card
        source_files[str(cid)] = path.name

    manifest_path = root / "manifest.json"
    if manifest_path.exists():
        manifest = load_json(manifest_path)
        if (not isinstance(manifest, dict)
                or manifest.get("format") != "metabase-dashboard-json-export"
                or manifest.get("format_version") != 1
                or manifest.get("dashboard_id") != dashboard.get("id")
                or set(manifest.get("all_card_ids", [])) != set(source_cards)
                or ("card_files" in manifest and manifest["card_files"] != source_files)):
            raise ValueError("Manifest does not match dashboard.json and cards/*.json")

    visible = {cid for cid in (old_card_id(dc) for dc in get_dashcards(dashboard)) if cid is not None}
    if not visible <= source_cards.keys():
        raise ValueError(f"Dashboard cards missing from export: {sorted(visible - source_cards.keys())}")
    for dc in get_dashcards(dashboard):
        for item in dc.get("series") or []:
            if isinstance(item, dict):
                sid = item.get("id") or item.get("card_id")
                if sid is not None and sid not in source_cards:
                    raise ValueError(f"Dashboard series card {sid} missing from export")

    # Topological import order for cards that depend on other saved questions.
    remaining = set(source_cards)
    order: list[int] = []
    resolved: set[int] = set()

    while remaining:
        progress = False
        for cid in sorted(remaining):
            deps = source_dependencies(source_cards[cid])
            internal_deps = deps & set(source_cards)
            missing_external = deps - set(source_cards)
            if missing_external:
                raise ValueError(
                    f"Card {cid} depends on card(s) not present in export: "
                    f"{sorted(missing_external)}"
                )
            if internal_deps <= resolved:
                order.append(cid)
                resolved.add(cid)
                remaining.remove(cid)
                progress = True
                break
        if not progress:
            raise ValueError(
                f"Cannot resolve saved-question dependency cycle: {sorted(remaining)}"
            )

    print(f"Cards/questions to import: {len(order)}")
    print("Import order:", ", ".join(map(str, order)))

    # Check layout and filter references before creating any remote objects.
    make_layout_payload(dashboard, {cid: cid for cid in source_cards})

    if args.dry_run:
        print("Dry run: no changes made.")
        return 0

    if not args.url:
        p.error("Metabase URL is required via --url or METABASE_URL.")

    mb = Metabase(
        args.url, setting("API_KEY"), setting("SESSION"),
        setting("USER"), setting("PASSWORD"), args.timeout
    )

    id_map: dict[int, int] = {}

    for old_id in order:
        payload = card_create_payload(
            source_cards[old_id], id_map, args.collection_id
        )
        created = mb.post("/api/card", payload)
        new_id = created["id"]
        id_map[old_id] = new_id
        print(f"Card {old_id} -> {new_id}: {payload['name']}")

    dashboard_payload = dashboard_create_payload(
        dashboard, args.collection_id, args.name_suffix
    )
    created_dashboard = mb.post("/api/dashboard", dashboard_payload)
    new_dashboard_id = created_dashboard["id"]
    print(
        f"Dashboard {dashboard.get('id')} -> {new_dashboard_id}: "
        f"{dashboard_payload['name']}"
    )

    layout_payload = make_layout_payload(dashboard, id_map)

    # Metabase 0.47+ bulk dashboard-card editing endpoint.
    # New dashcards/tabs use negative temporary IDs.
    mb.put(f"/api/dashboard/{new_dashboard_id}/cards", layout_payload)

    mapping_path = root / "import_id_map.json"
    mapping_path.write_text(
        json.dumps(
            {
                "dashboard": {
                    "old_id": dashboard.get("id"),
                    "new_id": new_dashboard_id,
                },
                "cards": {str(k): v for k, v in sorted(id_map.items())},
            },
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )

    print(f"Import complete. ID mapping written to {mapping_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
