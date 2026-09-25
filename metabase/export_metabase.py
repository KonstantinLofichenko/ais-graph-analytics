#!/usr/bin/env python3
"""
Export a Metabase dashboard and all referenced saved questions/cards to JSON.

Environment variables:
  METABASE_URL         e.g. http://localhost:3000
  METABASE_API_KEY     preferred when available
  METABASE_SESSION     existing Metabase session token
  METABASE_USER        username/email (used with METABASE_PASSWORD)
  METABASE_PASSWORD    password

Example:
  export METABASE_URL=http://localhost:3000
  export METABASE_API_KEY=...
  python export_metabase.py 12 --output metabase_export
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable

import requests

CARD_REF_RE = re.compile(r"^card__(\d+)$")
SQL_CARD_REF_RE = re.compile(r"\{\{\s*#(\d+)(?:-[^{}]*?)?\s*\}\}")


def setting(name: str) -> str | None:
    """Accept the repository's METABASE_* names and older MB_* aliases."""
    return os.getenv(f"METABASE_{name}") or os.getenv(f"MB_{name}")


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

    def get(self, path: str) -> Any:
        r = self.session.get(f"{self.base_url}{path}", timeout=self.timeout)
        self._raise(r)
        return r.json()


def dump_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


CARD_EXPORT_KEYS = (
    "id", "name", "description", "dataset_query", "display", "type",
    "visualization_settings", "parameters", "parameter_mappings", "cache_ttl",
)
DASHBOARD_EXPORT_KEYS = (
    "id", "name", "description", "parameters", "width", "auto_apply_filters",
    "cache_ttl",
)
DASHCARD_EXPORT_KEYS = (
    "card_id", "row", "col", "size_x", "size_y", "dashboard_tab_id",
    "parameter_mappings", "inline_parameters", "visualization_settings",
    "action_id", "series",
)


def card_filename(card: dict[str, Any]) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(card.get("name") or "card").lower()).strip("-")
    return f"{card['id']}-{slug or 'card'}.json"


def export_card(card: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields needed to recreate a saved card."""
    return {key: card[key] for key in CARD_EXPORT_KEYS if key in card}


def export_dashboard(dashboard: dict[str, Any]) -> dict[str, Any]:
    """Drop personal/activity metadata and hydrated copies of saved cards."""
    result = {key: dashboard[key] for key in DASHBOARD_EXPORT_KEYS if key in dashboard}
    result["tabs"] = dashboard.get("tabs") or dashboard.get("ordered_tabs") or []
    result["dashcards"] = []
    for dc in dashboard.get("dashcards") or dashboard.get("ordered_cards") or dashboard.get("cards") or []:
        item = {key: dc[key] for key in DASHCARD_EXPORT_KEYS if key in dc}
        if item.get("card_id") is None and isinstance(dc.get("card"), dict):
            item["card"] = export_card(dc["card"])
        result["dashcards"].append(item)
    return result


def write_export(root: Path, dashboard: dict[str, Any], cards: dict[int, dict[str, Any]]) -> None:
    """Write a minimal, named export from already fetched Metabase API objects."""
    cards_dir = root / "cards"
    cards_dir.mkdir(parents=True, exist_ok=True)
    dump_json(root / "dashboard.json", export_dashboard(dashboard))
    dependency_map = {}
    card_files = {}
    for card_id, card in sorted(cards.items()):
        filename = card_filename(card)
        card_files[str(card_id)] = filename
        dependency_map[str(card_id)] = sorted(card_dependencies(card) - {card_id})
        dump_json(cards_dir / filename, export_card(card))
    dump_json(root / "manifest.json", {
        "format": "metabase-dashboard-json-export",
        "format_version": 1,
        "dashboard_id": dashboard["id"],
        "dashboard_name": dashboard.get("name"),
        "visible_card_ids": sorted(card_ids_from_dashboard(dashboard)),
        "all_card_ids": sorted(cards),
        "card_dependencies": dependency_map,
        "card_files": card_files,
    })


def walk(obj: Any) -> Iterable[Any]:
    """Yield every scalar/container recursively."""
    yield obj
    if isinstance(obj, dict):
        for value in obj.values():
            yield from walk(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from walk(value)


def card_ids_from_dashboard(dashboard: dict[str, Any]) -> set[int]:
    """
    Extract saved-question IDs from dashboard/dashcard structures across
    Metabase versions. Virtual/text cards with no saved card are ignored.
    """
    ids: set[int] = set()

    dashcards = (
        dashboard.get("dashcards")
        or dashboard.get("ordered_cards")
        or dashboard.get("cards")
        or []
    )

    for dc in dashcards:
        if not isinstance(dc, dict):
            continue

        # Common modern shape: card_id
        cid = dc.get("card_id")
        if isinstance(cid, int) and cid > 0:
            ids.add(cid)

        # Hydrated dashboard responses often include card: {id: ...}
        card = dc.get("card")
        if isinstance(card, dict):
            cid = card.get("id")
            if isinstance(cid, int) and cid > 0:
                ids.add(cid)

        # A dashcard can show additional saved questions as series.
        for series_item in dc.get("series") or []:
            if isinstance(series_item, dict):
                sid = series_item.get("id") or series_item.get("card_id")
                if isinstance(sid, int) and sid > 0:
                    ids.add(sid)

    return ids


def card_dependencies(card: dict[str, Any]) -> set[int]:
    """
    Find query builder `card__123` and native SQL `{{#123-name}}` references.
    """
    deps: set[int] = set()
    for value in walk(card.get("dataset_query", {})):
        if isinstance(value, str):
            m = CARD_REF_RE.match(value)
            if m:
                deps.add(int(m.group(1)))
            deps.update(int(match.group(1)) for match in SQL_CARD_REF_RE.finditer(value))
    return deps


def main() -> int:
    p = argparse.ArgumentParser(
        description="Export a Metabase dashboard and referenced cards/questions to JSON."
    )
    p.add_argument("dashboard_id", type=int)
    p.add_argument("-o", "--output", default="metabase_export")
    p.add_argument("--url", default=setting("URL"))
    p.add_argument("--timeout", type=int, default=30)
    args = p.parse_args()

    if not args.url:
        p.error("Metabase URL is required via --url or METABASE_URL.")

    mb = Metabase(
        args.url, setting("API_KEY"), setting("SESSION"),
        setting("USER"), setting("PASSWORD"), args.timeout
    )

    root = Path(args.output)
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        p.error(f"Output directory must be empty: {root}")

    dashboard = mb.get(f"/api/dashboard/{args.dashboard_id}")
    if dashboard.get("id") != args.dashboard_id:
        raise ValueError("Metabase returned a different dashboard ID")

    pending = list(sorted(card_ids_from_dashboard(dashboard)))
    exported: dict[int, dict[str, Any]] = {}

    while pending:
        card_id = pending.pop(0)
        if card_id in exported:
            continue

        card = mb.get(f"/api/card/{card_id}")
        if card.get("id") != card_id:
            raise ValueError(f"Metabase returned a different ID for card {card_id}")
        exported[card_id] = card

        deps = card_dependencies(card) - {card_id}
        for dep in sorted(deps):
            if dep not in exported and dep not in pending:
                pending.append(dep)

    write_export(root, dashboard, exported)

    print(f"Exported dashboard {args.dashboard_id}: {dashboard.get('name')!r}")
    print(f"Exported {len(exported)} saved question/card(s)")
    print(f"Output: {root.resolve()}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
