"""Small BarentsWatch Historic AIS API client."""

from datetime import datetime
import time
from typing import Any, Optional
from urllib.parse import quote

import requests


class HistoricAISClient:
    def __init__(self, token_url: str, base_url: str, client_id: str, client_secret: str,
                 scope: str, timeout_seconds: float, session: Optional[requests.Session] = None):
        self.token_url = token_url.rstrip('/')
        self.base_url = base_url.rstrip('/')
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.timeout = timeout_seconds
        self.session = session or requests.Session()
        self._access_token = None
        self._token_expires_at = 0.0

    def close(self) -> None:
        self.session.close()

    def _token(self) -> str:
        now = time.monotonic()
        if self._access_token and now < self._token_expires_at:
            return self._access_token
        try:
            response = self.session.post(
                self.token_url,
                data={'client_id': self.client_id, 'client_secret': self.client_secret,
                      'scope': self.scope, 'grant_type': 'client_credentials'},
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise RuntimeError('Historic AIS OAuth request failed') from exc
        if not response.ok:
            raise RuntimeError(
                f'Historic AIS OAuth request failed (HTTP {response.status_code}): '
                f'{response.text[:1000]}'
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError('Historic AIS OAuth response was malformed') from exc
        token = payload.get('access_token') if isinstance(payload, dict) else None
        if not token:
            raise RuntimeError('Historic AIS OAuth response did not contain a token')
        try:
            expires_in = float(payload.get('expires_in', 300))
            if expires_in <= 0:
                raise ValueError
        except (TypeError, ValueError):
            expires_in = 300.0
        self._access_token = token
        self._token_expires_at = time.monotonic() + max(expires_in - 60.0, 0.0)
        return token

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = dict(kwargs.pop('headers', {}))
        headers['Authorization'] = f'Bearer {self._token()}'
        try:
            response = self.session.request(
                method, f'{self.base_url}/{path.lstrip("/")}',
                headers=headers, timeout=self.timeout, **kwargs,
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            raise RuntimeError(f'Historic AIS {method} request failed') from exc

    @staticmethod
    def _require_list(payload: Any, name: str) -> list:
        if not isinstance(payload, list):
            raise RuntimeError(f'Historic AIS {name} response was not a list')
        return payload

    def mmsis_in_area(self, polygon: dict, start: datetime, end: datetime) -> list:
        payload = self._request(
            'POST', 'v1/historic/mmsiinarea',
            json={'msgtimefrom': start.isoformat(), 'msgtimeto': end.isoformat(), 'polygon': polygon},
        )
        values = self._require_list(payload, 'mmsiinarea')
        if not all(isinstance(value, int) and not isinstance(value, bool) for value in values):
            raise RuntimeError('Historic AIS mmsiinarea response contained a non-integer MMSI')
        return values

    def tracks(self, mmsi: int, start: datetime, end: datetime) -> list:
        payload = self._request(
            'GET', f'v1/historic/tracks/{mmsi}/{quote(start.isoformat(), safe="")}/{quote(end.isoformat(), safe="")}',
        )
        values = self._require_list(payload, 'tracks')
        if not all(isinstance(value, dict) for value in values):
            raise RuntimeError('Historic AIS tracks response contained a non-object record')
        return values