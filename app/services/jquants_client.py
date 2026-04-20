from __future__ import annotations

import os
import re
from typing import Any

import requests


class JQuantsClient:
    """Small wrapper around J-Quants V2."""

    BASE_URL = "https://api.jquants.com/v2"

    def __init__(self, api_key: str | None = None, timeout: int = 30) -> None:
        self.api_key = api_key or os.getenv("JQUANTS_API_KEY")
        if not self.api_key:
            raise ValueError("環境変数 JQUANTS_API_KEY が設定されていません。")

        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"x-api-key": self.api_key})

    def _request_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        response = self.session.get(
            f"{self.BASE_URL}{path}",
            params=params or {},
            timeout=timeout or self.timeout,
        )
        response.raise_for_status()
        payload = response.json()

        if isinstance(payload, dict) and payload.get("error"):
            raise RuntimeError(payload["error"])

        return payload

    def paginate(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        current_params = dict(params or {})
        rows: list[dict[str, Any]] = []

        while True:
            payload = self._request_json(path, params=current_params)
            data = payload.get("data", [])
            if not isinstance(data, list):
                raise RuntimeError(f"Unexpected response shape from {path}")

            rows.extend(data)
            pagination_key = payload.get("pagination_key")
            if not pagination_key:
                break

            current_params["pagination_key"] = pagination_key

        return rows

    def fetch_equity_master(self, code: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        return self.paginate("/equities/master", params=params)

    def fetch_daily_bars(
        self,
        code: str,
        from_date: str,
        to_date: str,
    ) -> list[dict[str, Any]]:
        params = {"code": code, "from": from_date, "to": to_date}
        try:
            return self.paginate("/equities/bars/daily", params=params)
        except requests.HTTPError as exc:
            response = exc.response
            if response is None:
                raise

            retry_from = self._extract_subscription_start(response)
            if retry_from and retry_from > from_date:
                retry_params = {"code": code, "from": retry_from, "to": to_date}
                return self.paginate("/equities/bars/daily", params=retry_params)
            raise

    def fetch_fins_summary(self, code: str) -> list[dict[str, Any]]:
        return self.paginate("/fins/summary", params={"code": code})

    def fetch_bulk_file_list(self, endpoint: str) -> list[dict[str, Any]]:
        payload = self._request_json("/bulk/list", params={"endpoint": endpoint})
        data = payload.get("data", [])
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected bulk file list response for {endpoint}")
        return data

    def fetch_bulk_download_url(self, key: str) -> str:
        payload = self._request_json("/bulk/get", params={"key": key}, timeout=60)
        url = payload.get("url")
        if not isinstance(url, str) or not url:
            raise RuntimeError(f"bulk/get did not return a URL for {key}")
        return url

    def _extract_subscription_start(self, response: requests.Response) -> str | None:
        try:
            payload = response.json()
            message = payload.get("message", "")
        except ValueError:
            message = response.text

        match = re.search(r"(\d{4}-\d{2}-\d{2})", message or "")
        if not match:
            return None
        return match.group(1).replace("-", "")
