from __future__ import annotations

import os
import re
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from app.services.analytics import (
    enrich_financial_dataframe,
    normalize_stock_code_series,
    normalize_stock_code_text,
    prepare_financial_dataframe,
    to_optional_float,
)
from app.services.jquants_client import JQuantsClient


class BulkDataCache:
    SUMMARY_COLUMNS = [
        "Code",
        "DiscDate",
        "DiscTime",
        "DiscNo",
        "DocType",
        "CurPerType",
        "CurPerEn",
        "CurFYEn",
        "Sales",
        "OP",
        "OdP",
        "NP",
        "EPS",
        "BPS",
        "TA",
        "Eq",
        "EqAR",
        "ShOutFY",
        "FSales",
        "FOP",
        "FOdP",
        "FNP",
        "FEPS",
        "NxFSales",
        "NxFOP",
        "NxFOdP",
        "NxFNp",
        "NxFEPS",
    ]
    DAILY_COLUMNS = ["Date", "Code", "C", "Vo"]

    def __init__(self, client: JQuantsClient, cache_dir: str | None = None) -> None:
        self.client = client
        self.cache_root = Path(cache_dir or os.getenv("JQUANTS_CACHE_DIR", "cache/jquants"))
        self.cache_root.mkdir(parents=True, exist_ok=True)

        self.summary_months = int(os.getenv("JQUANTS_BULK_MONTHS", "60"))
        self._file_index_cache: dict[tuple[str, str], list[dict[str, object]]] = {}

    def _extract_file_date(self, key: str) -> pd.Timestamp | None:
        match = re.search(r"_(\d{6}|\d{8})\.csv\.gz$", key)
        if not match:
            return None

        stamp = match.group(1)
        if len(stamp) == 6:
            return pd.to_datetime(f"{stamp}01", format="%Y%m%d", errors="coerce")
        return pd.to_datetime(stamp, format="%Y%m%d", errors="coerce")

    def _list_files(self, endpoint: str) -> list[dict[str, object]]:
        cache_key = (endpoint, date.today().isoformat())
        if cache_key not in self._file_index_cache:
            self._file_index_cache[cache_key] = self.client.fetch_bulk_file_list(endpoint)
        return self._file_index_cache[cache_key]

    def _select_summary_keys(self) -> list[str]:
        entries: list[tuple[pd.Timestamp, str]] = []
        cutoff = pd.Timestamp(date.today()) - pd.DateOffset(months=self.summary_months)

        for item in self._list_files("/fins/summary"):
            key = str(item["Key"])
            file_date = self._extract_file_date(key)
            if file_date is None:
                continue
            if file_date >= cutoff:
                entries.append((file_date, key))

        entries.sort(key=lambda item: item[0])
        return [key for _, key in entries]

    def _select_daily_keys(self, limit: int = 7) -> list[str]:
        entries: list[tuple[pd.Timestamp, str]] = []
        for item in self._list_files("/equities/bars/daily"):
            key = str(item["Key"])
            file_date = self._extract_file_date(key)
            if file_date is None:
                continue
            entries.append((file_date, key))

        entries.sort(key=lambda item: item[0])
        return [key for _, key in entries[-limit:]]

    def ensure_file(self, key: str) -> Path:
        path = self.cache_root / key
        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists() and path.stat().st_size > 0:
            return path

        url = self.client.fetch_bulk_download_url(key)
        temporary_path = path.with_suffix(path.suffix + ".tmp")

        with requests.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()
            with temporary_path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        handle.write(chunk)

        temporary_path.replace(path)
        return path

    def load_summary_frame(self, sector_codes: set[str]) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for key in self._select_summary_keys():
            path = self.ensure_file(key)
            frame = pd.read_csv(
                path,
                compression="gzip",
                usecols=lambda column: column in self.SUMMARY_COLUMNS,
                dtype={"Code": "string", "DiscNo": "string", "DocType": "string", "CurPerType": "string"},
                low_memory=False,
            )
            frame["Code"] = normalize_stock_code_series(frame["Code"])
            frame = frame[frame["Code"].isin(sector_codes)]
            if not frame.empty:
                frames.append(frame)

        if not frames:
            return pd.DataFrame(columns=self.SUMMARY_COLUMNS)

        return pd.concat(frames, ignore_index=True)

    def load_latest_prices(self, sector_codes: set[str]) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for key in self._select_daily_keys():
            path = self.ensure_file(key)
            frame = pd.read_csv(
                path,
                compression="gzip",
                usecols=lambda column: column in self.DAILY_COLUMNS,
                dtype={"Code": "string"},
                low_memory=False,
            )
            frame["Code"] = normalize_stock_code_series(frame["Code"])
            frame = frame[frame["Code"].isin(sector_codes)]
            if frame.empty:
                continue
            frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
            frame["C"] = pd.to_numeric(frame["C"], errors="coerce")
            frames.append(frame)

        if not frames:
            return pd.DataFrame(columns=self.DAILY_COLUMNS)

        prices = pd.concat(frames, ignore_index=True)
        prices = prices.sort_values(["Code", "Date"], kind="stable")
        prices = prices.drop_duplicates(subset=["Code"], keep="last")
        return prices.reset_index(drop=True)

    def load_price_snapshots(
        self,
        sector_codes: set[str],
        target_dates: list[pd.Timestamp],
        lookback_days: int = 14,
    ) -> pd.DataFrame:
        normalized_codes = {
            normalized
            for code in sector_codes
            if (normalized := normalize_stock_code_text(code))
        }
        normalized_targets = sorted(
            {
                pd.Timestamp(value).normalize()
                for value in target_dates
                if value is not None and not pd.isna(value)
            }
        )
        if not normalized_codes or not normalized_targets:
            return pd.DataFrame(columns=["TargetDate", "Code", "Date", "C"])

        available_entries: list[tuple[pd.Timestamp, str]] = []
        for item in self._list_files("/equities/bars/daily"):
            key = str(item["Key"])
            file_date = self._extract_file_date(key)
            if file_date is None:
                continue
            available_entries.append((file_date.normalize(), key))

        selected_keys: set[str] = set()
        for target_date in normalized_targets:
            earliest_date = target_date - pd.Timedelta(days=lookback_days)
            for file_date, key in available_entries:
                if earliest_date <= file_date <= target_date:
                    selected_keys.add(key)

        frames: list[pd.DataFrame] = []
        for key in sorted(selected_keys):
            path = self.ensure_file(key)
            frame = pd.read_csv(
                path,
                compression="gzip",
                usecols=lambda column: column in self.DAILY_COLUMNS,
                dtype={"Code": "string"},
                low_memory=False,
            )
            frame["Code"] = normalize_stock_code_series(frame["Code"])
            frame = frame[frame["Code"].isin(normalized_codes)]
            if frame.empty:
                continue
            frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
            frame["C"] = pd.to_numeric(frame["C"], errors="coerce")
            frames.append(frame)

        if not frames:
            return pd.DataFrame(columns=["TargetDate", "Code", "Date", "C"])

        daily_prices = pd.concat(frames, ignore_index=True)
        daily_prices = daily_prices.sort_values(["Code", "Date"], kind="stable")

        snapshots: list[pd.DataFrame] = []
        for target_date in normalized_targets:
            price_frame = daily_prices[daily_prices["Date"] <= target_date]
            if price_frame.empty:
                continue

            latest = price_frame.drop_duplicates(subset=["Code"], keep="last").copy()
            latest["TargetDate"] = target_date
            snapshots.append(latest[["TargetDate", "Code", "Date", "C"]])

        if not snapshots:
            return pd.DataFrame(columns=["TargetDate", "Code", "Date", "C"])

        return pd.concat(snapshots, ignore_index=True)

    def compute_sector_averages(self, sector_codes: list[str]) -> dict[str, float | int | None]:
        normalized_codes = {
            normalized
            for code in sector_codes
            if (normalized := normalize_stock_code_text(code))
        }
        summary_frame = self.load_summary_frame(normalized_codes)
        if summary_frame.empty:
            return {"psr": None, "per": None, "peer_count": 0, "psr_count": 0, "per_count": 0}

        financial_df = enrich_financial_dataframe(
            prepare_financial_dataframe(summary_frame.to_dict(orient="records"))
        )
        if financial_df.empty:
            return {"psr": None, "per": None, "peer_count": 0, "psr_count": 0, "per_count": 0}

        latest_financials = (
            financial_df.sort_values(["Code", "CurPerEn", "DiscDate", "DiscTime"], kind="stable")
            .groupby("Code", sort=False)
            .tail(1)
            .copy()
        )

        latest_prices = self.load_latest_prices(normalized_codes)
        latest_financials = latest_financials.merge(
            latest_prices[["Code", "Date", "C"]],
            on="Code",
            how="left",
        )

        latest_financials["MarketCap"] = latest_financials["C"] * latest_financials["ShOutFY"]
        latest_financials["PSR"] = np.where(
            (latest_financials["MarketCap"] > 0) & (latest_financials["TTM_Sales"] > 0),
            latest_financials["MarketCap"] / latest_financials["TTM_Sales"],
            np.nan,
        )
        latest_financials["PER"] = np.where(
            (latest_financials["MarketCap"] > 0) & (latest_financials["TTM_NP"] > 0),
            latest_financials["MarketCap"] / latest_financials["TTM_NP"],
            np.nan,
        )

        return {
            "psr": to_optional_float(latest_financials["PSR"].mean(skipna=True)),
            "per": to_optional_float(latest_financials["PER"].mean(skipna=True)),
            "peer_count": int(latest_financials["Code"].nunique()),
            "psr_count": int(latest_financials["PSR"].notna().sum()),
            "per_count": int(latest_financials["PER"].notna().sum()),
        }
