from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from app.services.analytics import (
    average_pair,
    build_weekly_dataframe,
    clean_series,
    compute_equity_ratio,
    display_stock_code,
    enrich_financial_dataframe,
    lookup_close_on_or_before,
    normalize_stock_code,
    prepare_daily_bar_dataframe,
    prepare_financial_dataframe,
    to_optional_float,
    to_optional_int,
)
from app.services.bulk_cache import BulkDataCache
from app.services.jquants_client import JQuantsClient


class StockAnalysisService:
    def __init__(self, client: JQuantsClient, bulk_cache: BulkDataCache) -> None:
        self.client = client
        self.bulk_cache = bulk_cache
        self._master_cache: tuple[str, pd.DataFrame] | None = None

    def analyze(self, user_code: str) -> dict[str, Any]:
        code = normalize_stock_code(user_code)

        company_rows = self.client.fetch_equity_master(code=code)
        if not company_rows:
            raise LookupError("指定した銘柄コードの情報が見つかりませんでした。")

        company = company_rows[0]
        financial_df = self._load_financials(code)
        if financial_df.empty:
            raise LookupError("財務情報が取得できませんでした。")

        daily_df = self._load_daily_bars(code)
        if daily_df.empty:
            raise LookupError("株価データが取得できませんでした。")

        sector_averages = self._load_sector_averages(company["S33"])
        latest_financial = financial_df.iloc[-1]
        latest_daily = daily_df.iloc[-1]
        weekly_df = build_weekly_dataframe(daily_df, financial_df)

        latest_close = to_optional_float(latest_daily.get("C"))
        latest_market_cap = None
        if latest_close is not None and to_optional_float(latest_financial.get("ShOutFY")) is not None:
            latest_market_cap = latest_close * float(latest_financial["ShOutFY"])

        equity_ratio = to_optional_float(compute_equity_ratio(latest_financial))
        roe = self._compute_roe(latest_financial)
        roa = self._compute_roa(latest_financial)
        peg = self._compute_peg(latest_financial, latest_close)

        valuation_chart = self._build_valuation_chart(financial_df, daily_df)
        profit_chart = self._build_profit_chart(financial_df)
        yoy_chart = self._build_yoy_chart(financial_df)
        weekly_price_chart = self._build_weekly_price_chart(weekly_df)
        weekly_market_cap_chart = self._build_weekly_market_cap_chart(weekly_df)
        weekly_volume_chart = self._build_weekly_volume_chart(weekly_df)

        notes: list[str] = []
        if financial_df["OdP"].isna().all():
            notes.append("IFRS 採用企業では経常利益が空欄になるため、該当系列は欠損する場合があります。")
        if sector_averages["psr"] is None or sector_averages["per"] is None:
            notes.append("業種平均 PSR / PER は同業種の開示状況によって算出できない場合があります。")
        notes.append("業種平均は J-Quants の 33 業種（S33）単位で集計しています。")

        return {
            "company": {
                "code": display_stock_code(code),
                "apiCode": code,
                "name": company.get("CoName"),
                "nameEn": company.get("CoNameEn"),
                "market": company.get("MktNm"),
                "scaleCategory": company.get("ScaleCat"),
                "industry17": company.get("S17Nm"),
                "industry33": company.get("S33Nm"),
                "lastDisclosureDate": self._format_date(latest_financial.get("DiscDate")),
                "latestPriceDate": self._format_date(latest_daily.get("Date")),
            },
            "metrics": {
                "industry": company.get("S33Nm"),
                "selfCapitalRatio": equity_ratio,
                "roe": roe,
                "roa": roa,
                "peg": peg,
                "industryAvgPSR": sector_averages["psr"],
                "industryAvgPER": sector_averages["per"],
                "industryPeerCount": sector_averages["peer_count"],
                "industryPSRCount": sector_averages["psr_count"],
                "industryPERCount": sector_averages["per_count"],
                "latestClose": latest_close,
                "latestMarketCap": to_optional_float(latest_market_cap),
            },
            "charts": {
                "valuation": valuation_chart,
                "profits": profit_chart,
                "yoy": yoy_chart,
                "weeklyPrice": weekly_price_chart,
                "weeklyMarketCap": weekly_market_cap_chart,
                "weeklyVolume": weekly_volume_chart,
            },
            "notes": notes,
        }

    def _load_financials(self, code: str) -> pd.DataFrame:
        rows = self.client.fetch_fins_summary(code)
        return enrich_financial_dataframe(prepare_financial_dataframe(rows))

    def _load_daily_bars(self, code: str) -> pd.DataFrame:
        start_date = "20200101"
        end_date = date.today().strftime("%Y%m%d")
        rows = self.client.fetch_daily_bars(code=code, from_date=start_date, to_date=end_date)
        return prepare_daily_bar_dataframe(rows)

    def _get_master_dataframe(self) -> pd.DataFrame:
        today = date.today().isoformat()
        if self._master_cache and self._master_cache[0] == today:
            return self._master_cache[1]

        rows = self.client.fetch_equity_master()
        frame = pd.DataFrame(rows)
        if frame.empty:
            frame = pd.DataFrame(columns=["Code", "S33", "S33Nm"])
        else:
            frame["Code"] = frame["Code"].astype(str).str.zfill(5)

        self._master_cache = (today, frame)
        return frame

    def _load_sector_averages(self, sector_code: str) -> dict[str, float | int | None]:
        master_df = self._get_master_dataframe()
        sector_codes = master_df.loc[master_df["S33"] == sector_code, "Code"].astype(str).tolist()
        if not sector_codes:
            return {"psr": None, "per": None, "peer_count": 0, "psr_count": 0, "per_count": 0}
        return self.bulk_cache.compute_sector_averages(sector_codes)

    def _compute_roe(self, latest_financial: pd.Series) -> float | None:
        average_equity = average_pair(latest_financial.get("Eq"), latest_financial.get("PrevSameEq"))
        ttm_np = latest_financial.get("TTM_NP")
        if pd.isna(average_equity) or average_equity == 0 or pd.isna(ttm_np):
            return None
        return to_optional_float((float(ttm_np) / average_equity) * 100.0)

    def _compute_roa(self, latest_financial: pd.Series) -> float | None:
        average_assets = average_pair(latest_financial.get("TA"), latest_financial.get("PrevSameTA"))
        ttm_np = latest_financial.get("TTM_NP")
        if pd.isna(average_assets) or average_assets == 0 or pd.isna(ttm_np):
            return None
        return to_optional_float((float(ttm_np) / average_assets) * 100.0)

    def _compute_peg(self, latest_financial: pd.Series, latest_close: float | None) -> float | None:
        forecast_eps = to_optional_float(latest_financial.get("FEPS"))
        previous_fy_eps = to_optional_float(latest_financial.get("PrevFYEPS"))
        if latest_close is None or forecast_eps is None or forecast_eps <= 0:
            return None
        if previous_fy_eps is None or previous_fy_eps <= 0:
            return None

        growth_rate = ((forecast_eps / previous_fy_eps) - 1.0) * 100.0
        if growth_rate <= 0:
            return None

        forward_per = latest_close / forecast_eps
        return to_optional_float(forward_per / growth_rate)

    def _build_valuation_chart(self, financial_df: pd.DataFrame, daily_df: pd.DataFrame) -> dict[str, Any]:
        labels: list[str] = []
        psr_values: list[float] = []
        per_values: list[float] = []
        pbr_values: list[float] = []

        for _, row in financial_df.iterrows():
            disclosure_date = row.get("DiscDate")
            price = lookup_close_on_or_before(daily_df, disclosure_date, column="C")
            shares = row.get("ShOutFY")
            market_cap = price * shares if not pd.isna(price) and not pd.isna(shares) else np.nan

            psr = np.nan
            per = np.nan
            pbr = np.nan

            if not pd.isna(market_cap) and row.get("TTM_Sales") is not None and row["TTM_Sales"] > 0:
                psr = market_cap / row["TTM_Sales"]
            if not pd.isna(market_cap) and row.get("TTM_NP") is not None and row["TTM_NP"] > 0:
                per = market_cap / row["TTM_NP"]
            if not pd.isna(market_cap) and row.get("Eq") is not None and row["Eq"] > 0:
                pbr = market_cap / row["Eq"]

            labels.append(self._format_date(disclosure_date) or "")
            psr_values.append(psr)
            per_values.append(per)
            pbr_values.append(pbr)

        return {
            "labels": labels,
            "series": {
                "psr": clean_series(psr_values),
                "per": clean_series(per_values),
                "pbr": clean_series(pbr_values),
            },
        }

    def _build_profit_chart(self, financial_df: pd.DataFrame) -> dict[str, Any]:
        return {
            "labels": financial_df["Label"].tolist(),
            "series": {
                "sales": clean_series(financial_df["Sales"].tolist()),
                "op": clean_series(financial_df["OP"].tolist()),
                "odp": clean_series(financial_df["OdP"].tolist()),
                "np": clean_series(financial_df["NP"].tolist()),
            },
        }

    def _build_yoy_chart(self, financial_df: pd.DataFrame) -> dict[str, Any]:
        return {
            "labels": financial_df["Label"].tolist(),
            "series": {
                "sales": clean_series(financial_df["YoY_Sales"].tolist()),
                "op": clean_series(financial_df["YoY_OP"].tolist()),
                "odp": clean_series(financial_df["YoY_OdP"].tolist()),
                "np": clean_series(financial_df["YoY_NP"].tolist()),
            },
        }

    def _build_weekly_price_chart(self, weekly_df: pd.DataFrame) -> dict[str, Any]:
        return {
            "labels": [self._format_date(value) or "" for value in weekly_df["Date"].tolist()],
            "series": {
                "close": clean_series(weekly_df["AdjC"].tolist()),
                "ma25": clean_series(weekly_df["MA25"].tolist()),
                "ma50": clean_series(weekly_df["MA50"].tolist()),
            },
        }

    def _build_weekly_market_cap_chart(self, weekly_df: pd.DataFrame) -> dict[str, Any]:
        return {
            "labels": [self._format_date(value) or "" for value in weekly_df["Date"].tolist()],
            "series": {
                "marketCap": clean_series(weekly_df["MarketCap"].tolist()),
            },
        }

    def _build_weekly_volume_chart(self, weekly_df: pd.DataFrame) -> dict[str, Any]:
        return {
            "labels": [self._format_date(value) or "" for value in weekly_df["Date"].tolist()],
            "series": {
                "volume": [to_optional_int(value) for value in weekly_df["Vo"].tolist()],
            },
        }

    def _format_date(self, value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None
        return pd.Timestamp(value).strftime("%Y-%m-%d")
