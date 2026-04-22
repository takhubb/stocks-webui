from __future__ import annotations

import re
import unicodedata
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
    normalize_stock_code_series,
    normalize_stock_code_text,
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

        sector_context = self._build_sector_context(
            sector_code=str(company.get("S33") or ""),
            financial_df=financial_df,
        )
        latest_sector_metrics = sector_context["latest"]

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

        analysis_start = min(pd.Timestamp(financial_df["DiscDate"].min()), pd.Timestamp(daily_df["Date"].min()))
        analysis_end = max(pd.Timestamp(financial_df["DiscDate"].max()), pd.Timestamp(daily_df["Date"].max()))
        topix_df = self._load_topix_dataframe(start_date=analysis_start, end_date=analysis_end)

        valuation_chart = self._build_valuation_chart(
            financial_df=financial_df,
            daily_df=daily_df,
            sector_history=sector_context["history"],
            topix_df=topix_df,
        )
        efficiency_chart = self._build_efficiency_chart(
            financial_df=financial_df,
            sector_history=sector_context["history"],
        )
        quarterly_yoy_chart = self._build_quarterly_yoy_chart(financial_df)
        year_end_chart = self._build_year_end_results_chart(financial_df)
        year_end_yoy_chart = self._build_year_end_yoy_chart(financial_df)
        weekly_price_chart = self._build_weekly_price_chart(weekly_df, topix_df)
        weekly_market_cap_chart = self._build_weekly_market_cap_chart(weekly_df)
        weekly_volume_chart = self._build_weekly_volume_chart(weekly_df)

        notes: list[str] = []
        if financial_df["OdP"].isna().all():
            notes.append("IFRS 採用企業では経常利益が空欄になるため、該当系列は欠損する場合があります。")
        if latest_sector_metrics["psr"] is None or latest_sector_metrics["per"] is None:
            notes.append("業種平均 PSR / PER は同業種の開示状況によって算出できない場合があります。")
        if topix_df.empty:
            notes.append("TOPIX データが取得できなかったため、比較線は非表示になります。")
        else:
            notes.append("TOPIX は最初の表示時点を 100 とした指数化系列を右軸に表示しています。")
        notes.append("年度末の業績と前年比は、全財務タイムラインの FY 位置にそろえて表示しています。")
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
                "industryAvgPSR": latest_sector_metrics["psr"],
                "industryAvgPER": latest_sector_metrics["per"],
                "industryPeerCount": latest_sector_metrics["peer_count"],
                "industryPSRCount": latest_sector_metrics["psr_count"],
                "industryPERCount": latest_sector_metrics["per_count"],
                "latestClose": latest_close,
                "latestMarketCap": to_optional_float(latest_market_cap),
            },
            "charts": {
                "valuation": valuation_chart,
                "efficiency": efficiency_chart,
                "quarterlyYoy": quarterly_yoy_chart,
                "yearEndResults": year_end_chart,
                "yearEndYoy": year_end_yoy_chart,
                "weeklyPrice": weekly_price_chart,
                "weeklyMarketCap": weekly_market_cap_chart,
                "weeklyVolume": weekly_volume_chart,
            },
            "notes": notes,
        }

    def search_companies(self, query: str, limit: int = 10) -> list[dict[str, str | None]]:
        raw_term = unicodedata.normalize("NFKC", str(query or "")).strip()
        compact_term = self._normalize_search_text(raw_term)
        if not compact_term:
            return []

        master_df = self._get_master_dataframe().copy()
        if master_df.empty:
            return []

        for column in ("Code", "CoName", "CoNameEn", "MktNm", "S33Nm"):
            if column not in master_df.columns:
                master_df[column] = ""

        master_df["Code"] = normalize_stock_code_series(master_df["Code"])
        master_df["DisplayCode"] = master_df["Code"].map(display_stock_code)
        master_df["CoName"] = master_df["CoName"].fillna("").astype(str)
        master_df["CoNameEn"] = master_df["CoNameEn"].fillna("").astype(str)
        master_df["MktNm"] = master_df["MktNm"].fillna("").astype(str)
        master_df["S33Nm"] = master_df["S33Nm"].fillna("").astype(str)

        company_name = self._normalize_search_series(master_df["CoName"])
        company_name_en = self._normalize_search_series(master_df["CoNameEn"]).str.casefold()
        market_name = self._normalize_search_series(master_df["MktNm"]).str.casefold()
        industry_name = self._normalize_search_series(master_df["S33Nm"]).str.casefold()
        display_code = master_df["DisplayCode"]
        code = master_df["Code"]
        display_code_search = display_code.astype(str).str.casefold()
        code_search = code.astype(str).str.casefold()

        tokens = [self._normalize_search_text(token) for token in raw_term.split()]
        tokens = [token for token in tokens if token]
        query_casefold = compact_term.casefold()
        code_query = normalize_stock_code_text(raw_term)
        probable_code_query = bool(code_query) and any(char.isdigit() for char in code_query)
        canonical_code_query = ""
        if probable_code_query and re.fullmatch(r"[0-9A-Z]{4,5}", code_query):
            canonical_code_query = code_query if len(code_query) == 5 else f"{code_query}0"

        match_mask = pd.Series(True, index=master_df.index)
        for token in tokens or [compact_term]:
            token_casefold = token.casefold()
            token_mask = (
                display_code_search.str.contains(token_casefold, regex=False)
                | code_search.str.contains(token_casefold, regex=False)
                | company_name.str.contains(token, regex=False)
                | company_name_en.str.contains(token_casefold, regex=False)
                | market_name.str.contains(token_casefold, regex=False)
                | industry_name.str.contains(token_casefold, regex=False)
            )
            match_mask &= token_mask

        matched = master_df.loc[match_mask].copy()
        if matched.empty:
            return []

        false_mask = pd.Series(False, index=master_df.index)
        exact_display_code = false_mask.copy()
        exact_api_code = false_mask.copy()
        prefix_display_code = false_mask.copy()
        prefix_api_code = false_mask.copy()
        if probable_code_query:
            query_code_casefold = code_query.casefold()
            exact_display_code = display_code_search.eq(query_code_casefold)
            prefix_display_code = display_code_search.str.startswith(query_code_casefold)
            exact_api_code = code_search.eq(query_code_casefold)
            prefix_api_code = code_search.str.startswith(query_code_casefold)
            if canonical_code_query:
                canonical_casefold = canonical_code_query.casefold()
                exact_api_code = exact_api_code | code_search.eq(canonical_casefold)
                prefix_api_code = prefix_api_code | code_search.str.startswith(canonical_casefold)

        exact_name = company_name.eq(compact_term) | company_name_en.eq(query_casefold)
        prefix_name = company_name.str.startswith(compact_term) | company_name_en.str.startswith(query_casefold)
        contains_name = company_name.str.contains(compact_term, regex=False) | company_name_en.str.contains(
            query_casefold,
            regex=False,
        )
        contains_market = market_name.str.contains(query_casefold, regex=False)
        contains_industry = industry_name.str.contains(query_casefold, regex=False)

        matched["exact_display_code_rank"] = exact_display_code.loc[matched.index].astype(int)
        matched["exact_api_code_rank"] = exact_api_code.loc[matched.index].astype(int)
        matched["prefix_display_code_rank"] = prefix_display_code.loc[matched.index].astype(int)
        matched["prefix_api_code_rank"] = prefix_api_code.loc[matched.index].astype(int)
        matched["exact_name_rank"] = exact_name.loc[matched.index].astype(int)
        matched["prefix_name_rank"] = prefix_name.loc[matched.index].astype(int)
        matched["contains_name_rank"] = contains_name.loc[matched.index].astype(int)
        matched["contains_market_rank"] = contains_market.loc[matched.index].astype(int)
        matched["contains_industry_rank"] = contains_industry.loc[matched.index].astype(int)

        matched = matched.sort_values(
            [
                "exact_display_code_rank",
                "exact_api_code_rank",
                "exact_name_rank",
                "prefix_display_code_rank",
                "prefix_api_code_rank",
                "prefix_name_rank",
                "contains_name_rank",
                "contains_market_rank",
                "contains_industry_rank",
                "DisplayCode",
            ],
            ascending=[False, False, False, False, False, False, False, False, False, True],
            kind="stable",
        )

        suggestions: list[dict[str, str | None]] = []
        for _, row in matched.head(limit).iterrows():
            suggestions.append(
                {
                    "code": display_stock_code(str(row.get("Code") or "")),
                    "apiCode": str(row.get("Code") or "") or None,
                    "name": row.get("CoName") or None,
                    "nameEn": row.get("CoNameEn") or None,
                    "market": row.get("MktNm") or None,
                    "industry": row.get("S33Nm") or None,
                }
            )
        return suggestions

    def _load_financials(self, code: str) -> pd.DataFrame:
        rows = self.client.fetch_fins_summary(code)
        return enrich_financial_dataframe(prepare_financial_dataframe(rows))

    def _load_daily_bars(self, code: str) -> pd.DataFrame:
        start_date = "20200101"
        end_date = date.today().strftime("%Y%m%d")
        rows = self.client.fetch_daily_bars(code=code, from_date=start_date, to_date=end_date)
        return prepare_daily_bar_dataframe(rows)

    def _load_topix_dataframe(
        self,
        start_date: pd.Timestamp | Any,
        end_date: pd.Timestamp | Any,
    ) -> pd.DataFrame:
        if pd.isna(start_date) or pd.isna(end_date):
            return pd.DataFrame(columns=["Date", "C"])

        try:
            rows = self.client.fetch_topix_bars(
                from_date=pd.Timestamp(start_date).strftime("%Y%m%d"),
                to_date=pd.Timestamp(end_date).strftime("%Y%m%d"),
            )
        except Exception:
            return pd.DataFrame(columns=["Date", "C"])

        frame = pd.DataFrame(rows)
        if frame.empty:
            return pd.DataFrame(columns=["Date", "C"])

        frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
        frame["C"] = pd.to_numeric(frame["C"], errors="coerce")
        return frame.sort_values("Date", kind="stable").reset_index(drop=True)

    def _get_master_dataframe(self) -> pd.DataFrame:
        today = date.today().isoformat()
        if self._master_cache and self._master_cache[0] == today:
            return self._master_cache[1]

        rows = self.client.fetch_equity_master()
        frame = pd.DataFrame(rows)
        if frame.empty:
            frame = pd.DataFrame(columns=["Code", "CoName", "CoNameEn", "MktNm", "S33", "S33Nm"])
        else:
            frame["Code"] = normalize_stock_code_series(frame["Code"])

        self._master_cache = (today, frame)
        return frame

    @staticmethod
    def _normalize_search_text(value: Any) -> str:
        text = unicodedata.normalize("NFKC", str(value or ""))
        return re.sub(r"\s+", "", text)

    def _normalize_search_series(self, series: pd.Series) -> pd.Series:
        return (
            series.fillna("")
            .astype("string")
            .str.normalize("NFKC")
            .str.replace(r"\s+", "", regex=True)
        )

    def _load_sector_codes(self, sector_code: str) -> list[str]:
        if not sector_code:
            return []
        master_df = self._get_master_dataframe()
        return master_df.loc[master_df["S33"] == sector_code, "Code"].astype(str).tolist()

    def _build_sector_context(self, sector_code: str, financial_df: pd.DataFrame) -> dict[str, Any]:
        empty_history = {
            "psr": [None] * len(financial_df),
            "per": [None] * len(financial_df),
            "pbr": [None] * len(financial_df),
            "roe": [None] * len(financial_df),
            "roa": [None] * len(financial_df),
        }
        empty_latest = {
            "psr": None,
            "per": None,
            "pbr": None,
            "roe": None,
            "roa": None,
            "peer_count": 0,
            "psr_count": 0,
            "per_count": 0,
            "pbr_count": 0,
            "roe_count": 0,
            "roa_count": 0,
        }

        sector_codes = self._load_sector_codes(sector_code)
        if not sector_codes:
            return {"history": empty_history, "latest": empty_latest}

        summary_frame = self.bulk_cache.load_summary_frame(set(sector_codes))
        sector_financial_df = enrich_financial_dataframe(
            prepare_financial_dataframe(summary_frame.to_dict(orient="records"))
        )
        if sector_financial_df.empty:
            return {"history": empty_history, "latest": empty_latest}

        price_snapshots = self.bulk_cache.load_price_snapshots(
            sector_codes=set(sector_codes),
            target_dates=financial_df["DiscDate"].tolist(),
        )

        return {
            "history": self._compute_sector_timeline_averages(
                company_financial_df=financial_df,
                sector_financial_df=sector_financial_df,
                price_snapshots=price_snapshots,
            ),
            "latest": self._compute_sector_latest_averages(
                sector_financial_df=sector_financial_df,
                sector_codes=set(sector_codes),
            ),
        }

    def _compute_sector_latest_averages(
        self,
        sector_financial_df: pd.DataFrame,
        sector_codes: set[str],
    ) -> dict[str, float | int | None]:
        if sector_financial_df.empty:
            return {
                "psr": None,
                "per": None,
                "pbr": None,
                "roe": None,
                "roa": None,
                "peer_count": 0,
                "psr_count": 0,
                "per_count": 0,
                "pbr_count": 0,
                "roe_count": 0,
                "roa_count": 0,
            }

        latest_financials = (
            sector_financial_df.sort_values(["Code", "CurPerEn", "DiscDate", "DiscTime"], kind="stable")
            .groupby("Code", sort=False)
            .tail(1)
            .copy()
        )
        latest_financials["ROE"] = latest_financials.apply(self._compute_roe, axis=1)
        latest_financials["ROA"] = latest_financials.apply(self._compute_roa, axis=1)

        latest_prices = self.bulk_cache.load_latest_prices(sector_codes)
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
        latest_financials["PBR"] = np.where(
            (latest_financials["MarketCap"] > 0) & (latest_financials["Eq"] > 0),
            latest_financials["MarketCap"] / latest_financials["Eq"],
            np.nan,
        )

        return {
            "psr": self._mean_or_none(latest_financials["PSR"]),
            "per": self._mean_or_none(latest_financials["PER"]),
            "pbr": self._mean_or_none(latest_financials["PBR"]),
            "roe": self._mean_or_none(latest_financials["ROE"]),
            "roa": self._mean_or_none(latest_financials["ROA"]),
            "peer_count": int(latest_financials["Code"].nunique()),
            "psr_count": int(pd.to_numeric(latest_financials["PSR"], errors="coerce").notna().sum()),
            "per_count": int(pd.to_numeric(latest_financials["PER"], errors="coerce").notna().sum()),
            "pbr_count": int(pd.to_numeric(latest_financials["PBR"], errors="coerce").notna().sum()),
            "roe_count": int(pd.to_numeric(latest_financials["ROE"], errors="coerce").notna().sum()),
            "roa_count": int(pd.to_numeric(latest_financials["ROA"], errors="coerce").notna().sum()),
        }

    def _compute_sector_timeline_averages(
        self,
        company_financial_df: pd.DataFrame,
        sector_financial_df: pd.DataFrame,
        price_snapshots: pd.DataFrame,
    ) -> dict[str, list[float | None]]:
        series: dict[str, list[float | None]] = {
            "psr": [],
            "per": [],
            "pbr": [],
            "roe": [],
            "roa": [],
        }

        if sector_financial_df.empty:
            return {key: [None] * len(company_financial_df) for key in series}

        sector_financial_df = sector_financial_df.copy()
        sector_financial_df["ROE"] = sector_financial_df.apply(self._compute_roe, axis=1)
        sector_financial_df["ROA"] = sector_financial_df.apply(self._compute_roa, axis=1)

        for _, row in company_financial_df.iterrows():
            target_date = pd.to_datetime(row.get("DiscDate"), errors="coerce")
            label = row.get("Label")
            if pd.isna(target_date) or not label:
                for key in series:
                    series[key].append(None)
                continue

            disclosed_peers = sector_financial_df[
                (sector_financial_df["Label"] == label) & (sector_financial_df["DiscDate"] <= target_date)
            ].copy()
            if disclosed_peers.empty:
                for key in series:
                    series[key].append(None)
                continue

            disclosed_peers = (
                disclosed_peers.sort_values(["Code", "DiscDate", "DiscTime"], kind="stable")
                .groupby("Code", sort=False)
                .tail(1)
                .copy()
            )

            series["roe"].append(self._mean_or_none(disclosed_peers["ROE"]))
            series["roa"].append(self._mean_or_none(disclosed_peers["ROA"]))

            snapshot = price_snapshots[
                price_snapshots["TargetDate"] == pd.Timestamp(target_date).normalize()
            ][["Code", "C"]]
            if snapshot.empty:
                series["psr"].append(None)
                series["per"].append(None)
                series["pbr"].append(None)
                continue

            valuation_frame = disclosed_peers.merge(snapshot, on="Code", how="left")
            valuation_frame["MarketCap"] = valuation_frame["C"] * valuation_frame["ShOutFY"]
            valuation_frame["PSR"] = np.where(
                (valuation_frame["MarketCap"] > 0) & (valuation_frame["TTM_Sales"] > 0),
                valuation_frame["MarketCap"] / valuation_frame["TTM_Sales"],
                np.nan,
            )
            valuation_frame["PER"] = np.where(
                (valuation_frame["MarketCap"] > 0) & (valuation_frame["TTM_NP"] > 0),
                valuation_frame["MarketCap"] / valuation_frame["TTM_NP"],
                np.nan,
            )
            valuation_frame["PBR"] = np.where(
                (valuation_frame["MarketCap"] > 0) & (valuation_frame["Eq"] > 0),
                valuation_frame["MarketCap"] / valuation_frame["Eq"],
                np.nan,
            )

            series["psr"].append(self._mean_or_none(valuation_frame["PSR"]))
            series["per"].append(self._mean_or_none(valuation_frame["PER"]))
            series["pbr"].append(self._mean_or_none(valuation_frame["PBR"]))

        return series

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

    def _build_valuation_chart(
        self,
        financial_df: pd.DataFrame,
        daily_df: pd.DataFrame,
        sector_history: dict[str, list[float | None]],
        topix_df: pd.DataFrame,
    ) -> dict[str, Any]:
        labels: list[str] = financial_df["Label"].tolist()
        psr_values: list[float] = []
        per_values: list[float] = []
        pbr_values: list[float] = []
        topix_reference = self._build_topix_reference_series(financial_df["DiscDate"].tolist(), topix_df)

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

            psr_values.append(psr)
            per_values.append(per)
            pbr_values.append(pbr)

        return {
            "labels": labels,
            "series": {
                "psr": clean_series(psr_values),
                "psrIndustry": sector_history["psr"],
                "per": clean_series(per_values),
                "perIndustry": sector_history["per"],
                "pbr": clean_series(pbr_values),
                "pbrIndustry": sector_history["pbr"],
                "topix": topix_reference,
            },
        }

    def _build_efficiency_chart(
        self,
        financial_df: pd.DataFrame,
        sector_history: dict[str, list[float | None]],
    ) -> dict[str, Any]:
        return {
            "labels": financial_df["Label"].tolist(),
            "series": {
                "roe": [self._compute_roe(row) for _, row in financial_df.iterrows()],
                "roeIndustry": sector_history["roe"],
                "roa": [self._compute_roa(row) for _, row in financial_df.iterrows()],
                "roaIndustry": sector_history["roa"],
            },
        }

    def _build_quarterly_yoy_chart(self, financial_df: pd.DataFrame) -> dict[str, Any]:
        return {
            "labels": financial_df["Label"].tolist(),
            "series": {
                "sales": clean_series(financial_df["YoY_Sales"].tolist()),
                "op": clean_series(financial_df["YoY_OP"].tolist()),
                "odp": clean_series(financial_df["YoY_OdP"].tolist()),
                "np": clean_series(financial_df["YoY_NP"].tolist()),
            },
        }

    def _build_year_end_results_chart(self, financial_df: pd.DataFrame) -> dict[str, Any]:
        return {
            "labels": financial_df["Label"].tolist(),
            "series": {
                "sales": self._year_end_series(financial_df, "Sales"),
                "op": self._year_end_series(financial_df, "OP"),
                "odp": self._year_end_series(financial_df, "OdP"),
                "np": self._year_end_series(financial_df, "NP"),
            },
        }

    def _build_year_end_yoy_chart(self, financial_df: pd.DataFrame) -> dict[str, Any]:
        return {
            "labels": financial_df["Label"].tolist(),
            "series": {
                "sales": self._year_end_series(financial_df, "YoY_Sales"),
                "op": self._year_end_series(financial_df, "YoY_OP"),
                "odp": self._year_end_series(financial_df, "YoY_OdP"),
                "np": self._year_end_series(financial_df, "YoY_NP"),
            },
        }

    def _build_weekly_price_chart(self, weekly_df: pd.DataFrame, topix_df: pd.DataFrame) -> dict[str, Any]:
        weekly_dates = weekly_df["Date"].tolist()
        return {
            "labels": [self._format_date(value) or "" for value in weekly_dates],
            "series": {
                "close": clean_series(weekly_df["AdjC"].tolist()),
                "ma25": clean_series(weekly_df["MA25"].tolist()),
                "ma50": clean_series(weekly_df["MA50"].tolist()),
                "topix": self._build_topix_reference_series(weekly_dates, topix_df),
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

    def _build_topix_reference_series(
        self,
        date_values: list[Any],
        topix_df: pd.DataFrame,
    ) -> list[float | None]:
        if topix_df.empty:
            return [None] * len(date_values)

        raw_values = [lookup_close_on_or_before(topix_df, value, column="C") for value in date_values]
        normalized: list[float | None] = []
        base_value = next(
            (
                float(value)
                for value in raw_values
                if value is not None and not pd.isna(value) and float(value) > 0
            ),
            None,
        )
        if base_value is None:
            return [None] * len(date_values)

        for value in raw_values:
            if value is None or pd.isna(value):
                normalized.append(None)
                continue
            normalized.append(to_optional_float((float(value) / base_value) * 100.0))
        return normalized

    def _year_end_series(self, financial_df: pd.DataFrame, column: str) -> list[float | None]:
        values: list[float | None] = []
        for _, row in financial_df.iterrows():
            if row.get("CurPerType") != "FY":
                values.append(None)
                continue
            values.append(to_optional_float(row.get(column)))
        return values

    def _mean_or_none(self, values: pd.Series) -> float | None:
        numeric = pd.to_numeric(values, errors="coerce")
        return to_optional_float(numeric.mean(skipna=True))

    def _format_date(self, value: Any) -> str | None:
        if value is None or pd.isna(value):
            return None
        timestamp = pd.Timestamp(value)
        return timestamp.strftime("%Y-%m-%d")
