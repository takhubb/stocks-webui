from __future__ import annotations

import math
import re
from typing import Any

import numpy as np
import pandas as pd

PERIOD_TYPES = ("1Q", "2Q", "3Q", "FY")
PERIOD_ORDER = {"1Q": 1, "2Q": 2, "3Q": 3, "FY": 4}
FINANCIAL_NUMERIC_COLUMNS = [
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


def normalize_stock_code(raw_code: str) -> str:
    digits = re.sub(r"\D", "", raw_code or "")
    if len(digits) == 4:
        return f"{digits}0"
    if len(digits) == 5:
        return digits
    raise ValueError("銘柄コードは4桁または5桁で入力してください。")


def display_stock_code(code: str) -> str:
    return code[:4] if len(code) == 5 and code.endswith("0") else code


def to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def to_optional_int(value: Any) -> int | None:
    number = to_optional_float(value)
    if number is None:
        return None
    return int(number)


def make_label(period_end: pd.Timestamp, period_type: str, fiscal_year_end: pd.Timestamp) -> str:
    fiscal_year = fiscal_year_end.year if pd.notna(fiscal_year_end) else period_end.year
    suffix = "FY" if period_type == "FY" else period_type
    return f"{fiscal_year} {suffix}"


def calculate_yoy(current: float | int | None, previous: float | int | None) -> float:
    if current is None or previous is None:
        return np.nan
    if pd.isna(current) or pd.isna(previous) or previous == 0:
        return np.nan
    return ((current / previous) - 1.0) * 100.0


def calculate_ttm(
    current_value: float | int | None,
    period_type: str,
    previous_fy_value: float | int | None,
    previous_same_period_value: float | int | None,
) -> float:
    if current_value is None or pd.isna(current_value):
        return np.nan
    if period_type == "FY":
        return float(current_value)
    if (
        previous_fy_value is None
        or previous_same_period_value is None
        or pd.isna(previous_fy_value)
        or pd.isna(previous_same_period_value)
    ):
        return np.nan
    return float(current_value) + float(previous_fy_value) - float(previous_same_period_value)


def average_pair(primary: float | int | None, secondary: float | int | None) -> float:
    values = [value for value in (primary, secondary) if value is not None and not pd.isna(value)]
    if not values:
        return np.nan
    if len(values) == 1:
        return float(values[0])
    return float(sum(values) / len(values))


def prepare_financial_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["Code"] = df["Code"].astype(str).str.zfill(5)
    df["DocType"] = df["DocType"].fillna("").astype(str)
    df["CurPerType"] = df["CurPerType"].fillna("").astype(str)
    df = df[df["DocType"].str.contains("FinancialStatements", na=False)]
    df = df[df["CurPerType"].isin(PERIOD_TYPES)]

    for column in ("DiscDate", "CurPerEn", "CurFYEn", "NxtFYEn"):
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")

    for column in FINANCIAL_NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df["PeriodOrder"] = df["CurPerType"].map(PERIOD_ORDER)
    df = df.sort_values(
        ["Code", "CurPerEn", "PeriodOrder", "DiscDate", "DiscTime", "DiscNo"],
        kind="stable",
    )
    df = df.drop_duplicates(subset=["Code", "CurPerType", "CurPerEn"], keep="last")
    df["Label"] = df.apply(
        lambda row: make_label(row["CurPerEn"], row["CurPerType"], row["CurFYEn"]),
        axis=1,
    )

    return df.reset_index(drop=True)


def enrich_financial_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    enriched_groups: list[pd.DataFrame] = []

    for _, group in df.groupby("Code", sort=False):
        group = group.sort_values(
            ["CurPerEn", "PeriodOrder", "DiscDate", "DiscTime", "DiscNo"],
            kind="stable",
        ).copy()

        ttm_sales: list[float] = []
        ttm_op: list[float] = []
        ttm_odp: list[float] = []
        ttm_np: list[float] = []

        yoy_sales: list[float] = []
        yoy_op: list[float] = []
        yoy_odp: list[float] = []
        yoy_np: list[float] = []

        prev_same_eq: list[float] = []
        prev_same_ta: list[float] = []
        prev_fy_eps: list[float] = []

        for _, row in group.iterrows():
            previous_fy = group[(group["CurPerType"] == "FY") & (group["CurPerEn"] < row["CurPerEn"])].tail(1)
            previous_same = group[
                (group["CurPerType"] == row["CurPerType"]) & (group["CurPerEn"] < row["CurPerEn"])
            ].tail(1)

            previous_fy_row = previous_fy.iloc[0] if not previous_fy.empty else None
            previous_same_row = previous_same.iloc[0] if not previous_same.empty else None

            ttm_sales.append(
                calculate_ttm(
                    row.get("Sales"),
                    row["CurPerType"],
                    None if previous_fy_row is None else previous_fy_row.get("Sales"),
                    None if previous_same_row is None else previous_same_row.get("Sales"),
                )
            )
            ttm_op.append(
                calculate_ttm(
                    row.get("OP"),
                    row["CurPerType"],
                    None if previous_fy_row is None else previous_fy_row.get("OP"),
                    None if previous_same_row is None else previous_same_row.get("OP"),
                )
            )
            ttm_odp.append(
                calculate_ttm(
                    row.get("OdP"),
                    row["CurPerType"],
                    None if previous_fy_row is None else previous_fy_row.get("OdP"),
                    None if previous_same_row is None else previous_same_row.get("OdP"),
                )
            )
            ttm_np.append(
                calculate_ttm(
                    row.get("NP"),
                    row["CurPerType"],
                    None if previous_fy_row is None else previous_fy_row.get("NP"),
                    None if previous_same_row is None else previous_same_row.get("NP"),
                )
            )

            yoy_sales.append(
                calculate_yoy(row.get("Sales"), None if previous_same_row is None else previous_same_row.get("Sales"))
            )
            yoy_op.append(
                calculate_yoy(row.get("OP"), None if previous_same_row is None else previous_same_row.get("OP"))
            )
            yoy_odp.append(
                calculate_yoy(row.get("OdP"), None if previous_same_row is None else previous_same_row.get("OdP"))
            )
            yoy_np.append(
                calculate_yoy(row.get("NP"), None if previous_same_row is None else previous_same_row.get("NP"))
            )

            prev_same_eq.append(np.nan if previous_same_row is None else previous_same_row.get("Eq"))
            prev_same_ta.append(np.nan if previous_same_row is None else previous_same_row.get("TA"))
            prev_fy_eps.append(np.nan if previous_fy_row is None else previous_fy_row.get("EPS"))

        group["TTM_Sales"] = ttm_sales
        group["TTM_OP"] = ttm_op
        group["TTM_OdP"] = ttm_odp
        group["TTM_NP"] = ttm_np

        group["YoY_Sales"] = yoy_sales
        group["YoY_OP"] = yoy_op
        group["YoY_OdP"] = yoy_odp
        group["YoY_NP"] = yoy_np

        group["PrevSameEq"] = prev_same_eq
        group["PrevSameTA"] = prev_same_ta
        group["PrevFYEPS"] = prev_fy_eps

        enriched_groups.append(group)

    return pd.concat(enriched_groups, ignore_index=True)


def prepare_daily_bar_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["Code"] = df["Code"].astype(str).str.zfill(5)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    numeric_columns = ["O", "H", "L", "C", "Vo", "Va", "AdjFactor", "AdjO", "AdjH", "AdjL", "AdjC", "AdjVo"]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.sort_values(["Code", "Date"], kind="stable").reset_index(drop=True)
    return df


def lookup_close_on_or_before(daily_df: pd.DataFrame, target_date: pd.Timestamp, column: str = "C") -> float:
    if daily_df.empty or pd.isna(target_date):
        return np.nan

    date_index = daily_df["Date"]
    position = int(date_index.searchsorted(pd.Timestamp(target_date), side="right")) - 1
    if position < 0:
        return np.nan
    return float(daily_df.iloc[position][column])


def build_weekly_dataframe(daily_df: pd.DataFrame, financial_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        return pd.DataFrame()

    frame = daily_df.set_index("Date").sort_index()
    weekly = frame.resample("W-FRI").agg({"AdjC": "last", "C": "last", "Vo": "sum"})
    weekly = weekly.dropna(subset=["C"]).copy()
    weekly["MA25"] = weekly["AdjC"].rolling(25, min_periods=1).mean()
    weekly["MA50"] = weekly["AdjC"].rolling(50, min_periods=1).mean()

    shares = (
        financial_df[["DiscDate", "ShOutFY"]]
        .dropna()
        .drop_duplicates(subset=["DiscDate"], keep="last")
        .sort_values("DiscDate")
        .set_index("DiscDate")["ShOutFY"]
    )
    if not shares.empty:
        weekly["Shares"] = shares.reindex(weekly.index, method="ffill").ffill().bfill()
        weekly["MarketCap"] = weekly["C"] * weekly["Shares"]
    else:
        weekly["Shares"] = np.nan
        weekly["MarketCap"] = np.nan

    return weekly.reset_index()


def compute_equity_ratio(row: pd.Series) -> float:
    eq_ratio = row.get("EqAR")
    if eq_ratio is not None and not pd.isna(eq_ratio):
        return float(eq_ratio) * 100.0

    equity = row.get("Eq")
    total_assets = row.get("TA")
    if equity is None or total_assets is None or pd.isna(equity) or pd.isna(total_assets) or total_assets == 0:
        return np.nan
    return (float(equity) / float(total_assets)) * 100.0


def clean_series(values: list[Any]) -> list[float | None]:
    return [to_optional_float(value) for value in values]
