"""Ingest daily OHLCV data for major Tadawul-listed companies via yfinance.

Yahoo Finance exposes Tadawul tickers with an `.SR` suffix (e.g. 2222.SR for
Saudi Aramco), which gives us free daily market data without an API key.
"""

from datetime import date

import pandas as pd
import yfinance as yf
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, MetadataValue, asset

from orchestration.resources import DataLakeResource

# Ticker -> (company, sector). Sectors follow Tadawul's industry group names
# so the dbt marts can aggregate sector performance without a separate lookup.
TADAWUL_TICKERS: dict[str, tuple[str, str]] = {
    "2222.SR": ("Saudi Aramco", "Energy"),
    "2380.SR": ("Petro Rabigh", "Energy"),
    "2381.SR": ("Arabian Drilling", "Energy"),
    "2223.SR": ("Luberef", "Energy"),
    "4030.SR": ("Bahri", "Energy"),
    "1120.SR": ("Al Rajhi Bank", "Financials"),
    "1180.SR": ("Saudi National Bank", "Financials"),
    "1010.SR": ("Riyad Bank", "Financials"),
    "1150.SR": ("Alinma Bank", "Financials"),
    "1060.SR": ("Saudi Awwal Bank", "Financials"),
    "1111.SR": ("Saudi Tadawul Group", "Financials"),
    "8210.SR": ("Bupa Arabia", "Insurance"),
    "8010.SR": ("Tawuniya", "Insurance"),
    "2010.SR": ("SABIC", "Materials"),
    "1211.SR": ("Ma'aden", "Materials"),
    "2020.SR": ("SABIC Agri-Nutrients", "Materials"),
    "7010.SR": ("stc", "Telecom"),
    "7020.SR": ("Mobily", "Telecom"),
    "2082.SR": ("ACWA Power", "Utilities"),
    "5110.SR": ("Saudi Electricity", "Utilities"),
    "2280.SR": ("Almarai", "Consumer Staples"),
    "2050.SR": ("Savola", "Consumer Staples"),
    "4001.SR": ("Abdullah Al Othaim Markets", "Consumer Staples"),
    "4190.SR": ("Jarir Marketing", "Consumer Discretionary"),
    "6004.SR": ("Saudi Airlines Catering", "Consumer Discretionary"),
    "4013.SR": ("Dr Sulaiman Al Habib", "Health Care"),
    "4002.SR": ("Mouwasat Medical Services", "Health Care"),
    "4300.SR": ("Dar Al Arkan", "Real Estate"),
}


def tidy_ohlcv(raw: pd.DataFrame, tickers: dict[str, tuple[str, str]]) -> pd.DataFrame:
    """Reshape yfinance's wide (field, ticker) columns into one tidy long table.

    Returns columns: trade_date, ticker, company, sector, open, high, low,
    close, volume. Rows where the ticker returned no data are dropped.
    """
    frames = []
    for ticker, (company, sector) in tickers.items():
        if ticker not in raw.columns.get_level_values(level=1):
            continue
        df = raw.xs(ticker, axis=1, level=1).reset_index()
        df.columns = [c.lower() for c in df.columns]
        df = df.rename(columns={"date": "trade_date"})
        df["ticker"] = ticker
        df["company"] = company
        df["sector"] = sector
        frames.append(df)

    if not frames:
        return pd.DataFrame(
            columns=[
                "trade_date", "ticker", "company", "sector",
                "open", "high", "low", "close", "volume",
            ]
        )

    tidy = pd.concat(frames, ignore_index=True)
    tidy = tidy.dropna(subset=["close"])
    tidy["trade_date"] = pd.to_datetime(tidy["trade_date"]).dt.date
    cols = ["trade_date", "ticker", "company", "sector", "open", "high", "low", "close", "volume"]
    return tidy[cols]


@asset(
    key=AssetKey(["lake", "tadawul_prices"]),
    group_name="ingestion",
    description="Daily OHLCV for ~30 major Tadawul tickers, landed as Parquet in the raw zone.",
)
def tadawul_prices(
    context: AssetExecutionContext, data_lake: DataLakeResource
) -> MaterializeResult:
    raw = yf.download(
        tickers=list(TADAWUL_TICKERS),
        period="5d",  # small overlap so reruns and holidays self-heal
        interval="1d",
        group_by="column",
        auto_adjust=True,
        progress=False,
    )
    tidy = tidy_ohlcv(raw, TADAWUL_TICKERS)
    if tidy.empty:
        raise ValueError("yfinance returned no rows for any Tadawul ticker")

    written = []
    for trade_date, day_df in tidy.groupby("trade_date"):
        path = data_lake.write_parquet(
            day_df, zone="raw", dataset="tadawul", partition=f"date={trade_date}"
        )
        written.append(path)
        context.log.info("Wrote %d rows to %s", len(day_df), path)

    return MaterializeResult(
        metadata={
            "rows": len(tidy),
            "tickers": tidy["ticker"].nunique(),
            "latest_trade_date": str(max(tidy["trade_date"])),
            "partitions_written": MetadataValue.json([str(p) for p in written]),
            "as_of": str(date.today()),
        }
    )
