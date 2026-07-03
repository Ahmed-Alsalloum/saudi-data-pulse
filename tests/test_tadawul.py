import pandas as pd

from orchestration.assets.ingestion.tadawul import TADAWUL_TICKERS, tidy_ohlcv

TICKERS = {
    "2222.SR": ("Saudi Aramco", "Energy"),
    "1120.SR": ("Al Rajhi Bank", "Financials"),
}


def make_yfinance_frame() -> pd.DataFrame:
    """Mimic yfinance's wide format: (field, ticker) MultiIndex columns."""
    dates = pd.to_datetime(["2026-06-30", "2026-07-01"])
    fields = ["Open", "High", "Low", "Close", "Volume"]
    columns = pd.MultiIndex.from_product([fields, list(TICKERS)])
    data = [[10.0] * len(columns), [11.0] * len(columns)]
    return pd.DataFrame(data, index=pd.Index(dates, name="Date"), columns=columns)


def test_tidy_ohlcv_reshapes_to_long_format():
    tidy = tidy_ohlcv(make_yfinance_frame(), TICKERS)

    assert len(tidy) == 4  # 2 dates x 2 tickers
    assert list(tidy.columns) == [
        "trade_date", "ticker", "company", "sector",
        "open", "high", "low", "close", "volume",
    ]
    aramco = tidy[tidy["ticker"] == "2222.SR"]
    assert set(aramco["company"]) == {"Saudi Aramco"}
    assert set(aramco["sector"]) == {"Energy"}


def test_tidy_ohlcv_drops_rows_without_close():
    raw = make_yfinance_frame()
    raw.iloc[0, raw.columns.get_loc(("Close", "2222.SR"))] = None

    tidy = tidy_ohlcv(raw, TICKERS)

    assert len(tidy) == 3
    assert tidy["close"].notna().all()


def test_tidy_ohlcv_handles_missing_ticker_columns():
    raw = make_yfinance_frame()
    extra = {**TICKERS, "9999.SR": ("Ghost Corp", "Nowhere")}

    tidy = tidy_ohlcv(raw, extra)

    assert "9999.SR" not in set(tidy["ticker"])


def test_ticker_universe_is_well_formed():
    assert len(TADAWUL_TICKERS) >= 25
    for ticker, (company, sector) in TADAWUL_TICKERS.items():
        assert ticker.endswith(".SR")
        assert company and sector
