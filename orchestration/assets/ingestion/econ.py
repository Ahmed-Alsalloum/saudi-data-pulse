"""Ingest Saudi macroeconomic indicators from the World Bank API.

The Saudi Open Data portal (open.data.gov.sa) rejects programmatic clients at
its WAF, so annual indicators come from the World Bank's keyless API instead —
same underlying GASTAT/SAMA figures, published with a stable contract.

Full-refresh: each run overwrites the single snapshot partition.
"""

import pandas as pd
import requests
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from orchestration.resources import DataLakeResource

WORLD_BANK_URL = "https://api.worldbank.org/v2/country/SAU/indicator/{code}"

INDICATORS: dict[str, str] = {
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "SP.POP.TOTL": "Population, total",
    "SL.UEM.TOTL.ZS": "Unemployment (% of total labor force)",
}


def tidy_econ(code: str, name: str, rows: list[dict]) -> pd.DataFrame:
    """Flatten one indicator's World Bank rows into (code, name, year, value)."""
    df = pd.DataFrame(
        [
            {
                "indicator_code": code,
                "indicator_name": name,
                "year": int(r["date"]),
                "value": r["value"],
            }
            for r in rows
            if r.get("value") is not None and str(r.get("date", "")).isdigit()
        ]
    )
    if df.empty:
        return pd.DataFrame(columns=["indicator_code", "indicator_name", "year", "value"])
    return df.sort_values("year", ignore_index=True)


@asset(
    key=AssetKey(["lake", "econ_indicators"]),
    group_name="ingestion",
    description="Annual Saudi macro indicators (CPI, GDP growth, population, unemployment).",
)
def econ_indicators(
    context: AssetExecutionContext, data_lake: DataLakeResource
) -> MaterializeResult:
    frames = []
    for code, name in INDICATORS.items():
        response = requests.get(
            WORLD_BANK_URL.format(code=code),
            params={"format": "json", "per_page": 200},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload[1] if len(payload) > 1 and payload[1] else []
        frames.append(tidy_econ(code, name, rows))
        context.log.info("Fetched %d rows for %s", len(frames[-1]), code)

    indicators = pd.concat(frames, ignore_index=True)
    if indicators.empty:
        raise ValueError("World Bank API returned no data for any indicator")

    path = data_lake.write_parquet(
        indicators, zone="raw", dataset="econ_indicators", partition="snapshot"
    )
    return MaterializeResult(
        metadata={
            "rows": len(indicators),
            "indicators": indicators["indicator_code"].nunique(),
            "latest_year": int(indicators["year"].max()),
            "path": str(path),
        }
    )
