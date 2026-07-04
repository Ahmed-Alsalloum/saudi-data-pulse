"""Ingest hourly weather observations for major Saudi cities via Open-Meteo.

Open-Meteo is keyless and free for non-commercial use. Each run fetches a
48-hour lookback window, so hourly runs overlap heavily and the pipeline
self-heals across missed runs; staging deduplicates on (city, observed_at).
"""

import pandas as pd
import requests
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, RetryPolicy, asset

from orchestration.resources import DataLakeResource

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

CITIES: dict[str, tuple[float, float]] = {
    "riyadh": (24.7136, 46.6753),
    "jeddah": (21.4858, 39.1925),
    "dammam": (26.4207, 50.0888),
    "makkah": (21.3891, 39.8579),
    "madinah": (24.5247, 39.5692),
    "abha": (18.2465, 42.5117),
}


def tidy_weather(city: str, payload: dict) -> pd.DataFrame:
    """Turn Open-Meteo's parallel hourly arrays into one tidy table for a city."""
    hourly = payload["hourly"]
    df = pd.DataFrame(
        {
            "observed_at": pd.to_datetime(hourly["time"]),
            "temperature_c": hourly["temperature_2m"],
            "humidity_pct": hourly["relative_humidity_2m"],
            "wind_speed_kmh": hourly["wind_speed_10m"],
        }
    )
    df["city"] = city
    df = df.dropna(subset=["temperature_c"])
    return df[["observed_at", "city", "temperature_c", "humidity_pct", "wind_speed_kmh"]]


@asset(
    key=AssetKey(["lake", "weather_hourly"]),
    group_name="ingestion",
    description="Hourly temperature/humidity/wind for 6 Saudi cities, from Open-Meteo.",
    retry_policy=RetryPolicy(max_retries=2, delay=20),
)
def weather_hourly(
    context: AssetExecutionContext, data_lake: DataLakeResource
) -> MaterializeResult:
    frames = []
    for city, (lat, lon) in CITIES.items():
        response = requests.get(
            OPEN_METEO_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
                "past_days": 2,
                "forecast_days": 1,
                "timezone": "Asia/Riyadh",
            },
            timeout=60,
        )
        response.raise_for_status()
        frames.append(tidy_weather(city, response.json()))

    observations = pd.concat(frames, ignore_index=True)
    # The forecast_days window includes future hours; keep only actual observations.
    now_riyadh = pd.Timestamp.now(tz="Asia/Riyadh").tz_localize(None)
    observations = observations[observations["observed_at"] <= now_riyadh]
    if observations.empty:
        raise ValueError("Open-Meteo returned no past observations for any city")

    written = 0
    for obs_date, day_df in observations.groupby(observations["observed_at"].dt.date):
        data_lake.write_parquet(day_df, zone="raw", dataset="weather", partition=f"date={obs_date}")
        written += len(day_df)

    return MaterializeResult(
        metadata={
            "rows": written,
            "cities": observations["city"].nunique(),
            "latest_observation": str(observations["observed_at"].max()),
        }
    )
