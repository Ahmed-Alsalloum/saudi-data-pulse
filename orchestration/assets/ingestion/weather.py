"""Ingest hourly weather observations for major Saudi cities.

Primary source: Open-Meteo (keyless, returns a 48h lookback window so hourly
runs overlap and self-heal). Open-Meteo throttles shared datacenter IPs, so
when it is unreachable (e.g. from GitHub Actions runners) the asset falls
back to MET Norway's api.met.no, which allows automated clients that send a
descriptive User-Agent and returns current conditions per city. Staging
deduplicates on (city, observed_at) either way.
"""

import pandas as pd
import requests
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, RetryPolicy, asset

from orchestration.resources import DataLakeResource

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
MET_NO_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
USER_AGENT = "saudi-data-pulse/1.0 (https://github.com/Ahmed-Alsalloum/saudi-data-pulse)"

CITIES: dict[str, tuple[float, float]] = {
    "riyadh": (24.7136, 46.6753),
    "jeddah": (21.4858, 39.1925),
    "dammam": (26.4207, 50.0888),
    "makkah": (21.3891, 39.8579),
    "madinah": (24.5247, 39.5692),
    "abha": (18.2465, 42.5117),
}

COLUMNS = ["observed_at", "city", "temperature_c", "humidity_pct", "wind_speed_kmh"]


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
    return df[COLUMNS]


def tidy_met_no(city: str, payload: dict, max_hours: int = 3) -> pd.DataFrame:
    """Extract the first few (i.e. current) hours from a met.no locationforecast.

    met.no timestamps are UTC; convert to Asia/Riyadh naive to match the
    Open-Meteo rows already in the lake. Wind arrives in m/s -> km/h.
    """
    rows = []
    for entry in payload["properties"]["timeseries"][:max_hours]:
        details = entry["data"]["instant"]["details"]
        if "air_temperature" not in details:
            continue
        observed_at = (
            pd.to_datetime(entry["time"]).tz_convert("Asia/Riyadh").tz_localize(None)
        )
        rows.append(
            {
                "observed_at": observed_at,
                "city": city,
                "temperature_c": details["air_temperature"],
                "humidity_pct": details.get("relative_humidity"),
                "wind_speed_kmh": round(details.get("wind_speed", 0) * 3.6, 1),
            }
        )
    return pd.DataFrame(rows, columns=COLUMNS)


def _fetch_open_meteo(city: str, lat: float, lon: float) -> pd.DataFrame:
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
        timeout=30,
    )
    response.raise_for_status()
    return tidy_weather(city, response.json())


def _fetch_met_no(city: str, lat: float, lon: float) -> pd.DataFrame:
    response = requests.get(
        MET_NO_URL,
        params={"lat": lat, "lon": lon},
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()
    return tidy_met_no(city, response.json())


@asset(
    key=AssetKey(["lake", "weather_hourly"]),
    group_name="ingestion",
    description="Hourly weather for 6 Saudi cities (Open-Meteo, met.no fallback).",
    retry_policy=RetryPolicy(max_retries=2, delay=20),
)
def weather_hourly(
    context: AssetExecutionContext, data_lake: DataLakeResource
) -> MaterializeResult:
    source = "open-meteo"
    try:
        frames = [_fetch_open_meteo(city, lat, lon) for city, (lat, lon) in CITIES.items()]
    except requests.RequestException as exc:
        context.log.warning("Open-Meteo unreachable (%s); falling back to met.no", exc)
        source = "met.no"
        frames = [_fetch_met_no(city, lat, lon) for city, (lat, lon) in CITIES.items()]

    observations = pd.concat(frames, ignore_index=True)
    # Forecast windows include future hours; keep only actual observations.
    now_riyadh = pd.Timestamp.now(tz="Asia/Riyadh").tz_localize(None)
    observations = observations[observations["observed_at"] <= now_riyadh]
    if observations.empty:
        raise ValueError(f"{source} returned no past observations for any city")

    written = 0
    for obs_date, day_df in observations.groupby(observations["observed_at"].dt.date):
        data_lake.write_parquet(day_df, zone="raw", dataset="weather", partition=f"date={obs_date}")
        written += len(day_df)

    return MaterializeResult(
        metadata={
            "rows": written,
            "cities": observations["city"].nunique(),
            "source": source,
            "latest_observation": str(observations["observed_at"].max()),
        }
    )
