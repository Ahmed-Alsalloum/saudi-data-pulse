from orchestration.assets.ingestion.weather import CITIES, tidy_weather

PAYLOAD = {
    "hourly": {
        "time": ["2026-07-03T00:00", "2026-07-03T01:00", "2026-07-03T02:00"],
        "temperature_2m": [31.2, 30.8, None],
        "relative_humidity_2m": [18, 20, 22],
        "wind_speed_10m": [11.5, 9.3, 8.1],
    }
}


def test_tidy_weather_builds_long_table():
    df = tidy_weather("riyadh", PAYLOAD)

    assert list(df.columns) == [
        "observed_at", "city", "temperature_c", "humidity_pct", "wind_speed_kmh",
    ]
    assert set(df["city"]) == {"riyadh"}
    assert len(df) == 2  # the null-temperature hour is dropped
    assert df["temperature_c"].tolist() == [31.2, 30.8]


def test_city_catalog_is_well_formed():
    assert len(CITIES) >= 5
    for city, (lat, lon) in CITIES.items():
        assert city == city.lower()
        assert 16 <= lat <= 33  # Saudi Arabia's latitude range
        assert 34 <= lon <= 56
