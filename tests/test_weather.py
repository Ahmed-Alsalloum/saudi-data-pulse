from orchestration.assets.ingestion.weather import CITIES, tidy_met_no, tidy_weather

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


MET_NO_PAYLOAD = {
    "properties": {
        "timeseries": [
            {
                "time": "2026-07-03T12:00:00Z",
                "data": {
                    "instant": {
                        "details": {
                            "air_temperature": 43.0,
                            "relative_humidity": 8.0,
                            "wind_speed": 5.0,  # m/s
                        }
                    }
                },
            },
            {
                "time": "2026-07-03T13:00:00Z",
                "data": {"instant": {"details": {}}},  # dropped: no temperature
            },
        ]
    }
}


def test_tidy_met_no_converts_units_and_timezone():
    df = tidy_met_no("riyadh", MET_NO_PAYLOAD)

    assert list(df.columns) == [
        "observed_at", "city", "temperature_c", "humidity_pct", "wind_speed_kmh",
    ]
    assert len(df) == 1
    row = df.iloc[0]
    assert str(row["observed_at"]) == "2026-07-03 15:00:00"  # UTC+3 Riyadh, tz-naive
    assert row["wind_speed_kmh"] == 18.0  # 5 m/s * 3.6


def test_city_catalog_is_well_formed():
    assert len(CITIES) >= 5
    for city, (lat, lon) in CITIES.items():
        assert city == city.lower()
        assert 16 <= lat <= 33  # Saudi Arabia's latitude range
        assert 34 <= lon <= 56
