from orchestration.assets.ingestion.econ import INDICATORS, tidy_econ

ROWS = [
    {"date": "2025", "value": 2.08},
    {"date": "2024", "value": 1.71},
    {"date": "2023", "value": None},  # dropped: no value
    {"date": "QIII", "value": 5.0},  # dropped: not a year
]


def test_tidy_econ_flattens_and_filters():
    df = tidy_econ("FP.CPI.TOTL.ZG", "Inflation", ROWS)

    assert list(df.columns) == ["indicator_code", "indicator_name", "year", "value"]
    assert df["year"].tolist() == [2024, 2025]  # sorted ascending, invalid rows gone
    assert df["value"].notna().all()


def test_tidy_econ_handles_empty_input():
    df = tidy_econ("X", "Nothing", [])
    assert df.empty


def test_indicator_catalog_is_well_formed():
    assert len(INDICATORS) >= 4
    for code, name in INDICATORS.items():
        assert "." in code
        assert name
