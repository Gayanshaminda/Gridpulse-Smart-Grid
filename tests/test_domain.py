import math
import pytest
from grid.domain import reading, validate, energy, bill
from grid.config import TICKS_PER_DAY


def test_energy_is_conserved():
    for demand, solar in [(4, 0), (4, 2), (4, 7), (0, 0)]:
        e = energy(demand, solar)
        assert e["grid_import_kwh"] + e["solar_used_kwh"] == demand
        assert e["solar_export_kwh"] + e["solar_used_kwh"] == solar


def test_exports_do_not_cancel_later_imports():
    imports = energy(1, 5)["grid_import_kwh"] + energy(3, 0)["grid_import_kwh"]
    assert imports == 3
    assert bill(imports, 35, False) == 105


def test_subsidy_and_money_rounding():
    assert bill(10, 35, True) == 280
    assert bill(0.0005, 10, False) == 0.01


@pytest.mark.parametrize("field,value", [
    ("consumption_kwh", -1), ("solar_generation_kwh", float("nan")),
    ("consumption_kwh", float("inf")), ("interval_seconds", 0),
    ("interval_seconds", True), ("household_id", ""), ("grid_zone", "Unknown"),
    ("event_time", "2026-01-01T00:00:00"), ("consumption_kwh", "2")])
def test_bad_readings_rejected(field, value):
    event = reading(0, 1)
    event[field] = value
    with pytest.raises(ValueError):
        validate(event)


def test_generator_clock_boundaries_and_reproducibility():
    assert reading(2, 4) == reading(2, 4)
    assert validate(reading(TICKS_PER_DAY - 1, 1)).date().isoformat() == "2026-01-01"
    assert validate(reading(TICKS_PER_DAY, 1)).date().isoformat() == "2026-01-02"
    assert all(validate(reading(i, 1)) for i in range(TICKS_PER_DAY * 2))


def test_cloud_changes_solar_not_consumption():
    sunny, cloudy = reading(TICKS_PER_DAY // 2, 1), reading(TICKS_PER_DAY // 2, 1, True)
    assert cloudy["solar_generation_kwh"] < sunny["solar_generation_kwh"]
    assert cloudy["consumption_kwh"] == sunny["consumption_kwh"]

