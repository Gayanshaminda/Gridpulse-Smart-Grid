import csv
import pytest
import grid.batch as batch


def write(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["simulated_date", "household_id", "tariff_rate", "subsidy_flag"])
        writer.writeheader()
        writer.writerows(rows)


@pytest.mark.parametrize("bad", ["duplicate", "missing", "negative", "nan", "wrong_day", "bad_flag"])
def test_invalid_tariff_feed_fails(tmp_path, monkeypatch, bad):
    monkeypatch.setattr(batch, "HOUSEHOLDS", 2)
    rows = [{"simulated_date": "2026-01-01", "household_id": f"H{i:03}", "tariff_rate": 35, "subsidy_flag": "0"} for i in (1, 2)]
    if bad == "duplicate": rows[1]["household_id"] = "H001"
    if bad == "missing": rows.pop()
    if bad == "negative": rows[0]["tariff_rate"] = -1
    if bad == "nan": rows[0]["tariff_rate"] = "nan"
    if bad == "wrong_day": rows[0]["simulated_date"] = "2026-01-02"
    if bad == "bad_flag": rows[0]["subsidy_flag"] = "yes"
    path = tmp_path / "tariff.csv"
    write(path, rows)
    with pytest.raises(ValueError):
        batch.load_tariffs(path, "2026-01-01")

