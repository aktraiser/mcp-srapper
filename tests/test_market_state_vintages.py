"""Le market state ne doit jamais utiliser une révision collectée dans le futur."""
from datetime import date, datetime, timezone

import pytest

from builders.build_market_state import FredVintage, momentum, point_in_time, trend_label

UTC = timezone.utc
DECISION = datetime(2025, 7, 10, 15, tzinfo=UTC)


def vintage(observed_on: date, value: float, fetched_at: datetime) -> FredVintage:
    return FredVintage(observed_on, value, fetched_at)


def test_revision_fetched_after_decision_is_excluded():
    rows = [
        vintage(date(2025, 7, 9), 100, datetime(2025, 7, 10, 10, tzinfo=UTC)),
        vintage(date(2025, 7, 9), 999, datetime(2025, 7, 10, 16, tzinfo=UTC)),
    ]
    chosen = point_in_time(rows, DECISION)
    assert chosen is not None
    assert chosen.value == 100


def test_newer_observation_unavailable_at_decision_does_not_replace_prior_day():
    rows = [
        vintage(date(2025, 7, 8), 98, datetime(2025, 7, 9, 8, tzinfo=UTC)),
        vintage(date(2025, 7, 9), 100, datetime(2025, 7, 10, 16, tzinfo=UTC)),
    ]
    assert point_in_time(rows, DECISION).value == 98


def test_same_day_observation_and_equal_fetch_boundary_are_strictly_excluded():
    rows = [
        vintage(date(2025, 7, 9), 99, datetime(2025, 7, 10, 14, tzinfo=UTC)),
        vintage(date(2025, 7, 10), 100, datetime(2025, 7, 10, 14, tzinfo=UTC)),
        vintage(date(2025, 7, 8), 999, DECISION),
    ]
    assert point_in_time(rows, DECISION).value == 99


def test_momentum_uses_only_vintages_available_at_the_same_decision():
    rows = [
        vintage(date(2025, 7, 3), 100, datetime(2025, 7, 4, 8, tzinfo=UTC)),
        vintage(date(2025, 7, 9), 102, datetime(2025, 7, 10, 8, tzinfo=UTC)),
        vintage(date(2025, 7, 9), 200, datetime(2025, 7, 11, 8, tzinfo=UTC)),
    ]
    assert momentum(rows, DECISION, days=5) == 2.0


def test_naive_fetch_timestamp_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        FredVintage(date(2025, 7, 9), 100, datetime(2025, 7, 10, 8))


def test_trend_label_keeps_missing_and_flat_distinct_from_down():
    assert trend_label(None) is None
    assert trend_label(0) == "flat"
    assert trend_label(0.01) == "up"
    assert trend_label(-0.01) == "down"
