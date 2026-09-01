"""Calendrier XNYS : DST, jours fermés, early closes et horizon strict."""
from datetime import date, datetime, timedelta, timezone

import pytest

from builders.build_outcomes import MarketObservation, forward_outcome, load_xnys_schedule

UTC = timezone.utc


@pytest.fixture(scope="module")
def xnys_2025():
    return load_xnys_schedule(date(2025, 1, 1), date(2025, 12, 31))


@pytest.mark.parametrize(
    ("moment", "expected_date", "expected_close"),
    [
        # Hiver : 16:00 ET = 21:00 UTC.
        (datetime(2025, 1, 6, 20, 59, 59, tzinfo=UTC), date(2025, 1, 6), datetime(2025, 1, 6, 21, tzinfo=UTC)),
        (datetime(2025, 1, 6, 21, 0, 0, tzinfo=UTC), date(2025, 1, 7), datetime(2025, 1, 7, 21, tzinfo=UTC)),
        # Été : 16:00 ET = 20:00 UTC.
        (datetime(2025, 7, 7, 19, 59, 59, tzinfo=UTC), date(2025, 7, 7), datetime(2025, 7, 7, 20, tzinfo=UTC)),
        (datetime(2025, 7, 7, 20, 0, 0, tzinfo=UTC), date(2025, 7, 8), datetime(2025, 7, 8, 20, tzinfo=UTC)),
        # 3 juillet et Black Friday : close anticipé à 13:00 ET.
        (datetime(2025, 7, 3, 16, 59, 59, tzinfo=UTC), date(2025, 7, 3), datetime(2025, 7, 3, 17, tzinfo=UTC)),
        (datetime(2025, 7, 3, 17, 0, 0, tzinfo=UTC), date(2025, 7, 7), datetime(2025, 7, 7, 20, tzinfo=UTC)),
        (datetime(2025, 11, 28, 17, 59, 59, tzinfo=UTC), date(2025, 11, 28), datetime(2025, 11, 28, 18, tzinfo=UTC)),
        (datetime(2025, 11, 28, 18, 0, 0, tzinfo=UTC), date(2025, 12, 1), datetime(2025, 12, 1, 21, tzinfo=UTC)),
        # Dimanche du changement DST : le lundi ferme déjà à 20:00 UTC.
        (datetime(2025, 3, 9, 12, 0, tzinfo=UTC), date(2025, 3, 10), datetime(2025, 3, 10, 20, tzinfo=UTC)),
        # 9 janvier 2025 : fermeture exceptionnelle (deuil national Carter).
        (datetime(2025, 1, 9, 12, 0, tzinfo=UTC), date(2025, 1, 10), datetime(2025, 1, 10, 21, tzinfo=UTC)),
    ],
)
def test_first_close_strictly_after(xnys_2025, moment, expected_date, expected_close):
    result = xnys_2025.first_close_after(moment)
    assert result is not None
    _, session_date, close = result
    assert session_date == expected_date
    assert close == expected_close


def test_timezone_offset_is_normalized(xnys_2025):
    edt = timezone(timedelta(hours=-4))
    result = xnys_2025.first_close_after(datetime(2025, 7, 7, 15, 59, 59, tzinfo=edt))
    assert result is not None
    assert result[1:] == (date(2025, 7, 7), datetime(2025, 7, 7, 20, tzinfo=UTC))


def test_naive_datetime_is_rejected(xnys_2025):
    with pytest.raises(ValueError, match="timezone-aware"):
        xnys_2025.first_close_after(datetime(2025, 7, 7, 12))


def test_schedule_boundary_fails_closed(xnys_2025):
    assert xnys_2025.first_close_after(datetime(2025, 12, 31, 23, tzinfo=UTC)) is None


def test_forward_horizon_is_exact_sessions_and_uses_exact_exit_close(xnys_2025):
    prices = {
        date(2025, 7, 3): 100.0,
        date(2025, 7, 9): 103.0,
    }
    outcome = forward_outcome(
        prices,
        xnys_2025,
        datetime(2025, 7, 3, 16, 59, 59, tzinfo=UTC),
        sessions=3,
    )
    assert outcome is not None
    assert outcome.entry_date == date(2025, 7, 3)
    assert outcome.exit_date == date(2025, 7, 9)
    assert outcome.return_pct == 3.0
    assert outcome.available_at == datetime(2025, 7, 9, 20, tzinfo=UTC)


def test_missing_exact_fred_session_does_not_skip_forward(xnys_2025):
    prices = {
        date(2025, 7, 3): 100.0,
        # Une observation plus tardive existe, mais celle du 9 manque.
        date(2025, 7, 10): 103.0,
    }
    assert forward_outcome(
        prices,
        xnys_2025,
        datetime(2025, 7, 3, 16, 59, 59, tzinfo=UTC),
        sessions=3,
    ) is None


def test_outcome_availability_includes_when_fred_was_really_fetched(xnys_2025):
    fetched_late = datetime(2025, 7, 10, 8, tzinfo=UTC)
    prices = {
        date(2025, 7, 3): MarketObservation(100, datetime(2025, 7, 3, 18, tzinfo=UTC)),
        date(2025, 7, 9): MarketObservation(103, fetched_late),
    }
    outcome = forward_outcome(
        prices,
        xnys_2025,
        datetime(2025, 7, 3, 16, 59, 59, tzinfo=UTC),
        sessions=3,
    )
    assert outcome is not None
    assert outcome.available_at == fetched_late


def test_direction_uses_unrounded_prices(xnys_2025):
    prices = {
        date(2025, 7, 3): 100.0,
        date(2025, 7, 9): 100.0001,
    }
    outcome = forward_outcome(
        prices,
        xnys_2025,
        datetime(2025, 7, 3, 16, 59, 59, tzinfo=UTC),
        sessions=3,
    )
    assert outcome is not None
    assert outcome.return_pct == 0.0
    assert outcome.is_up is True
