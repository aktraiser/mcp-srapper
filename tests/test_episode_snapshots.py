"""La vue sémantique d'un épisode ne doit jamais être réécrite par le futur."""
from datetime import datetime, timedelta, timezone

import pytest

from builders.build_episodes import (
    ArticleObservation,
    freeze_snapshot,
    onset,
    snapshot_metadata,
)

UTC = timezone.utc
BASE = datetime(2025, 7, 1, 12, tzinfo=UTC)


def article(
    identifier: str,
    published_delta: timedelta,
    collected_delta: timedelta,
    **kwargs,
) -> ArticleObservation:
    return ArticleObservation(
        id=identifier,
        published_at=BASE + published_delta,
        collected_at=BASE + collected_delta,
        source_domain=kwargs.pop("source_domain", "example.com"),
        title=kwargs.pop("title", identifier),
        **kwargs,
    )


def test_waits_for_full_window_and_freezes_exactly_at_cutoff():
    first = article("a", timedelta(0), timedelta(0))
    assert freeze_snapshot([first], BASE + timedelta(hours=5, minutes=59, seconds=59)) is None
    frozen = freeze_snapshot([first], BASE + timedelta(hours=6))
    assert frozen is not None
    assert frozen.first_observed_at == BASE
    assert frozen.source_cutoff == BASE + timedelta(hours=6)
    assert frozen.as_of == BASE + timedelta(hours=6)
    assert frozen.provenance_status == "historical_reconstruction_unversioned_cluster"


def test_new_rows_use_verified_cluster_assignment_time():
    first = article(
        "a",
        timedelta(0),
        timedelta(0),
        event_assigned_at=BASE + timedelta(minutes=2),
        embedding_generated_at=BASE + timedelta(minutes=1),
        has_embedding=True,
    )
    assert freeze_snapshot([first], BASE + timedelta(hours=6, minutes=1)) is None
    frozen = freeze_snapshot([first], BASE + timedelta(hours=6, minutes=2))
    assert frozen is not None
    assert frozen.source_cutoff == BASE + timedelta(hours=6, minutes=2)
    assert frozen.as_of == BASE + timedelta(hours=6, minutes=2)
    assert frozen.provenance_status == "verified_feature_timestamps"


def test_reassignment_after_cutoff_cannot_remove_article_retroactively():
    first = article(
        "a",
        timedelta(0),
        timedelta(0),
        event_assigned_at=BASE,
        membership_valid_to=BASE + timedelta(hours=7),
    )
    frozen = freeze_snapshot([first], BASE + timedelta(hours=8))
    assert frozen is not None
    assert frozen.source_cutoff == BASE + timedelta(hours=6)
    assert frozen.as_of == BASE + timedelta(hours=8)
    assert [item.id for item in frozen.articles] == ["a"]


def test_membership_closed_at_or_before_cutoff_is_excluded():
    removed = article(
        "removed",
        timedelta(0),
        timedelta(0),
        event_assigned_at=BASE,
        membership_valid_to=BASE + timedelta(hours=5),
    )
    survivor = article(
        "survivor",
        timedelta(minutes=1),
        timedelta(minutes=1),
        event_assigned_at=BASE + timedelta(minutes=1),
    )
    frozen = freeze_snapshot([removed, survivor], BASE + timedelta(hours=8))
    assert frozen is not None
    assert [item.id for item in frozen.articles] == ["survivor"]


def test_legacy_taint_forces_unverified_even_when_remaining_members_have_a_ledger():
    tracked = article(
        "new-member",
        timedelta(0),
        timedelta(0),
        event_assigned_at=BASE,
    )
    frozen = freeze_snapshot(
        [tracked], BASE + timedelta(hours=8), force_unverified=True,
    )
    assert frozen is not None
    assert frozen.provenance_status == "historical_reconstruction_unversioned_cluster"


def test_observed_at_is_max_of_publication_and_collection():
    backdated = article("late", timedelta(hours=-10), timedelta(hours=2))
    scheduled = article("future", timedelta(hours=3), timedelta(hours=1))
    assert backdated.observed_at == BASE + timedelta(hours=2)
    assert scheduled.observed_at == BASE + timedelta(hours=3)


def test_cutoff_is_inclusive_but_one_microsecond_later_is_excluded():
    first = article("a", timedelta(0), timedelta(0))
    boundary = article("b", timedelta(hours=6), timedelta(hours=6))
    future = article(
        "c",
        timedelta(hours=6, microseconds=1),
        timedelta(hours=6, microseconds=1),
    )
    frozen = freeze_snapshot([future, boundary, first], BASE + timedelta(days=1))
    assert frozen is not None
    assert [a.id for a in frozen.articles] == ["a", "b"]


def test_late_collected_backdated_article_is_excluded():
    first = article("a", timedelta(0), timedelta(0))
    late = article("late", timedelta(hours=1), timedelta(hours=7))
    frozen = freeze_snapshot([first, late], BASE + timedelta(days=1))
    assert frozen is not None
    assert [a.id for a in frozen.articles] == ["a"]


def test_future_burst_cannot_change_snapshot_or_onset():
    initial = [
        article("a", timedelta(0), timedelta(0), source_domain="a.test"),
        article("b", timedelta(hours=1), timedelta(hours=1), source_domain="b.test"),
    ]
    future_burst = [
        article(f"future-{i}", timedelta(hours=48, minutes=i), timedelta(hours=48, minutes=i))
        for i in range(8)
    ]
    now = BASE + timedelta(days=4)
    before = freeze_snapshot(initial, now)
    after = freeze_snapshot(list(reversed(initial + future_burst)), now)
    assert before == after
    assert before is not None
    before_onset = onset([(a.published_at, a.source_domain or "") for a in before.articles])
    after_onset = onset([(a.published_at, a.source_domain or "") for a in after.articles])
    assert before_onset == after_onset


def test_input_permutation_is_deterministic_and_empty_domains_are_not_counted():
    items = [
        article("b", timedelta(hours=1), timedelta(hours=1), source_domain=""),
        article("a", timedelta(0), timedelta(0), source_domain=None),
        article("c", timedelta(hours=2), timedelta(hours=2), source_domain="c.test"),
    ]
    one = freeze_snapshot(items, BASE + timedelta(days=1))
    two = freeze_snapshot(reversed(items), BASE + timedelta(days=1))
    assert one == two
    assert one is not None
    result = onset([(a.published_at, a.source_domain or "") for a in one.articles])
    assert result[-1] == 1


def test_snapshot_metadata_comes_only_from_eligible_articles():
    items = [
        article(
            "known",
            timedelta(0),
            timedelta(0),
            title="Known at decision time",
            event_hint="earnings",
            theme="markets",
            entities=[{"name": "ACME"}],
            relevance_score=9,
            source_tier=1,
        ),
        article(
            "future",
            timedelta(hours=9),
            timedelta(hours=9),
            title="Future secret",
            event_hint="acquisition",
            theme="future",
            entities=[{"name": "LEAK"}],
            relevance_score=10,
            source_tier=1,
        ),
    ]
    frozen = freeze_snapshot(items, BASE + timedelta(days=1))
    assert frozen is not None
    assert snapshot_metadata(frozen) == (
        "earnings", "earnings", "markets", ["ACME"], "Known at decision time",
    )


def test_naive_timestamps_fail_closed():
    bad = ArticleObservation(
        id="bad",
        published_at=datetime(2025, 1, 1),
        collected_at=datetime(2025, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        freeze_snapshot([bad], BASE + timedelta(days=1))


def test_missing_collection_time_fails_closed():
    bad = ArticleObservation(
        id="bad",
        published_at=BASE,
        collected_at=None,
    )
    with pytest.raises(ValueError, match="collected_at manque"):
        freeze_snapshot([bad], BASE + timedelta(days=1))
