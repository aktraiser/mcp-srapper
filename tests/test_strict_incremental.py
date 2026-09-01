"""Mode strict-only incrémental : le builder ignore le legacy tainté et ne
matérialise jamais de reconstruction historique."""
from builders.build_episodes import is_buildable

FROZEN = {"e_frozen"}
TAINTED = {"e_legacy"}


def test_frozen_event_is_never_rebuilt():
    # Une représentation figée est immuable : jamais reconstruite, quel que soit le mode.
    assert is_buildable("e_frozen", FROZEN, TAINTED, strict_only=True) is False
    assert is_buildable("e_frozen", FROZEN, TAINTED, strict_only=False) is False


def test_strict_only_skips_legacy_tainted_event():
    # Un event ancien tainté ne peut jamais devenir vérifiable -> ignoré en strict.
    assert is_buildable("e_legacy", FROZEN, TAINTED, strict_only=True) is False


def test_legacy_included_only_when_explicitly_requested():
    # Hors strict (MARKET_MEMORY_INCLUDE_LEGACY=1), le legacy redevient constructible.
    assert is_buildable("e_legacy", FROZEN, TAINTED, strict_only=False) is True


def test_new_untainted_event_is_buildable():
    assert is_buildable("e_new", FROZEN, TAINTED, strict_only=True) is True
    assert is_buildable("e_new", FROZEN, TAINTED, strict_only=False) is True
