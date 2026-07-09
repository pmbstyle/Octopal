from __future__ import annotations

from octopal.runtime.octo.prompt_builder import _prune_recent_history_window


def test_context_pruning_keeps_recent_tail_under_budget() -> None:
    history = [
        ("user", "u1 " * 600),
        ("assistant", "a1 " * 600),
        ("user", "u2 " * 600),
        ("assistant", "a2 " * 600),
        ("user", "u3 " * 600),
    ]
    pruned, stats = _prune_recent_history_window(
        history,
        max_history_chars=3000,
        keep_recent=2,
        per_message_chars=5000,
    )

    assert len(pruned) <= 2
    assert stats["dropped"] >= 1
    assert stats["total_chars"] <= 3000


def test_context_pruning_trims_oversized_messages() -> None:
    large = "x" * 9000
    history = [("assistant", large)]
    pruned, stats = _prune_recent_history_window(
        history,
        max_history_chars=10000,
        keep_recent=1,
        per_message_chars=1200,
    )

    assert len(pruned) == 1
    assert len(pruned[0][1]) <= 1200
    assert "pruned for context window" in pruned[0][1]
    assert stats["trimmed"] == 1
