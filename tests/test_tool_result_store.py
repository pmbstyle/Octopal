from __future__ import annotations

import re

import pytest

from octopal.runtime.tool_result_store import ToolResultStore


def test_store_preserves_complete_unicode_source_for_exact_reads_and_searches(tmp_path) -> None:
    content = '{"url":"https://example.test","snippet":"Начало NEEDLE конец"}'
    store = ToolResultStore(tmp_path)

    reference = store.store("web_fetch", content)

    assert reference is not None
    search = store.search(reference.handle, query="needle", max_matches=1)
    assert search["matches"] == [{"offset": content.index("NEEDLE"), "length": 6}]
    assert store.read(reference.handle, offset=0, length=len(content))["content"] == content
    assert (
        store.read(reference.handle, offset=content.index("NEEDLE"), length=6)["content"]
        == "NEEDLE"
    )


def test_store_rejects_unknown_handles_and_detects_tampering(tmp_path) -> None:
    store = ToolResultStore(tmp_path)
    reference = store.store("web_fetch", "complete source")

    assert reference is not None
    with pytest.raises(ValueError, match="unknown source handle"):
        store.read("source-missing", offset=0, length=1)

    (tmp_path / "tool-results" / f"{reference.handle}.json").write_text(
        "modified", encoding="utf-8"
    )
    with pytest.raises(RuntimeError, match="integrity check"):
        store.read(reference.handle, offset=0, length=1)


def test_store_recovers_existing_handles_after_a_worker_process_restart(tmp_path) -> None:
    first_store = ToolResultStore(tmp_path)
    reference = first_store.store("web_fetch", "complete saved source")

    assert reference is not None
    restarted_store = ToolResultStore(tmp_path)

    assert (
        restarted_store.read(reference.handle, offset=0, length=21)["content"]
        == "complete saved source"
    )


def test_search_stops_scanning_after_the_requested_match_limit(tmp_path, monkeypatch) -> None:
    store = ToolResultStore(tmp_path)
    reference = store.store("web_fetch", "needle " * 100)
    yielded_matches = 0
    original_finditer = re.finditer

    def counting_finditer(*args, **kwargs):
        nonlocal yielded_matches
        for match in original_finditer(*args, **kwargs):
            yielded_matches += 1
            yield match

    monkeypatch.setattr("octopal.runtime.tool_result_store.re.finditer", counting_finditer)

    assert reference is not None
    result = store.search(reference.handle, query="needle", max_matches=2)

    assert len(result["matches"]) == 2
    assert yielded_matches == 2
