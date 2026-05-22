from __future__ import annotations

import json
from pathlib import Path


def _worker_prompt(worker_id: str) -> str:
    raw = json.loads(
        (Path("workspace_templates") / "workers" / worker_id / "worker.json").read_text(
            encoding="utf-8"
        )
    )
    return str(raw["system_prompt"])


def test_default_web_worker_prompts_stay_compact_without_losing_guardrails() -> None:
    prompt_limits = {
        "web_search_ranked": 380,
        "web_search_answer": 340,
        "web_researcher": 280,
        "web_fetcher": 240,
    }

    for worker_id, limit in prompt_limits.items():
        prompt = _worker_prompt(worker_id)

        assert len(prompt) < limit
        assert "\n\n" not in prompt

    ranked_prompt = _worker_prompt("web_search_ranked")
    assert "Do not fetch pages" in ranked_prompt
    assert "invent sources/URLs" in ranked_prompt
    assert "sequential ranks starting at 1" in ranked_prompt

    answer_prompt = _worker_prompt("web_search_answer")
    assert "Do not fabricate facts" in answer_prompt
    assert "output.sources" in answer_prompt
