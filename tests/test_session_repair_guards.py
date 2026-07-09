from __future__ import annotations

import json
from pathlib import Path

from octopal.infrastructure.jsonl import read_jsonl_dicts
from octopal.runtime.workers.runtime import _repair_worker_result_payload


def test_jsonl_guard_repairs_malformed_lines(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(
        '{"id":"ok-1","status":"pending"}\n'
        "{not-json}\n"
        "42\n"
        '{"id":"ok-2","status":"done"}\n',
        encoding="utf-8",
    )

    rows, report = read_jsonl_dicts(path, repair=True)

    assert report.repaired is True
    assert report.dropped_lines == 2
    assert len(rows) == 2
    rewritten = path.read_text(encoding="utf-8").strip().splitlines()
    assert [json.loads(line)["id"] for line in rewritten] == ["ok-1", "ok-2"]
    assert report.backup_path is not None
    assert Path(report.backup_path).exists()


def test_worker_result_payload_repair_normalizes_invalid_shapes() -> None:
    class Unserializable:
        def __repr__(self) -> str:
            return "<Unserializable>"

    repaired = _repair_worker_result_payload(
        {
            "summary": "",
            "output": {"bad": Unserializable()},
            "questions": [1, "   ", "ok"],
            "tools_used": ["read", 42, ""],
            "thinking_steps": -5,
        }
    )

    assert repaired["summary"] == "Worker completed"
    assert isinstance(repaired["output"], dict)
    assert repaired["output"].get("repr")
    assert repaired["questions"] == ["1", "ok"]
    assert repaired["tools_used"] == ["read", "42"]
    assert repaired["thinking_steps"] == 0
