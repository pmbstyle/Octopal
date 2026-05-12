from __future__ import annotations

import asyncio

from octopal.tools.memory.canon import manage_canon


class DummyCanon:
    def __init__(self) -> None:
        self.writes: list[tuple[str, str, str]] = []

    def list_files(self) -> list[str]:
        return ["facts.md", "decisions.md", "failures.md", "weather.md"]

    def read_canon(self, filename: str) -> str:
        return f"read:{filename}"

    async def write_canon(self, filename: str, content: str, mode: str) -> str:
        self.writes.append((filename, content, mode))
        return "Success"


class DummyOcto:
    def __init__(self) -> None:
        self.canon = DummyCanon()


def test_manage_canon_list_only_exposes_supported_files() -> None:
    octo = DummyOcto()

    result = asyncio.run(manage_canon({"action": "list"}, {"octo": octo}))

    assert result == "Canonical Files: facts.md, decisions.md, failures.md"


def test_manage_canon_rejects_non_canon_weather_artifacts() -> None:
    octo = DummyOcto()

    result = asyncio.run(
        manage_canon(
            {
                "action": "write",
                "filename": "weather.md",
                "content": "# Weather",
                "mode": "overwrite",
            },
            {"octo": octo},
        )
    )

    assert result == "Error: manage_canon only supports facts.md, decisions.md, and failures.md."
    assert octo.canon.writes == []


def test_manage_canon_still_writes_supported_files() -> None:
    octo = DummyOcto()

    result = asyncio.run(
        manage_canon(
            {
                "action": "write",
                "filename": "facts",
                "content": "# Facts",
                "mode": "overwrite",
            },
            {"octo": octo},
        )
    )

    assert result == "Success"
    assert octo.canon.writes == [("facts.md", "# Facts", "overwrite")]
