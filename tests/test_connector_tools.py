from __future__ import annotations

import asyncio
import json

from octopal.infrastructure.config.models import ConnectorInstanceConfig, OctopalConfig
from octopal.infrastructure.connectors.manager import ConnectorManager
from octopal.tools.catalog import get_tools
from octopal.tools.connectors.calendar import get_calendar_connector_tools
from octopal.tools.connectors.drive import get_drive_connector_tools
from octopal.tools.connectors.gmail import get_gmail_connector_tools
from octopal.tools.connectors.status import connector_status_read


def test_catalog_includes_read_only_connector_status_tool() -> None:
    tools = get_tools(mcp_manager=None)
    names = {tool.name for tool in tools}

    assert "connector_status" in names


def test_connector_status_tool_reads_status_from_octo_context() -> None:
    config = OctopalConfig()
    config.connectors.instances["google"] = ConnectorInstanceConfig(
        enabled=True,
        enabled_services=["gmail"],
        credentials={"client_id": "client-id", "client_secret": "client-secret"},
    )
    manager = ConnectorManager(config=config.connectors, mcp_manager=None, octo_config=config)

    class _Octo:
        connector_manager = manager

    payload = asyncio.run(connector_status_read({}, {"octo": _Octo()}))
    data = json.loads(payload)

    assert data["connectors"]["google"]["status"] == "needs_auth"


def test_connector_status_tool_can_filter_to_single_connector() -> None:
    config = OctopalConfig()
    config.connectors.instances["google"] = ConnectorInstanceConfig(enabled=False)
    manager = ConnectorManager(config=config.connectors, mcp_manager=None, octo_config=config)

    class _Octo:
        connector_manager = manager

    payload = asyncio.run(connector_status_read({"name": "google"}, {"octo": _Octo()}))
    data = json.loads(payload)

    assert set(data["connectors"]) == {"google"}
    assert data["connectors"]["google"]["status"] == "disabled"


def test_catalog_includes_first_class_gmail_tools_when_mcp_manager_is_present() -> None:
    class _Manager:
        def get_all_tools(self):
            return []

    tools = get_tools(mcp_manager=_Manager())
    names = {tool.name for tool in tools}

    assert "gmail_list_messages" in names
    assert "gmail_search_messages" in names
    assert "gmail_get_message" in names


def test_catalog_includes_first_class_calendar_tools_when_mcp_manager_is_present() -> None:
    class _Manager:
        def get_all_tools(self):
            return []

    tools = get_tools(mcp_manager=_Manager())
    names = {tool.name for tool in tools}

    assert "calendar_list_calendars" in names
    assert "calendar_list_events" in names
    assert "calendar_create_event" in names
    assert "calendar_update_event" in names
    assert "calendar_delete_event" in names
    assert "calendar_freebusy" in names


def test_catalog_includes_first_class_drive_tools_when_mcp_manager_is_present() -> None:
    class _Manager:
        def get_all_tools(self):
            return []

    tools = get_tools(mcp_manager=_Manager())
    names = {tool.name for tool in tools}

    assert "drive_list_files" in names
    assert "drive_get_file" in names
    assert "drive_upload_file_content" in names
    assert "drive_download_to_workspace" in names
    assert "drive_upload_from_workspace" in names


def test_gmail_connector_tool_proxies_and_parses_json_payload() -> None:
    class _Text:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Result:
        def __init__(self, text: str) -> None:
            self.content = [_Text(text)]

    class _Manager:
        async def call_tool(self, server_id, tool_name, args, allow_name_fallback=False):
            assert server_id == "google-gmail"
            assert tool_name == "list_messages"
            assert args == {"max_results": 1}
            assert allow_name_fallback is True
            return _Result('{"messages":[{"id":"msg-1"}],"result_size_estimate":1}')

    tools = {tool.name: tool for tool in get_gmail_connector_tools(_Manager())}
    payload = asyncio.run(tools["gmail_list_messages"].handler({"max_results": 1}, {}))

    assert payload["messages"][0]["id"] == "msg-1"
    assert payload["result_size_estimate"] == 1


def test_calendar_connector_tool_proxies_and_parses_json_payload() -> None:
    class _Text:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Result:
        def __init__(self, text: str) -> None:
            self.content = [_Text(text)]

    class _Manager:
        async def call_tool(self, server_id, tool_name, args, allow_name_fallback=False):
            assert server_id == "google-calendar"
            assert tool_name == "list_events"
            assert args == {"calendar_id": "primary", "max_results": 1}
            assert allow_name_fallback is True
            return _Result('{"events":[{"id":"evt-1"}],"calendar_id":"primary"}')

    tools = {tool.name: tool for tool in get_calendar_connector_tools(_Manager())}
    payload = asyncio.run(
        tools["calendar_list_events"].handler({"calendar_id": "primary", "max_results": 1}, {})
    )

    assert payload["events"][0]["id"] == "evt-1"
    assert payload["calendar_id"] == "primary"


def test_calendar_freebusy_tool_proxies_and_parses_json_payload() -> None:
    class _Text:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Result:
        def __init__(self, text: str) -> None:
            self.content = [_Text(text)]

    class _Manager:
        async def call_tool(self, server_id, tool_name, args, allow_name_fallback=False):
            assert server_id == "google-calendar"
            assert tool_name == "freebusy"
            assert args == {
                "calendar_ids": ["primary"],
                "time_min": "2026-04-02T00:00:00Z",
                "time_max": "2026-04-03T00:00:00Z",
            }
            assert allow_name_fallback is True
            return _Result(
                '{"time_min":"2026-04-02T00:00:00Z","time_max":"2026-04-03T00:00:00Z","calendars":{"primary":{"busy":[{"start":"2026-04-02T14:00:00Z","end":"2026-04-02T15:00:00Z"}],"errors":[]}}}'
            )

    tools = {tool.name: tool for tool in get_calendar_connector_tools(_Manager())}
    payload = asyncio.run(
        tools["calendar_freebusy"].handler(
            {
                "calendar_ids": ["primary"],
                "time_min": "2026-04-02T00:00:00Z",
                "time_max": "2026-04-03T00:00:00Z",
            },
            {},
        )
    )

    assert payload["calendars"]["primary"]["busy"][0]["start"] == "2026-04-02T14:00:00Z"


def test_drive_connector_tool_proxies_and_parses_json_payload() -> None:
    class _Text:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Result:
        def __init__(self, text: str) -> None:
            self.content = [_Text(text)]

    class _Manager:
        async def call_tool(self, server_id, tool_name, args, allow_name_fallback=False):
            assert server_id == "google-drive"
            assert tool_name == "list_files"
            assert args == {"page_size": 1}
            assert allow_name_fallback is True
            return _Result('{"files":[{"id":"file-1","name":"spec.md"}]}')

    tools = {tool.name: tool for tool in get_drive_connector_tools(_Manager())}
    payload = asyncio.run(tools["drive_list_files"].handler({"page_size": 1}, {}))

    assert payload["files"][0]["id"] == "file-1"


def test_drive_download_to_workspace_writes_file(tmp_path) -> None:
    class _Text:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Result:
        def __init__(self, text: str) -> None:
            self.content = [_Text(text)]

    class _Manager:
        async def call_tool(self, server_id, tool_name, args, allow_name_fallback=False):
            assert server_id == "google-drive"
            assert tool_name == "download_file"
            assert args == {"file_id": "file-1"}
            assert allow_name_fallback is True
            return _Result('{"file":{"id":"file-1","name":"spec.md"},"content_base64":"aGVsbG8=","size":5}')

    tools = {tool.name: tool for tool in get_drive_connector_tools(_Manager())}
    payload = asyncio.run(
        tools["drive_download_to_workspace"].handler(
            {"file_id": "file-1", "path": "downloads/spec.md"},
            {"base_dir": tmp_path},
        )
    )

    assert payload["ok"] is True
    assert (tmp_path / "downloads" / "spec.md").read_text(encoding="utf-8") == "hello"


def test_drive_upload_from_workspace_reads_file(tmp_path) -> None:
    upload_file = tmp_path / "notes.txt"
    upload_file.write_text("hello", encoding="utf-8")

    class _Text:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Result:
        def __init__(self, text: str) -> None:
            self.content = [_Text(text)]

    class _Manager:
        async def call_tool(self, server_id, tool_name, args, allow_name_fallback=False):
            assert server_id == "google-drive"
            assert tool_name == "upload_file"
            assert args["name"] == "notes.txt"
            assert args["content_base64"] == "aGVsbG8="
            assert allow_name_fallback is True
            return _Result('{"id":"drive-file-1","name":"notes.txt"}')

    tools = {tool.name: tool for tool in get_drive_connector_tools(_Manager())}
    payload = asyncio.run(
        tools["drive_upload_from_workspace"].handler(
            {"path": "notes.txt"},
            {"base_dir": tmp_path},
        )
    )

    assert payload["id"] == "drive-file-1"
    assert payload["uploaded_from"] == "notes.txt"
