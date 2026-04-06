from __future__ import annotations

import asyncio
import json

from octopal.infrastructure.config.models import ConnectorInstanceConfig, OctopalConfig
from octopal.infrastructure.connectors.manager import ConnectorManager
from octopal.runtime.octo.router import _budget_tool_specs
from octopal.tools.catalog import get_tools
from octopal.tools.connectors.calendar import get_calendar_connector_tools
from octopal.tools.connectors.drive import get_drive_connector_tools
from octopal.tools.connectors.gmail import get_gmail_connector_tools
from octopal.tools.connectors.github import get_github_connector_tools
from octopal.tools.connectors.status import connector_status_read
from octopal.tools.registry import ToolSpec


def test_catalog_includes_read_only_connector_status_tool() -> None:
    tools = get_tools(mcp_manager=None)
    names = {tool.name for tool in tools}

    assert "connector_status" in names
    assert "send_file_to_user" in names


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


def test_octo_budget_keeps_system_baseline_tools() -> None:
    class _Manager:
        def get_all_tools(self):
            return [
                ToolSpec(
                    name=f"mcp_demo_tool_{index}",
                    description="demo",
                    parameters={"type": "object"},
                    permission="mcp_exec",
                    handler=lambda _args, _ctx: "ok",
                    is_async=True,
                )
                for index in range(40)
            ]

    tools = get_tools(mcp_manager=_Manager())
    active = _budget_tool_specs(tools, max_count=64)
    names = {tool.name for tool in active}

    assert "octo_context_health" in names
    assert "check_schedule" in names
    assert "tool_catalog_search" in names
    assert "list_workers" in names
    assert "start_worker" in names
    assert "get_worker_status" in names
    assert "list_active_workers" in names
    assert "get_worker_result" in names
    assert "stop_worker" in names


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
    assert "drive_update_file_content" in names
    assert "drive_create_text_file" in names
    assert "drive_update_text_file" in names
    assert "drive_read_text_file" in names
    assert "drive_list_children" in names
    assert "drive_trash_file" in names
    assert "drive_download_to_workspace" in names
    assert "drive_upload_from_workspace" in names
    assert "drive_upload_and_get_link" in names
    assert "drive_update_from_workspace" in names


def test_catalog_includes_first_class_github_tools_when_mcp_manager_is_present() -> None:
    class _Manager:
        def get_all_tools(self):
            return []

    tools = get_tools(mcp_manager=_Manager())
    names = {tool.name for tool in tools}

    assert "github_list_repositories" in names
    assert "github_get_repository" in names
    assert "github_list_issues" in names
    assert "github_list_pull_requests" in names


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


def test_drive_list_children_tool_proxies_and_parses_json_payload() -> None:
    class _Text:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Result:
        def __init__(self, text: str) -> None:
            self.content = [_Text(text)]

    class _Manager:
        async def call_tool(self, server_id, tool_name, args, allow_name_fallback=False):
            assert server_id == "google-drive"
            assert tool_name == "list_children"
            assert args == {"parent_id": "folder-1", "page_size": 2}
            assert allow_name_fallback is True
            return _Result('{"files":[{"id":"file-2","name":"child.txt"}]}')

    tools = {tool.name: tool for tool in get_drive_connector_tools(_Manager())}
    payload = asyncio.run(
        tools["drive_list_children"].handler({"parent_id": "folder-1", "page_size": 2}, {})
    )

    assert payload["files"][0]["name"] == "child.txt"


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


def test_drive_upload_and_get_link_returns_view_link(tmp_path) -> None:
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
            return _Result(
                '{"id":"drive-file-3","name":"notes.txt","web_view_link":"https://drive.google.com/file/d/drive-file-3/view"}'
            )

    tools = {tool.name: tool for tool in get_drive_connector_tools(_Manager())}
    payload = asyncio.run(
        tools["drive_upload_and_get_link"].handler(
            {"path": "notes.txt"},
            {"base_dir": tmp_path},
        )
    )

    assert payload["ok"] is True
    assert payload["file_id"] == "drive-file-3"
    assert payload["web_view_link"] == "https://drive.google.com/file/d/drive-file-3/view"


def test_drive_update_from_workspace_reads_file(tmp_path) -> None:
    update_file = tmp_path / "notes.txt"
    update_file.write_text("updated", encoding="utf-8")

    class _Text:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Result:
        def __init__(self, text: str) -> None:
            self.content = [_Text(text)]

    class _Manager:
        async def call_tool(self, server_id, tool_name, args, allow_name_fallback=False):
            assert server_id == "google-drive"
            assert tool_name == "update_file"
            assert args["file_id"] == "drive-file-1"
            assert args["content_base64"] == "dXBkYXRlZA=="
            assert allow_name_fallback is True
            return _Result('{"id":"drive-file-1","name":"notes.txt","modified_time":"2026-04-02T12:00:00Z"}')

    tools = {tool.name: tool for tool in get_drive_connector_tools(_Manager())}
    payload = asyncio.run(
        tools["drive_update_from_workspace"].handler(
            {"file_id": "drive-file-1", "path": "notes.txt"},
            {"base_dir": tmp_path},
        )
    )

    assert payload["id"] == "drive-file-1"
    assert payload["updated_from"] == "notes.txt"


def test_drive_create_text_file_encodes_plain_text() -> None:
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
            assert args["name"] == "notes.md"
            assert args["content_base64"] == "IyBIZWxsbw=="
            assert args["mime_type"] == "text/markdown"
            assert allow_name_fallback is True
            return _Result('{"id":"drive-file-2","name":"notes.md"}')

    tools = {tool.name: tool for tool in get_drive_connector_tools(_Manager())}
    payload = asyncio.run(
        tools["drive_create_text_file"].handler(
            {"name": "notes.md", "content": "# Hello", "mime_type": "text/markdown"},
            {},
        )
    )

    assert payload["id"] == "drive-file-2"
    assert payload["text_length"] == 7


def test_drive_update_text_file_encodes_plain_text() -> None:
    class _Text:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Result:
        def __init__(self, text: str) -> None:
            self.content = [_Text(text)]

    class _Manager:
        async def call_tool(self, server_id, tool_name, args, allow_name_fallback=False):
            assert server_id == "google-drive"
            assert tool_name == "update_file"
            assert args["file_id"] == "drive-file-2"
            assert args["content_base64"] == "dXBkYXRlZA=="
            assert allow_name_fallback is True
            return _Result('{"id":"drive-file-2","name":"notes.md"}')

    tools = {tool.name: tool for tool in get_drive_connector_tools(_Manager())}
    payload = asyncio.run(
        tools["drive_update_text_file"].handler(
            {"file_id": "drive-file-2", "content": "updated"},
            {},
        )
    )

    assert payload["id"] == "drive-file-2"
    assert payload["text_length"] == 7


def test_drive_read_text_file_decodes_plain_text() -> None:
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
            assert args == {"file_id": "drive-file-2"}
            assert allow_name_fallback is True
            return _Result('{"file":{"id":"drive-file-2","name":"notes.md"},"content_base64":"IyBIZWxsbw=="}')

    tools = {tool.name: tool for tool in get_drive_connector_tools(_Manager())}
    payload = asyncio.run(
        tools["drive_read_text_file"].handler(
            {"file_id": "drive-file-2"},
            {},
        )
    )

    assert payload["ok"] is True
    assert payload["content"] == "# Hello"


def test_github_connector_tool_proxies_and_parses_json_payload() -> None:
    class _Text:
        def __init__(self, text: str) -> None:
            self.text = text

    class _Result:
        def __init__(self, text: str) -> None:
            self.content = [_Text(text)]

    class _Manager:
        async def call_tool(self, server_id, tool_name, args, allow_name_fallback=False):
            assert server_id == "github-core"
            assert tool_name == "list_repositories"
            assert args == {"per_page": 1}
            assert allow_name_fallback is True
            return _Result('{"repositories":[{"id":1,"full_name":"octo/demo"}]}')

    tools = {tool.name: tool for tool in get_github_connector_tools(_Manager())}
    payload = asyncio.run(tools["github_list_repositories"].handler({"per_page": 1}, {}))

    assert payload["repositories"][0]["full_name"] == "octo/demo"
