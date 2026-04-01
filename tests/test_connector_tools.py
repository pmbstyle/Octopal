from __future__ import annotations

import asyncio
import json

from octopal.infrastructure.config.models import ConnectorInstanceConfig, OctopalConfig
from octopal.infrastructure.connectors.manager import ConnectorManager
from octopal.tools.catalog import get_tools
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
