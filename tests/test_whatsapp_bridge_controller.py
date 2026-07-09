from pathlib import Path
from types import SimpleNamespace

from octopal.channels.whatsapp.bridge import WhatsAppBridgeController, WhatsAppBridgeError


def test_parse_node_major_accepts_v_prefix() -> None:
    assert WhatsAppBridgeController._parse_node_major("v22.15.0") == 22


def test_parse_node_major_accepts_plain_number() -> None:
    assert WhatsAppBridgeController._parse_node_major("20.19.1") == 20


def test_parse_node_major_returns_none_for_invalid_text() -> None:
    assert WhatsAppBridgeController._parse_node_major("not-a-version") is None


def test_require_supported_node_rejects_old_versions(monkeypatch) -> None:
    monkeypatch.setattr(
        WhatsAppBridgeController, "_node_version", staticmethod(lambda _: "v18.19.1")
    )

    try:
        WhatsAppBridgeController._require_supported_node("node")
    except WhatsAppBridgeError as exc:
        assert "Node.js 20 or newer is required" in str(exc)
    else:
        raise AssertionError("expected WhatsAppBridgeError for unsupported Node.js version")


def test_require_supported_node_accepts_supported_versions(monkeypatch) -> None:
    monkeypatch.setattr(
        WhatsAppBridgeController, "_node_version", staticmethod(lambda _: "v22.15.0")
    )

    WhatsAppBridgeController._require_supported_node("node")


def test_project_root_resolves_repository_root() -> None:
    settings = SimpleNamespace(
        whatsapp_bridge_host="127.0.0.1",
        whatsapp_bridge_port=8765,
        whatsapp_auth_dir=None,
        state_dir=Path("data"),
        whatsapp_node_command="node",
        whatsapp_callback_token="",
    )
    controller = WhatsAppBridgeController(settings)

    assert (controller.project_root / "pyproject.toml").is_file()
    assert controller.bridge_dir == controller.project_root / "scripts" / "whatsapp_bridge"
    assert (controller.bridge_dir / "package.json").is_file()
