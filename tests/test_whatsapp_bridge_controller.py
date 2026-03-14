from broodmind.channels.whatsapp.bridge import WhatsAppBridgeController, WhatsAppBridgeError


def test_parse_node_major_accepts_v_prefix() -> None:
    assert WhatsAppBridgeController._parse_node_major("v22.15.0") == 22


def test_parse_node_major_accepts_plain_number() -> None:
    assert WhatsAppBridgeController._parse_node_major("20.19.1") == 20


def test_parse_node_major_returns_none_for_invalid_text() -> None:
    assert WhatsAppBridgeController._parse_node_major("not-a-version") is None


def test_require_supported_node_rejects_old_versions(monkeypatch) -> None:
    monkeypatch.setattr(WhatsAppBridgeController, "_node_version", staticmethod(lambda _: "v18.19.1"))

    try:
        WhatsAppBridgeController._require_supported_node("node")
    except WhatsAppBridgeError as exc:
        assert "Node.js 20 or newer is required" in str(exc)
    else:
        raise AssertionError("expected WhatsAppBridgeError for unsupported Node.js version")


def test_require_supported_node_accepts_supported_versions(monkeypatch) -> None:
    monkeypatch.setattr(WhatsAppBridgeController, "_node_version", staticmethod(lambda _: "v22.15.0"))

    WhatsAppBridgeController._require_supported_node("node")
