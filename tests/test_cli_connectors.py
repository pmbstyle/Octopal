from __future__ import annotations

import json

from typer.testing import CliRunner

from octopal.cli.main import _connector_next_action, app

runner = CliRunner()


def test_connector_status_json_reports_google_needs_auth(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "connectors": {
                    "instances": {
                        "google": {
                            "enabled": True,
                            "enabled_services": ["gmail"],
                            "credentials": {
                                "client_id": "client-id",
                                "client_secret": "client-secret",
                            },
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["connector", "status", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["connectors"]["google"]["status"] == "needs_auth"


def test_connector_status_human_output_shows_next_action(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "connectors": {
                    "instances": {
                        "google": {
                            "enabled": True,
                            "enabled_services": ["gmail"],
                            "credentials": {
                                "client_id": "client-id",
                                "client_secret": "client-secret",
                            },
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["connector", "status"])

    assert result.exit_code == 0
    assert "CLI authorization" in result.stdout


def test_connector_next_action_maps_needs_auth_to_auth_command() -> None:
    action = _connector_next_action(
        "google",
        {"status": "needs_auth"},
    )

    assert action == "run `octopal connector auth google`"


def test_connector_auth_requires_enabled_connector(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(json.dumps({}), encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["connector", "auth", "google"])

    assert result.exit_code == 1
    assert "Run octopal configure first" in result.stdout or "Run `octopal configure` first" in result.stdout


def test_connector_auth_success_uses_cli_flow(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "connectors": {
                    "instances": {
                        "google": {
                            "enabled": True,
                            "enabled_services": ["gmail"],
                            "credentials": {
                                "client_id": "client-id",
                                "client_secret": "client-secret",
                            },
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    async def fake_authorize(self):
        return {"status": "success", "message": "Google connector authorized for gmail."}

    monkeypatch.setattr(
        "octopal.infrastructure.connectors.google.GoogleConnector.authorize",
        fake_authorize,
    )

    result = runner.invoke(app, ["connector", "auth", "google"])

    assert result.exit_code == 0
    assert "authorized for gmail" in result.stdout


def test_connector_disconnect_clears_auth_state(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "connectors": {
                    "instances": {
                        "google": {
                            "enabled": True,
                            "enabled_services": ["gmail"],
                            "credentials": {
                                "client_id": "client-id",
                                "client_secret": "client-secret",
                            },
                            "auth": {
                                "authorized_services": ["gmail"],
                                "refresh_token": "refresh-token",
                                "access_token": "access-token",
                            },
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["connector", "disconnect", "google"])

    assert result.exit_code == 0
    assert "disconnected" in result.stdout

    payload = json.loads(config_path.read_text(encoding="utf-8"))
    instance = payload["connectors"]["instances"]["google"]
    assert instance["auth"]["refresh_token"] is None
    assert instance["auth"]["access_token"] is None
    assert instance["auth"]["authorized_services"] == []
    assert instance["credentials"]["client_id"] == "client-id"
