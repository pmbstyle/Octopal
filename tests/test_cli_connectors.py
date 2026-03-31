from __future__ import annotations

import json

from typer.testing import CliRunner

from octopal.cli.main import app

runner = CliRunner()


def test_connector_status_json_reports_google_needs_auth(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "connectors": {
                    "instances": {
                        "google": {
                            "enabled": True,
                            "settings": {
                                "enabled_services": ["gmail"],
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
                            "settings": {
                                "enabled_services": ["gmail"],
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
