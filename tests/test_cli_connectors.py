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


def test_connector_next_action_maps_github_needs_auth_to_auth_command() -> None:
    action = _connector_next_action(
        "github",
        {"status": "needs_auth"},
    )

    assert action == "run `octopal connector auth github`"


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
    answers = iter(["client-id-from-prompt", "client-secret-from-prompt"])
    monkeypatch.setattr("octopal.cli.main.typer.prompt", lambda *args, **kwargs: next(answers))

    async def fake_authorize(self):
        return {"status": "success", "message": "Google connector authorized for gmail."}

    monkeypatch.setattr(
        "octopal.infrastructure.connectors.google.GoogleConnector.authorize",
        fake_authorize,
    )

    result = runner.invoke(app, ["connector", "auth", "google"])

    assert result.exit_code == 0
    assert "authorized for gmail" in result.stdout


def test_connector_auth_prints_google_setup_help_when_credentials_missing(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "connectors": {
                    "instances": {
                        "google": {
                            "enabled": True,
                            "enabled_services": ["gmail"],
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    answers = iter(["client-id-from-prompt", "client-secret-from-prompt"])
    monkeypatch.setattr("octopal.cli.main.typer.prompt", lambda *args, **kwargs: next(answers))

    async def fake_authorize(self):
        return {"status": "success", "message": "Google connector authorized for gmail."}

    monkeypatch.setattr(
        "octopal.infrastructure.connectors.google.GoogleConnector.authorize",
        fake_authorize,
    )

    result = runner.invoke(app, ["connector", "auth", "google"])

    assert result.exit_code == 0
    assert "Google OAuth setup" in result.stdout
    assert "your own Google OAuth Desktop App credentials" in result.stdout
    assert "Desktop app" in result.stdout
    assert "console.cloud.google.com/apis/credentials" in result.stdout
    assert "docs/google_connector_setup.md" in result.stdout


def test_connector_auth_prompts_for_credentials_even_when_saved(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "connectors": {
                    "instances": {
                        "google": {
                            "enabled": True,
                            "enabled_services": ["gmail"],
                            "credentials": {
                                "client_id": "saved-client-id",
                                "client_secret": "saved-client-secret",
                            },
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    prompts: list[str] = []
    answers = iter(["fresh-client-id", "fresh-client-secret"])

    def fake_prompt(message: str, *args, **kwargs):
        prompts.append(message)
        return next(answers)

    monkeypatch.setattr("octopal.cli.main.typer.prompt", fake_prompt)

    async def fake_authorize(self):
        config = self._get_config()
        assert config.credentials.client_id == "fresh-client-id"
        assert config.credentials.client_secret == "fresh-client-secret"
        return {"status": "success", "message": "Google connector authorized for gmail."}

    monkeypatch.setattr(
        "octopal.infrastructure.connectors.google.GoogleConnector.authorize",
        fake_authorize,
    )

    result = runner.invoke(app, ["connector", "auth", "google"])

    assert result.exit_code == 0
    assert prompts == [
        "Your Google OAuth Desktop App client ID",
        "Your Google OAuth Desktop App client secret",
    ]


def test_connector_auth_falls_back_to_manual_flow_when_browser_is_unavailable(tmp_path, monkeypatch) -> None:
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
    monkeypatch.setattr("octopal.cli.main.typer.prompt", lambda *args, **kwargs: "http://localhost/?code=abc")

    async def fake_authorize(self):
        return {"status": "manual_required", "error": "could not locate runnable browser"}

    async def fake_begin_manual_authorize(self):
        return {"auth_url": "https://accounts.google.com/mock-auth", "redirect_uri": "http://localhost"}

    async def fake_complete_manual_authorize(self, authorization_response: str):
        assert authorization_response == "http://localhost/?code=abc"
        return {"status": "success", "message": "Google connector authorized for gmail."}

    monkeypatch.setattr(
        "octopal.infrastructure.connectors.google.GoogleConnector.authorize",
        fake_authorize,
    )
    monkeypatch.setattr(
        "octopal.infrastructure.connectors.google.GoogleConnector.begin_manual_authorize",
        fake_begin_manual_authorize,
    )
    monkeypatch.setattr(
        "octopal.infrastructure.connectors.google.GoogleConnector.complete_manual_authorize",
        fake_complete_manual_authorize,
    )

    result = runner.invoke(app, ["connector", "auth", "google"])

    assert result.exit_code == 0
    assert "Headless Google authorization" in result.stdout
    assert "accounts.google.com/mock-auth" in result.stdout
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


def test_connector_auth_success_uses_github_cli_flow(tmp_path, monkeypatch) -> None:
    (tmp_path / "config.json").write_text(
        json.dumps(
            {
                "connectors": {
                    "instances": {
                        "github": {
                            "enabled": True,
                            "enabled_services": ["repos"],
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("octopal.cli.main.typer.prompt", lambda *args, **kwargs: "ghp_test")

    async def fake_authorize(self):
        config = self._get_config()
        assert config.auth.access_token == "ghp_test"
        return {"status": "success", "message": "GitHub connector authorized for repos."}

    monkeypatch.setattr(
        "octopal.infrastructure.connectors.github.GitHubConnector.authorize",
        fake_authorize,
    )

    result = runner.invoke(app, ["connector", "auth", "github"])

    assert result.exit_code == 0
    assert "authorized for repos" in result.stdout
