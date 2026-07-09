from __future__ import annotations

import subprocess

from typer.testing import CliRunner

from octopal.cli.main import _git_checkout_ready_for_update, _perform_git_update, app

runner = CliRunner()


def test_update_rejects_dirty_git_checkout(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("octopal.cli.main._project_root", lambda: tmp_path)
    monkeypatch.setattr(
        "octopal.cli.main._git_checkout_ready_for_update", lambda _root: (False, "dirty tree")
    )

    result = runner.invoke(app, ["update"])

    assert result.exit_code == 1
    assert "Update unavailable:" in result.stdout
    assert "dirty tree" in result.stdout


def test_update_runs_git_pull_and_uv_sync(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("octopal.cli.main._project_root", lambda: tmp_path)
    monkeypatch.setattr(
        "octopal.cli.main._git_checkout_ready_for_update", lambda _root: (True, None)
    )
    monkeypatch.setattr("octopal.cli.main.list_octopal_runtime_pids", lambda: [])
    monkeypatch.setattr(
        "octopal.cli.main._perform_git_update",
        lambda _root: (True, "Already up to date."),
    )

    result = runner.invoke(app, ["update"])

    assert result.exit_code == 0
    assert "Octopal updated." in result.stdout
    assert "Already up to date." in result.stdout
    assert "uv run octopal start" in result.stdout


def test_update_warns_when_runtime_is_active(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("octopal.cli.main._project_root", lambda: tmp_path)
    monkeypatch.setattr(
        "octopal.cli.main._git_checkout_ready_for_update", lambda _root: (True, None)
    )
    monkeypatch.setattr("octopal.cli.main.list_octopal_runtime_pids", lambda: [12345])
    monkeypatch.setattr(
        "octopal.cli.main._perform_git_update",
        lambda _root: (True, "Updating abc..def"),
    )

    result = runner.invoke(app, ["update"])

    assert result.exit_code == 0
    assert "Octopal is running right now." in result.stdout
    assert "uv run octopal restart" in result.stdout


def test_perform_git_update_pulls_tracking_checkout(monkeypatch, tmp_path) -> None:
    calls: list[list[str]] = []

    def fake_run_capture(command: list[str], *, cwd, timeout=10.0):
        calls.append(command)
        if command == ["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"]:
            return subprocess.CompletedProcess(command, 0, stdout="origin/main\n", stderr="")
        if command == ["git", "pull", "--ff-only"]:
            return subprocess.CompletedProcess(command, 0, stdout="Updating abc..def\n", stderr="")
        if command == ["uv", "sync"]:
            return subprocess.CompletedProcess(command, 0, stdout="synced", stderr="")
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(
        "octopal.cli.main.shutil.which", lambda name: "/usr/bin/uv" if name == "uv" else None
    )
    monkeypatch.setattr("octopal.cli.main._run_capture", fake_run_capture)

    ok, detail = _perform_git_update(tmp_path)

    assert ok is True
    assert detail == "Updating abc..def"
    assert ["git", "fetch", "--tags", "--force", "origin"] not in calls


def test_perform_git_update_checks_out_latest_release_tag_without_upstream(
    monkeypatch, tmp_path
) -> None:
    calls: list[list[str]] = []

    def fake_run_capture(command: list[str], *, cwd, timeout=10.0):
        calls.append(command)
        if command == ["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"]:
            return subprocess.CompletedProcess(command, 128, stdout="", stderr="no upstream")
        if command == ["git", "fetch", "--tags", "--force", "origin"]:
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command == ["git", "tag", "--list", "v[0-9]*"]:
            return subprocess.CompletedProcess(
                command,
                0,
                stdout="v2026.04.14\nv2026.05.03\nv2026.05.03.1\nnot-a-release\n",
                stderr="",
            )
        if command == ["git", "checkout", "--detach", "v2026.05.03.1"]:
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command == ["uv", "sync"]:
            return subprocess.CompletedProcess(command, 0, stdout="synced", stderr="")
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(
        "octopal.cli.main.shutil.which", lambda name: "/usr/bin/uv" if name == "uv" else None
    )
    monkeypatch.setattr("octopal.cli.main._run_capture", fake_run_capture)

    ok, detail = _perform_git_update(tmp_path)

    assert ok is True
    assert detail == "Checked out release tag v2026.05.03.1."
    assert ["git", "pull", "--ff-only"] not in calls


def test_git_checkout_ready_allows_mode_only_changes(monkeypatch, tmp_path) -> None:
    (tmp_path / ".git").mkdir()

    def fake_run_capture(command: list[str], *, cwd, timeout=10.0):
        if command == ["git", "status", "--porcelain"]:
            return subprocess.CompletedProcess(
                command, 0, stdout=" M scripts/bootstrap.sh\n", stderr=""
            )
        if command == ["git", "diff", "--name-only"]:
            return subprocess.CompletedProcess(
                command, 0, stdout="scripts/bootstrap.sh\n", stderr=""
            )
        if command == ["git", "diff", "--numstat", "--", "scripts/bootstrap.sh"]:
            return subprocess.CompletedProcess(
                command, 0, stdout="0\t0\tscripts/bootstrap.sh\n", stderr=""
            )
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("octopal.cli.main.shutil.which", lambda _name: "/usr/bin/git")
    monkeypatch.setattr("octopal.cli.main._run_capture", fake_run_capture)

    assert _git_checkout_ready_for_update(tmp_path) == (True, None)


def test_git_checkout_ready_blocks_real_content_changes(monkeypatch, tmp_path) -> None:
    (tmp_path / ".git").mkdir()

    def fake_run_capture(command: list[str], *, cwd, timeout=10.0):
        if command == ["git", "status", "--porcelain"]:
            return subprocess.CompletedProcess(
                command, 0, stdout=" M scripts/bootstrap.sh\n", stderr=""
            )
        if command == ["git", "diff", "--name-only"]:
            return subprocess.CompletedProcess(
                command, 0, stdout="scripts/bootstrap.sh\n", stderr=""
            )
        if command == ["git", "diff", "--numstat", "--", "scripts/bootstrap.sh"]:
            return subprocess.CompletedProcess(
                command, 0, stdout="3\t1\tscripts/bootstrap.sh\n", stderr=""
            )
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr("octopal.cli.main.shutil.which", lambda _name: "/usr/bin/git")
    monkeypatch.setattr("octopal.cli.main._run_capture", fake_run_capture)

    ok, reason = _git_checkout_ready_for_update(tmp_path)
    assert ok is False
    assert "scripts/bootstrap.sh" in str(reason)
