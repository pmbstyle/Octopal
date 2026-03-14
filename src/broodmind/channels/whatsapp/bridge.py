from __future__ import annotations

import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx

from broodmind.config.settings import Settings


class WhatsAppBridgeError(RuntimeError):
    pass


class WhatsAppBridgeController:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._process: subprocess.Popen[str] | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.settings.whatsapp_bridge_host}:{self.settings.whatsapp_bridge_port}"

    @property
    def project_root(self) -> Path:
        return Path(__file__).resolve().parents[3]

    @property
    def bridge_dir(self) -> Path:
        return self.project_root / "scripts" / "whatsapp_bridge"

    @property
    def auth_dir(self) -> Path:
        if self.settings.whatsapp_auth_dir is not None:
            auth_dir = Path(self.settings.whatsapp_auth_dir)
        else:
            auth_dir = self.settings.state_dir / "whatsapp-auth"
        if not auth_dir.is_absolute():
            auth_dir = self.project_root / auth_dir
        return auth_dir

    def bridge_installed(self) -> bool:
        return (self.bridge_dir / "node_modules" / "@whiskeysockets" / "baileys" / "package.json").is_file()

    def install_bridge(self) -> None:
        npm = self._find_command(("npm.cmd", "npm"))
        if npm is None:
            raise WhatsAppBridgeError("npm is required to install the WhatsApp bridge dependencies.")
        node = self._find_command((self.settings.whatsapp_node_command, "node"))
        self._require_supported_node(node)
        subprocess.run([npm, "install"], cwd=str(self.bridge_dir), check=True)

    def start(self, *, callback_url: str | None = None) -> None:
        if self._process and self._process.poll() is None:
            return
        if not self.bridge_installed():
            raise WhatsAppBridgeError(
                "WhatsApp bridge dependencies are not installed. Run `broodmind whatsapp install-bridge` first."
            )
        node = self._find_command((self.settings.whatsapp_node_command, "node"))
        if node is None:
            raise WhatsAppBridgeError("Node.js is required to run the WhatsApp bridge.")
        self._require_supported_node(node)

        self.auth_dir.mkdir(parents=True, exist_ok=True)
        log_dir = self.settings.state_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = log_dir / "whatsapp-bridge.stdout.log"
        stderr_path = log_dir / "whatsapp-bridge.stderr.log"

        env = os.environ.copy()
        env.update(
            {
                "BROODMIND_WHATSAPP_BRIDGE_HOST": self.settings.whatsapp_bridge_host,
                "BROODMIND_WHATSAPP_BRIDGE_PORT": str(self.settings.whatsapp_bridge_port),
                "BROODMIND_WHATSAPP_AUTH_DIR": str(self.auth_dir),
                "BROODMIND_WHATSAPP_CALLBACK_URL": callback_url or "",
                "BROODMIND_WHATSAPP_CALLBACK_TOKEN": self.settings.whatsapp_callback_token,
            }
        )

        with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
            "w", encoding="utf-8"
        ) as stderr_handle:
            self._process = subprocess.Popen(
                [node, "bridge.mjs"],
                cwd=str(self.bridge_dir),
                env=env,
                stdout=stdout_handle,
                stderr=stderr_handle,
                stdin=subprocess.DEVNULL,
                text=True,
            )
        self.wait_until_ready()

    def stop(self) -> None:
        if not self._process:
            return
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._process.kill()
        self._process = None

    def wait_until_ready(self, timeout_seconds: float = 20.0) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error = ""
        while time.monotonic() < deadline:
            try:
                status = self.status()
                if status:
                    return
            except Exception as exc:
                last_error = str(exc)
            time.sleep(0.5)
        raise WhatsAppBridgeError(f"WhatsApp bridge did not become ready: {last_error or 'timeout'}")

    def status(self) -> dict[str, Any]:
        return self._request("GET", "/status")

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health")

    def qr(self) -> dict[str, Any]:
        return self._request("GET", "/qr")

    def qr_terminal(self) -> dict[str, Any]:
        return self._request("GET", "/qr-terminal")

    def send_message(self, to: str, text: str) -> dict[str, Any]:
        return self._request("POST", "/send", json={"to": to, "text": text})

    def logout(self) -> dict[str, Any]:
        return self._request("POST", "/logout")

    def _request(self, method: str, path: str, json: dict[str, Any] | None = None) -> dict[str, Any]:
        with httpx.Client(timeout=10.0) as client:
            response = client.request(method, f"{self.base_url}{path}", json=json)
            response.raise_for_status()
            payload = response.json()
        if not isinstance(payload, dict):
            raise WhatsAppBridgeError(f"Unexpected WhatsApp bridge response for {path}.")
        return payload

    @staticmethod
    def _find_command(candidates: tuple[str, ...]) -> str | None:
        import shutil

        for candidate in candidates:
            found = shutil.which(candidate)
            if found:
                return found
        return None

    @staticmethod
    def _require_supported_node(node_command: str | None) -> None:
        if node_command is None:
            raise WhatsAppBridgeError("Node.js 20 or newer is required to run the WhatsApp bridge.")
        version = WhatsAppBridgeController._node_version(node_command)
        major = WhatsAppBridgeController._parse_node_major(version)
        if major is None:
            raise WhatsAppBridgeError(
                f"Could not determine Node.js version from `{node_command}`. Node.js 20 or newer is required."
            )
        if major < 20:
            raise WhatsAppBridgeError(
                f"Node.js 20 or newer is required for the WhatsApp bridge. Found {version or 'unknown version'}."
            )

    @staticmethod
    def _node_version(node_command: str) -> str:
        try:
            completed = subprocess.run(
                [node_command, "--version"],
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise WhatsAppBridgeError(
                f"Failed to run `{node_command} --version`. Node.js 20 or newer is required."
            ) from exc
        return (completed.stdout or completed.stderr or "").strip()

    @staticmethod
    def _parse_node_major(version_text: str) -> int | None:
        match = re.search(r"v?(?P<major>\d+)", version_text.strip())
        if not match:
            return None
        return int(match.group("major"))
