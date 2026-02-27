from __future__ import annotations

import json
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from broodmind.config.settings import Settings
from broodmind.runtime_metrics import read_metrics_snapshot
from broodmind.state import is_pid_running, read_status
from broodmind.store.sqlite import SQLiteStore


def register_dashboard_routes(app: FastAPI) -> None:
    @app.get("/dashboard", response_class=HTMLResponse)
    async def dashboard_page() -> str:
        return _dashboard_html()

    @app.get("/api/dashboard/snapshot")
    async def dashboard_snapshot(
        request: Request,
        last: int = Query(8, ge=1, le=50),
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        store = _get_store(app, settings)
        return _build_snapshot(settings, store, last)

    @app.get("/api/dashboard/logs")
    async def dashboard_logs(
        request: Request,
        lines: int = Query(50, ge=1, le=500),
    ) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        log_path = settings.state_dir / "logs" / "broodmind.log"
        entries: list[dict[str, str]] = []
        for line in _read_last_lines(log_path, max_lines=lines):
            raw = line.strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                entries.append({"event": raw[:200], "level": "info"})
                continue
            if isinstance(data, dict):
                entries.append(
                    {
                        "event": str(data.get("event", ""))[:200],
                        "level": str(data.get("level", "info")),
                        "timestamp": str(data.get("timestamp", "")),
                    }
                )
        return {"count": len(entries), "entries": entries}

    @app.get("/api/dashboard/settings")
    async def dashboard_settings(request: Request) -> dict[str, Any]:
        settings = _get_settings(app)
        _verify_dashboard_token(request, settings)
        return {
            "gateway_host": settings.gateway_host,
            "gateway_port": settings.gateway_port,
            "state_dir": str(settings.state_dir),
            "workspace_dir": str(settings.workspace_dir),
            "log_level": settings.log_level,
            "tailscale_ips_configured": bool(settings.tailscale_ips.strip()),
            "dashboard_token_configured": bool(settings.dashboard_token.strip()),
        }


def _get_settings(app: FastAPI) -> Settings:
    settings = getattr(app.state, "settings", None)
    if not isinstance(settings, Settings):
        raise HTTPException(status_code=500, detail="Settings not initialized")
    return settings


def _get_store(app: FastAPI, settings: Settings) -> SQLiteStore:
    store = getattr(app.state, "dashboard_store", None)
    if isinstance(store, SQLiteStore):
        return store
    store = SQLiteStore(settings)
    app.state.dashboard_store = store
    return store


def _verify_dashboard_token(request: Request, settings: Settings) -> None:
    expected = settings.dashboard_token.strip()
    if not expected:
        return

    header_token = request.headers.get("x-broodmind-token", "").strip()
    auth_header = request.headers.get("authorization", "").strip()
    bearer_token = ""
    if auth_header.lower().startswith("bearer "):
        bearer_token = auth_header[7:].strip()
    query_token = str(request.query_params.get("token", "")).strip()

    provided = header_token or bearer_token or query_token
    if provided != expected:
        raise HTTPException(status_code=401, detail="Invalid dashboard token")


def _build_snapshot(settings: Settings, store: SQLiteStore, last: int) -> dict[str, Any]:
    status_data = read_status(settings) or {}
    pid = status_data.get("pid")
    running = is_pid_running(pid)
    metrics = read_metrics_snapshot(settings.state_dir) or {}
    queen_metrics = metrics.get("queen", {}) if isinstance(metrics, dict) else {}
    telegram_metrics = metrics.get("telegram", {}) if isinstance(metrics, dict) else {}
    exec_metrics = metrics.get("exec_run", {}) if isinstance(metrics, dict) else {}
    connectivity_metrics = metrics.get("connectivity", {}) if isinstance(metrics, dict) else {}

    active_workers = store.get_active_workers(older_than_minutes=5)
    recent_workers = store.list_recent_workers(max(50, last))

    now = _now_utc()
    cutoff = now.timestamp() - 24 * 60 * 60
    spawned_24h = int(store.count_workers_created_since(datetime.fromtimestamp(cutoff, tz=UTC)))

    by_status: dict[str, int] = {}
    for worker in active_workers:
        by_status[worker.status] = by_status.get(worker.status, 0) + 1
    running_nodes = [w for w in active_workers if w.status in {"started", "running"}]
    root_running = sum(1 for w in running_nodes if not w.parent_worker_id)
    subworkers_running = sum(1 for w in running_nodes if bool(w.parent_worker_id))

    followup_q = int(queen_metrics.get("followup_queues", 0) or 0)
    internal_q = int(queen_metrics.get("internal_queues", 0) or 0)
    thinking_count = int(queen_metrics.get("thinking_count", 0) or 0)
    queen_state = "thinking" if thinking_count > 0 or (followup_q + internal_q) > 0 else "idle"

    requests = _read_jsonl(settings.state_dir / "control_requests.jsonl")
    acks = _read_jsonl(settings.state_dir / "control_acks.jsonl")
    acked_ids = {str(a.get("request_id", "")) for a in acks}
    pending_requests = [r for r in requests if str(r.get("request_id", "")) not in acked_ids]
    last_ack = acks[-1] if acks else None

    log_path = settings.state_dir / "logs" / "broodmind.log"
    recent_logs = _tail_logs(log_path, 12)

    return {
        "system": {
            "running": running,
            "pid": pid,
            "active_channel": status_data.get("active_channel", "Telegram"),
            "started_at": status_data.get("started_at"),
            "last_heartbeat": status_data.get("last_message_at"),
            "uptime": _uptime_human(status_data.get("started_at")),
        },
        "queen": {
            "state": queen_state,
            "followup_queues": followup_q,
            "internal_queues": internal_q,
            "followup_tasks": int(queen_metrics.get("followup_tasks", 0) or 0),
            "internal_tasks": int(queen_metrics.get("internal_tasks", 0) or 0),
        },
        "connectivity": {"mcp_servers": connectivity_metrics.get("mcp_servers", {})},
        "logs": recent_logs,
        "queues": {
            "telegram_send_tasks": int(telegram_metrics.get("send_tasks", 0) or 0),
            "telegram_queues": int(telegram_metrics.get("chat_queues", 0) or 0),
            "exec_sessions_running": int(exec_metrics.get("background_sessions_running", 0) or 0),
            "exec_sessions_total": int(exec_metrics.get("background_sessions_total", 0) or 0),
        },
        "workers": {
            "spawned_24h": spawned_24h,
            "running": by_status.get("running", 0) + by_status.get("started", 0),
            "root_running": root_running,
            "subworkers_running": subworkers_running,
            "completed": by_status.get("completed", 0),
            "failed": by_status.get("failed", 0),
            "stopped": by_status.get("stopped", 0),
            "topology": [
                {
                    "id": w.id,
                    "template_name": w.template_name or w.template_id or "",
                    "status": w.status,
                    "task": w.task,
                    "updated_at": w.updated_at.isoformat(),
                    "parent_worker_id": w.parent_worker_id,
                    "lineage_id": w.lineage_id,
                    "spawn_depth": w.spawn_depth,
                }
                for w in running_nodes
            ],
            "recent": [
                {
                    "id": w.id,
                    "template_name": w.template_name or w.template_id or "",
                    "status": w.status,
                    "task": w.task,
                    "updated_at": w.updated_at.isoformat(),
                    "summary": w.summary or "",
                    "error": w.error or "",
                    "tools_used": w.tools_used or [],
                    "parent_worker_id": w.parent_worker_id,
                    "lineage_id": w.lineage_id,
                    "spawn_depth": w.spawn_depth,
                }
                for w in recent_workers[:last]
            ],
        },
        "control": {
            "pending_requests": len(pending_requests),
            "last_ack": last_ack,
        },
    }


def _tail_logs(path: Path, max_lines: int) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for line in _read_last_lines(path, max_lines=max_lines):
        raw = line.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            out.append({"event": raw[:120], "level": "info"})
            continue
        if not isinstance(data, dict):
            continue
        out.append(
            {
                "event": str(data.get("event", ""))[:120],
                "level": str(data.get("level", "info")),
            }
        )
    return out


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for line in _read_last_lines(path, max_lines=250):
        raw = line.strip()
        if not raw:
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            out.append(item)
    return out


def _read_last_lines(path: Path, max_lines: int = 200, max_bytes: int = 256 * 1024) -> list[str]:
    if not path.exists() or max_lines <= 0:
        return []
    try:
        size = path.stat().st_size
    except OSError:
        return []
    start = max(0, size - max(1, max_bytes))
    tail: deque[str] = deque(maxlen=max_lines)
    try:
        with path.open("rb") as handle:
            if start > 0:
                handle.seek(start)
                _ = handle.readline()
            for raw in handle:
                text = raw.decode("utf-8", errors="ignore").rstrip("\n\r")
                tail.append(text)
    except OSError:
        return []
    return list(tail)


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _uptime_human(started_at: str | None) -> str:
    if not started_at:
        return "N/A"
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except ValueError:
        return "N/A"
    delta = _now_utc() - start
    total = int(delta.total_seconds())
    if total < 0:
        return "N/A"
    hours = total // 3600
    minutes = (total % 3600) // 60
    seconds = total % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def _dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BroodMind Control Deck</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {
      --ink: #e5e7eb;
      --muted: #98a4b8;
      --paper: #0a0f1b;
      --panel: rgba(16, 24, 43, 0.78);
      --line: #25324d;
      --teal: #2dd4bf;
      --amber: #f59e0b;
      --rose: #fb7185;
      --mint: #34d399;
      --sky: #38bdf8;
    }
    * { box-sizing: border-box; }
    html, body { margin: 0; min-height: 100%; }
    body {
      color: var(--ink);
      font-family: "Space Grotesk", sans-serif;
      background:
        radial-gradient(1000px 600px at 5% -5%, rgba(56, 189, 248, 0.18), transparent 60%),
        radial-gradient(900px 700px at 100% 0%, rgba(251, 113, 133, 0.14), transparent 55%),
        linear-gradient(170deg, #070b14, #0a1220 48%, #0b1526);
    }
    .noise::before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      opacity: 0.06;
      background-image: radial-gradient(#fff 0.4px, transparent 0.5px);
      background-size: 3px 3px;
    }
    .wrap { width: min(1280px, 96vw); margin: 24px auto 40px; }
    .topbar {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 14px;
    }
    .headline { display: flex; gap: 12px; align-items: baseline; }
    .title {
      margin: 0;
      font-size: clamp(24px, 3.4vw, 38px);
      line-height: 1;
      letter-spacing: 0.02em;
    }
    .subtitle { color: var(--muted); font-size: 13px; letter-spacing: 0.08em; text-transform: uppercase; }
    .controls { display: flex; gap: 8px; flex-wrap: wrap; }
    .input, .btn {
      border: 1px solid var(--line);
      background: rgba(7, 13, 23, 0.85);
      color: var(--ink);
      border-radius: 12px;
      height: 40px;
      padding: 0 12px;
      font-family: inherit;
    }
    .input { width: 260px; }
    .btn {
      cursor: pointer;
      font-weight: 600;
      transition: transform 120ms ease, border-color 120ms ease, background-color 120ms ease;
    }
    .btn:hover { transform: translateY(-1px); border-color: var(--sky); }
    .btn.primary { background: linear-gradient(90deg, #0f766e, #155e75); border-color: transparent; }
    .status-strip {
      display: grid;
      grid-template-columns: repeat(5, minmax(160px, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }
    .kpi {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.18);
      transform: translateY(8px);
      opacity: 0;
      animation: lift 420ms ease forwards;
    }
    .kpi:nth-child(2) { animation-delay: 80ms; }
    .kpi:nth-child(3) { animation-delay: 120ms; }
    .kpi:nth-child(4) { animation-delay: 180ms; }
    .kpi:nth-child(5) { animation-delay: 240ms; }
    .kpi-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.1em; }
    .kpi-value { margin-top: 6px; font-size: 28px; font-weight: 700; line-height: 1.1; }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      margin-top: 8px;
      padding: 3px 10px;
      border-radius: 999px;
      border: 1px solid var(--line);
      color: var(--muted);
      font-size: 12px;
    }
    .ok { color: var(--mint); }
    .warn { color: var(--amber); }
    .bad { color: var(--rose); }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1.5fr) minmax(0, 1fr);
      gap: 10px;
    }
    .layout > * { min-width: 0; }
    .layout.mcp-topology {
      grid-template-columns: minmax(220px, 320px) minmax(0, 1fr);
      align-items: start;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      box-shadow: 0 10px 28px rgba(0, 0, 0, 0.2);
      overflow: hidden;
    }
    .card h3 { margin: 0 0 10px; font-size: 15px; letter-spacing: 0.04em; text-transform: uppercase; color: var(--muted); }
    .chart-wrap { height: 230px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { text-align: left; padding: 9px 6px; border-bottom: 1px solid rgba(37, 50, 77, 0.75); vertical-align: top; }
    th { color: var(--muted); font-weight: 600; }
    td strong { font-size: 12px; }
    .mono { font-family: "JetBrains Mono", monospace; }
    .task-prefix { color: var(--amber); font-weight: 700; }
    .task-prefix-sched { color: var(--mint); font-weight: 700; }
    .workers { max-height: 310px; overflow: auto; }
    .logs { max-height: 270px; overflow: auto; }
    .log-line { border-bottom: 1px dashed rgba(37, 50, 77, 0.75); padding: 8px 2px; font-size: 13px; line-height: 1.35; }
    .topology { max-height: 190px; overflow: auto; display: grid; gap: 8px; }
    .topo-row {
      border: 1px solid rgba(37, 50, 77, 0.75);
      border-radius: 12px;
      padding: 8px 10px;
      background: rgba(9, 15, 28, 0.75);
    }
    .topo-head { display: flex; gap: 8px; align-items: center; justify-content: space-between; font-size: 12px; }
    .topo-id { font-family: "JetBrains Mono", monospace; color: #c7d2fe; }
    .topo-task {
      margin-top: 6px;
      font-size: 12px;
      color: var(--ink);
      white-space: normal;
      word-break: break-word;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    .topo-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 2px 8px;
      border: 1px solid var(--line);
      font-size: 11px;
      color: var(--muted);
    }
    .pulse {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--mint);
      box-shadow: 0 0 0 0 rgba(52, 211, 153, 0.8);
      animation: pulse 1.4s infinite;
    }
    .meta { margin-top: 10px; color: var(--muted); font-size: 12px; font-family: "JetBrains Mono", monospace; }
    .err { color: var(--rose); margin-top: 8px; font-size: 13px; min-height: 1.1em; }
    .workers table { table-layout: fixed; }
    .workers th:nth-child(1), .workers td:nth-child(1) { width: 14%; }
    .workers th:nth-child(2), .workers td:nth-child(2) { width: 8%; }
    .workers th:nth-child(3), .workers td:nth-child(3) { width: 46%; word-break: break-word; }
    .workers th:nth-child(4), .workers td:nth-child(4) { width: 12%; }
    .workers th:nth-child(5), .workers td:nth-child(5) { width: 20%; word-break: break-word; }
    @keyframes lift { to { transform: translateY(0); opacity: 1; } }
    @keyframes pulse {
      0% { box-shadow: 0 0 0 0 rgba(52, 211, 153, 0.8); }
      70% { box-shadow: 0 0 0 8px rgba(52, 211, 153, 0); }
      100% { box-shadow: 0 0 0 0 rgba(52, 211, 153, 0); }
    }
    @media (max-width: 1060px) {
      .status-strip { grid-template-columns: repeat(2, minmax(150px, 1fr)); }
      .layout { grid-template-columns: 1fr; }
    }
    @media (max-width: 580px) {
      .wrap { width: min(1280px, 94vw); }
      .controls { width: 100%; }
      .input { width: 100%; }
      .btn { flex: 1; }
      .status-strip { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body class="noise">
  <div class="wrap">
    <div class="topbar">
      <div class="headline">
        <h1 class="title">BroodMind Control Deck</h1>
        <span class="subtitle">private tailnet telemetry</span>
      </div>
      <div class="controls">
        <input id="token" class="input" type="password" placeholder="Dashboard token" />
        <button id="save-token" class="btn">Save Token</button>
        <button id="refresh" class="btn primary">Refresh</button>
      </div>
    </div>

    <section class="status-strip">
      <article class="kpi">
        <div class="kpi-label">System</div>
        <div id="system-running" class="kpi-value">-</div>
        <div id="chip-channel" class="chip">Channel -</div>
      </article>
      <article class="kpi">
        <div class="kpi-label">Queen State</div>
        <div id="queen-state" class="kpi-value">-</div>
        <div id="chip-uptime" class="chip">Uptime -</div>
      </article>
      <article class="kpi">
        <div class="kpi-label">Workers Running</div>
        <div id="workers-running" class="kpi-value">0</div>
        <div id="chip-spawned" class="chip">24h spawned - | subworkers -</div>
      </article>
      <article class="kpi">
        <div class="kpi-label">Failures</div>
        <div id="workers-failed" class="kpi-value">0</div>
        <div id="chip-completed" class="chip">completed -</div>
      </article>
      <article class="kpi">
        <div class="kpi-label">Control Queue</div>
        <div id="control-pending" class="kpi-value">0</div>
        <div id="chip-telegram" class="chip">telegram queues -</div>
      </article>
    </section>

    <section class="layout">
      <div class="card">
        <h3>Worker Throughput (rolling)</h3>
        <div class="chart-wrap"><canvas id="activity-chart"></canvas></div>
      </div>
      <div class="card">
        <h3>Recent Events</h3>
        <div id="logs" class="logs">No data yet.</div>
      </div>
    </section>

    <section class="layout mcp-topology" style="margin-top: 10px;">
      <div class="card">
        <h3>MCP Connectivity</h3>
        <div id="mcp-status">No MCP data yet.</div>
      </div>
      <div class="card">
        <h3>Live Worker Topology</h3>
        <div id="worker-topology" class="topology">No running workers.</div>
      </div>
    </section>

    <section class="card" style="margin-top: 10px;">
      <h3>Recent Workers</h3>
      <div class="workers">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Status</th>
              <th>Task</th>
              <th>Last Tool</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody id="workers-table"><tr><td colspan="5">No workers yet.</td></tr></tbody>
        </table>
      </div>
    </section>

    <div class="meta" id="meta">Last refresh: never</div>
    <div class="err" id="error"></div>
  </div>

  <script>
    const tokenInput = document.getElementById("token");
    const saveBtn = document.getElementById("save-token");
    const refreshBtn = document.getElementById("refresh");
    const tokenKey = "broodmind.dashboard.token";
    tokenInput.value = localStorage.getItem(tokenKey) || "";

    const historySize = 30;
    const history = [];
    let chart = null;
    const browserTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || undefined;
    const dateTimeFormatter = new Intl.DateTimeFormat(undefined, {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      hour12: true,
      timeZone: browserTimeZone
    });
    const timeFormatter = new Intl.DateTimeFormat(undefined, {
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      hour12: true,
      timeZone: browserTimeZone
    });

    saveBtn.addEventListener("click", () => {
      localStorage.setItem(tokenKey, tokenInput.value || "");
      runOnce();
    });
    refreshBtn.addEventListener("click", runOnce);

    function headers() {
      const token = tokenInput.value || "";
      return token ? { "x-broodmind-token": token } : {};
    }

    function esc(v) {
      return String(v ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
    }

    function statusClass(value) {
      const v = String(value || "").toLowerCase();
      if (["running", "idle", "thinking", "connected", "ok", "completed"].includes(v)) return "ok";
      if (["warning", "stopped"].includes(v)) return "warn";
      return "bad";
    }

    function highlightTaskPrefixes(text) {
      const source = String(text || "");
      const parts = source.split(/(\[[^\]]+\])/g);
      return parts.map((part) => {
        if (!part) return "";
        if (part.startsWith("[") && part.endsWith("]")) {
          const cls = part.toLowerCase().includes("schedul") ? "task-prefix-sched" : "task-prefix";
          return "<span class='" + cls + "'>" + esc(part) + "</span>";
        }
        return esc(part);
      }).join("");
    }

    function setKpi(id, text, cls) {
      const el = document.getElementById(id);
      el.textContent = text;
      el.className = "kpi-value " + (cls || "");
    }

    function setChip(id, text) {
      document.getElementById(id).textContent = text;
    }

    function formatTimestampLocal(value) {
      if (value === null || value === undefined || value === "") return "never";
      const raw = String(value).trim();
      const d = value instanceof Date ? value : new Date(raw);
      if (Number.isNaN(d.getTime())) return raw;
      return dateTimeFormatter.format(d);
    }

    function formatTimeLocal(value) {
      if (value === null || value === undefined || value === "") return "never";
      const d = value instanceof Date ? value : new Date(String(value).trim());
      if (Number.isNaN(d.getTime())) return String(value);
      return timeFormatter.format(d);
    }

    function ensureChart() {
      if (chart) return chart;
      const ctx = document.getElementById("activity-chart");
      chart = new Chart(ctx, {
        type: "line",
        data: {
          labels: [],
          datasets: [
            {
              label: "Running workers",
              data: [],
              borderColor: "#34d399",
              backgroundColor: "rgba(52, 211, 153, 0.20)",
              fill: true,
              tension: 0.3,
              pointRadius: 0
            },
            {
              label: "Queue pressure",
              data: [],
              borderColor: "#f59e0b",
              backgroundColor: "rgba(245, 158, 11, 0.14)",
              fill: true,
              tension: 0.3,
              pointRadius: 0
            }
          ]
        },
        options: {
          animation: false,
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { labels: { color: "#98a4b8", boxWidth: 12 } }
          },
          scales: {
            x: { ticks: { color: "#98a4b8", maxTicksLimit: 6 }, grid: { color: "rgba(37, 50, 77, 0.42)" } },
            y: { ticks: { color: "#98a4b8" }, grid: { color: "rgba(37, 50, 77, 0.42)" }, beginAtZero: true }
          }
        }
      });
      return chart;
    }

    function updateChartPoint(data) {
      const queuePressure = Number(data.queen.followup_queues || 0) + Number(data.queen.internal_queues || 0);
      history.push({
        t: formatTimeLocal(new Date()),
        workers: Number(data.workers.running || 0),
        queues: queuePressure
      });
      while (history.length > historySize) history.shift();
      const c = ensureChart();
      c.data.labels = history.map((h) => h.t);
      c.data.datasets[0].data = history.map((h) => h.workers);
      c.data.datasets[1].data = history.map((h) => h.queues);
      const peak = Math.max(
        0,
        ...history.map((h) => Math.max(Number(h.workers || 0), Number(h.queues || 0)))
      );
      const paddedMax = peak <= 2 ? 4 : Math.max(4, Math.ceil(peak * 1.35));
      c.options.scales.y.max = paddedMax;
      c.options.scales.y.suggestedMax = paddedMax;
      c.update();
    }

    function renderWorkers(workers) {
      const rows = (workers || []).map((w) => {
        const lastTool = (Array.isArray(w.tools_used) && w.tools_used.length > 0) ? w.tools_used[w.tools_used.length - 1] : "-";
        const taskRaw = String(w.task || "");
        const taskShort = taskRaw.length > 220 ? (taskRaw.slice(0, 217) + "...") : taskRaw;
        const fullId = String(w.id || "");
        const shortId = fullId.includes("-") ? fullId.split("-")[0] : fullId.slice(0, 8);
        const workerName = String(w.template_name || "").trim();
        const workerDisplay = workerName ? (workerName + " (" + shortId + ")") : shortId;
        return "<tr>" +
          "<td class='mono' title='" + esc(fullId) + "'>" + esc(workerDisplay) + "</td>" +
          "<td class='" + statusClass(w.status) + "'><strong>" + esc(w.status) + "</strong></td>" +
          "<td title='" + esc(taskRaw) + "'>" + highlightTaskPrefixes(taskShort) + "</td>" +
          "<td class='mono'>" + esc(lastTool) + "</td>" +
          "<td class='mono'>" + esc(formatTimestampLocal(w.updated_at)) + "</td>" +
          "</tr>";
      });
      document.getElementById("workers-table").innerHTML = rows.length ? rows.join("") : "<tr><td colspan='5'>No workers</td></tr>";
    }

    function renderLogs(logs) {
      const el = document.getElementById("logs");
      const html = (logs || []).map((l) => {
        const level = String(l.level || "info").toLowerCase();
        const cls = level === "error" ? "bad" : (level === "warning" ? "warn" : "ok");
        return "<div class='log-line'><span class='" + cls + "'>" + esc(level.toUpperCase()) + "</span> " + esc(l.event || "") + "</div>";
      });
      el.innerHTML = html.length ? html.join("") : "No logs.";
      el.scrollTop = el.scrollHeight;
    }

    function renderMcp(servers) {
      const mcp = servers || {};
      const keys = Object.keys(mcp);
      if (!keys.length) {
        document.getElementById("mcp-status").innerHTML = "<div class='meta'>No MCP servers configured.</div>";
        return;
      }
      const rows = keys.map((k) => {
        const item = mcp[k] || {};
        const name = item.name || k;
        const status = String(item.status || "unknown").toLowerCase();
        const toolCount = Number(item.tool_count || 0);
        const cls = status === "connected" ? "ok" : (status === "error" ? "bad" : "warn");
        return "<div class='log-line'>" +
          "<strong>" + esc(name) + "</strong> " +
          "<span class='" + cls + "'>" + esc(status.toUpperCase()) + "</span> " +
          "<span class='mono'>(" + toolCount + " tools)</span>" +
          "</div>";
      });
      document.getElementById("mcp-status").innerHTML = rows.join("");
    }

    function renderTopology(nodes) {
      const items = Array.isArray(nodes) ? nodes.slice() : [];
      items.sort((a, b) => {
        const depthA = Number(a.spawn_depth || 0);
        const depthB = Number(b.spawn_depth || 0);
        if (depthA !== depthB) return depthA - depthB;
        return String(a.updated_at || "").localeCompare(String(b.updated_at || ""));
      });
      if (!items.length) {
        document.getElementById("worker-topology").innerHTML = "No running workers.";
        return;
      }
      const html = items.map((w) => {
        const depth = Math.max(0, Number(w.spawn_depth || 0));
        const left = Math.min(depth * 16, 64);
        const parent = w.parent_worker_id ? ("child of " + String(w.parent_worker_id).slice(0, 8)) : "root worker";
        const wid = String(w.id || "");
        const shortId = wid.includes("-") ? wid.split("-")[0] : wid.slice(0, 8);
        const workerName = String(w.template_name || "").trim();
        const workerLabel = workerName ? (workerName + " (" + shortId + ")") : shortId;
        return "<div class='topo-row' style='margin-left:" + left + "px'>" +
          "<div class='topo-head'>" +
          "<span class='topo-id' title='" + esc(wid) + "'>" + esc(workerLabel) + "</span>" +
          "<span class='topo-badge'><span class='pulse'></span>" + esc(parent) + "</span>" +
          "</div>" +
          "<div class='topo-task'>" + highlightTaskPrefixes(w.task || "") + "</div>" +
          "</div>";
      });
      document.getElementById("worker-topology").innerHTML = html.join("");
    }

    async function runOnce() {
      const errorEl = document.getElementById("error");
      errorEl.textContent = "";
      try {
        const rsp = await fetch("/api/dashboard/snapshot?last=14", { headers: headers() });
        if (!rsp.ok) throw new Error("API " + rsp.status);
        const data = await rsp.json();

        setKpi("system-running", data.system.running ? "Running" : "Stopped", data.system.running ? "ok" : "bad");
        setKpi("queen-state", data.queen.state || "-", statusClass(data.queen.state));
        setKpi("workers-running", String(data.workers.running || 0), (data.workers.running || 0) > 0 ? "ok" : "warn");
        setKpi("workers-failed", String(data.workers.failed || 0), (data.workers.failed || 0) > 0 ? "bad" : "ok");
        setKpi("control-pending", String(data.control.pending_requests || 0), (data.control.pending_requests || 0) > 0 ? "warn" : "ok");

        setChip("chip-channel", "Channel " + (data.system.active_channel || "-"));
        setChip("chip-uptime", "Uptime " + (data.system.uptime || "N/A"));
        setChip(
          "chip-spawned",
          "24h spawned " + String(data.workers.spawned_24h || 0) +
          " | subworkers " + String(data.workers.subworkers_running || 0)
        );
        setChip("chip-completed", "completed " + String(data.workers.completed || 0));
        setChip("chip-telegram", "telegram queues " + String(data.queues.telegram_queues || 0));

        renderWorkers(data.workers.recent || []);
        renderLogs(data.logs || []);
        renderMcp((data.connectivity || {}).mcp_servers || {});
        renderTopology(data.workers.topology || []);
        updateChartPoint(data);

        document.getElementById("meta").textContent =
          "Last refresh " + formatTimestampLocal(new Date()) +
          " | heartbeat " + formatTimestampLocal(data.system.last_heartbeat) +
          " | tz " + (browserTimeZone || "local") +
          " | pid " + (data.system.pid || "N/A");
      } catch (err) {
        errorEl.textContent = "Dashboard request failed: " + err;
      }
    }

    runOnce();
    setInterval(runOnce, 2000);
  </script>
</body>
</html>
"""
