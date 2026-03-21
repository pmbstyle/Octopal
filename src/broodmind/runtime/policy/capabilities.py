from __future__ import annotations

DEFAULT_CAPABILITY_WHITELIST: dict[str, list[str]] = {
    "filesystem": ["worker", "/workspace/**"],
    "filesystem_read": ["worker"],
    "filesystem_write": ["worker"],
    "network": ["*"],
    "exec": ["worker", "python", "node"],
    "service_read": ["worker"],
    "service_control": ["worker"],
    "deploy_control": ["worker"],
    "db_admin": ["worker"],
    "security_audit": ["worker"],
    "self_control": ["worker"],
    "mcp_exec": ["*"],
    "mcp_manage": ["worker"],
    "skill_use": ["worker"],
    "skill_exec": ["worker", "python", "node"],
    "skill_manage": ["worker"],
    "email": ["*"],
    "payment": [],
}
