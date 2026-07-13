from __future__ import annotations

VALID_MESSAGE_TYPES = {
    "log",
    "intent_request",
    "intent_executed",
    "octo_tool_call",
    "octo_tool_result",
    "programmatic_read_batch",
    "programmatic_read_batch_result",
    "mcp_call",
    "mcp_result",
    "error",
    "await_children",
    "resume_children",
    "instruction_request",
    "resume_instruction",
    "result",
    "permit",
    "permit_denied",
    "shutdown",
}
