from __future__ import annotations

import secrets
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request

def register_whatsapp_routes(app: FastAPI) -> None:
    @app.post("/api/channels/whatsapp/inbound")
    async def whatsapp_inbound(
        request: Request,
        x_broodmind_whatsapp_token: str | None = Header(default=None),
    ) -> dict[str, Any]:
        runtime = getattr(app.state, "whatsapp_runtime", None)
        if runtime is None or not hasattr(runtime, "settings") or not hasattr(runtime, "handle_inbound"):
            raise HTTPException(status_code=404, detail="WhatsApp runtime not enabled")
        expected = runtime.settings.whatsapp_callback_token.strip()
        provided = (x_broodmind_whatsapp_token or "").strip()
        if expected and not secrets.compare_digest(expected, provided):
            raise HTTPException(status_code=403, detail="Invalid WhatsApp callback token")
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload")
        return await runtime.handle_inbound(payload)
