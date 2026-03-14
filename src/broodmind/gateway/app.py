from __future__ import annotations

import os
from fastapi import FastAPI
from broodmind.config.settings import Settings
from broodmind.gateway.dashboard import register_dashboard_routes
from broodmind.gateway.ws import register_ws_routes
from broodmind.queen.core import Queen
from broodmind.tools.skills_tools import ensure_skills_layout
from broodmind.channels.whatsapp.routes import register_whatsapp_routes

def build_app(settings: Settings, queen: Queen | None = None) -> FastAPI:
    """Build the FastAPI app for the BroodMind Gateway.
    
    It reuses the shared Queen instance for WebSocket communication.
    """
    os.environ.setdefault("BROODMIND_STATE_DIR", str(settings.state_dir))
    os.environ.setdefault("BROODMIND_WORKSPACE_DIR", str(settings.workspace_dir))
    ensure_skills_layout(settings.workspace_dir)
    app = FastAPI(title="BroodMind Gateway")
    
    app.state.settings = settings
    app.state.queen = queen
    
    # Expose necessary components if any route needs them
    if queen:
        app.state.store = queen.store
        app.state.policy = queen.policy
        app.state.runtime = queen.runtime
        app.state.provider = queen.provider
        app.state.memory = queen.memory
        app.state.canon = queen.canon
    
    register_ws_routes(app)
    register_dashboard_routes(app)
    register_whatsapp_routes(app)
    return app
