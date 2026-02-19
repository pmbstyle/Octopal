from __future__ import annotations

import os
from fastapi import FastAPI
from broodmind.config.settings import Settings
from broodmind.gateway.ws import register_ws_routes
from broodmind.queen.core import Queen

def build_app(settings: Settings, queen: Queen | None = None) -> FastAPI:
    """Build the FastAPI app for the BroodMind Gateway.
    
    It reuses the shared Queen instance for WebSocket communication.
    """
    os.environ.setdefault("BROODMIND_STATE_DIR", str(settings.state_dir))
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
    return app
