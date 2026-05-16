from __future__ import annotations

import os

from fastapi import FastAPI

from octopal.channels.whatsapp.routes import register_whatsapp_routes
from octopal.gateway.dashboard import register_dashboard_routes
from octopal.gateway.ws import register_ws_routes
from octopal.infrastructure.config.settings import Settings
from octopal.interop.a2a.routes import register_a2a_routes
from octopal.runtime.octo.core import Octo
from octopal.tools.skills.management import ensure_skills_layout


def build_app(settings: Settings, octo: Octo | None = None) -> FastAPI:
    """Build the FastAPI app for the Octopal Gateway.

    It reuses the shared Octo instance for WebSocket communication.
    """
    os.environ.setdefault("OCTOPAL_STATE_DIR", str(settings.state_dir))
    os.environ.setdefault("OCTOPAL_WORKSPACE_DIR", str(settings.workspace_dir))
    ensure_skills_layout(settings.workspace_dir)
    app = FastAPI(title="Octopal Gateway")

    app.state.settings = settings
    app.state.octo = octo

    # Expose necessary components if any route needs them
    if octo:
        app.state.store = octo.store
        app.state.policy = octo.policy
        app.state.runtime = octo.runtime
        app.state.provider = octo.provider
        app.state.memory = octo.memory
        app.state.canon = octo.canon

    register_a2a_routes(app)
    register_ws_routes(app)
    register_whatsapp_routes(app)
    register_dashboard_routes(app)
    return app
