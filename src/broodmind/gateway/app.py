from __future__ import annotations

from fastapi import FastAPI

from broodmind.config.settings import Settings
from broodmind.gateway.ws import register_ws_routes
from broodmind.memory.service import MemoryService
from broodmind.policy.engine import PolicyEngine
from broodmind.providers.openai_embeddings import OpenAIEmbeddingsProvider
from broodmind.providers.litellm_provider import LiteLLMProvider
from broodmind.store.sqlite import SQLiteStore
from broodmind.workers.launcher_factory import build_launcher
from broodmind.workers.runtime import WorkerRuntime


def build_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="BroodMind Gateway")
    store = SQLiteStore(settings)

    # Initialize default worker templates
    from broodmind.workers.templates import initialize_templates
    initialize_templates(store)

    policy = PolicyEngine()
    launcher = build_launcher(settings)
    runtime = WorkerRuntime(
        store=store,
        policy=policy,
        workspace_dir=settings.workspace_dir,
        launcher=launcher,
    )

    # Use unified LiteLLM provider (supports both OpenRouter and z.ai)
    provider = LiteLLMProvider(settings)

    embeddings = None
    if settings.openai_api_key:
        embeddings = OpenAIEmbeddingsProvider(settings)
    memory = MemoryService(
        store=store,
        embeddings=embeddings,
        top_k=settings.memory_top_k,
        min_score=settings.memory_min_score,
        max_chars=settings.memory_max_chars,
    )
    app.state.settings = settings
    app.state.store = store
    app.state.policy = policy
    app.state.runtime = runtime
    app.state.provider = provider
    app.state.memory = memory
    register_ws_routes(app)
    return app
