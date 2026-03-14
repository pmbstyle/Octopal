from __future__ import annotations

import os

from broodmind.config.settings import Settings
from broodmind.mcp.manager import MCPManager
from broodmind.memory.canon import CanonService
from broodmind.memory.service import MemoryService
from broodmind.policy.engine import PolicyEngine
from broodmind.providers.litellm_provider import LiteLLMProvider
from broodmind.providers.openai_embeddings import OpenAIEmbeddingsProvider
from broodmind.queen.core import Queen
from broodmind.scheduler.service import SchedulerService
from broodmind.store.sqlite import SQLiteStore
from broodmind.channels.telegram.approvals import ApprovalManager
from broodmind.tools.skills_tools import ensure_skills_layout
from broodmind.workers.launcher_factory import build_launcher
from broodmind.workers.runtime import WorkerRuntime


def build_queen(settings: Settings) -> Queen:
    os.environ.setdefault("BROODMIND_STATE_DIR", str(settings.state_dir))
    os.environ.setdefault("BROODMIND_WORKSPACE_DIR", str(settings.workspace_dir))
    ensure_skills_layout(settings.workspace_dir)

    provider = LiteLLMProvider(settings)
    store = SQLiteStore(settings)

    from broodmind.workers.templates import initialize_templates

    initialize_templates(store)

    policy = PolicyEngine()
    launcher = build_launcher(settings)
    mcp_manager = MCPManager(workspace_dir=settings.workspace_dir)
    runtime = WorkerRuntime(
        store=store,
        policy=policy,
        workspace_dir=settings.workspace_dir,
        launcher=launcher,
        mcp_manager=mcp_manager,
    )
    approvals = ApprovalManager(bot=None)
    embeddings = OpenAIEmbeddingsProvider(settings) if settings.openai_api_key else None
    memory = MemoryService(
        store=store,
        embeddings=embeddings,
        owner_id=settings.memory_owner_id,
        top_k=settings.memory_top_k,
        prefilter_k=settings.memory_prefilter_k,
        min_score=settings.memory_min_score,
        max_chars=settings.memory_max_chars,
    )
    canon = CanonService(
        workspace_dir=settings.workspace_dir,
        store=store,
        embeddings=embeddings,
    )
    scheduler = SchedulerService(store=store, workspace_dir=settings.workspace_dir)
    queen = Queen(
        provider=provider,
        store=store,
        policy=policy,
        runtime=runtime,
        approvals=approvals,
        memory=memory,
        canon=canon,
        scheduler=scheduler,
        mcp_manager=mcp_manager,
    )
    runtime.queen = queen
    return queen
