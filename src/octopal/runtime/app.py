from __future__ import annotations

import os

from octopal.channels.telegram.approvals import ApprovalManager
from octopal.infrastructure.config.settings import Settings
from octopal.infrastructure.mcp.manager import MCPManager
from octopal.infrastructure.providers.litellm_provider import LiteLLMProvider
from octopal.infrastructure.providers.openai_embeddings import OpenAIEmbeddingsProvider
from octopal.infrastructure.store.sqlite import SQLiteStore
from octopal.runtime.memory.canon import CanonService
from octopal.runtime.memory.service import MemoryService
from octopal.runtime.octo.core import Octo
from octopal.runtime.policy.engine import PolicyEngine
from octopal.runtime.scheduler.service import SchedulerService
from octopal.runtime.workers.launcher_factory import build_launcher
from octopal.runtime.workers.runtime import WorkerRuntime
from octopal.tools.skills.management import ensure_skills_layout


def build_octo(settings: Settings) -> Octo:
    os.environ.setdefault("OCTOPAL_STATE_DIR", str(settings.state_dir))
    os.environ.setdefault("OCTOPAL_WORKSPACE_DIR", str(settings.workspace_dir))
    ensure_skills_layout(settings.workspace_dir)

    provider = LiteLLMProvider(settings)
    store = SQLiteStore(settings)

    from octopal.runtime.workers.templates import initialize_templates

    initialize_templates(store)

    policy = PolicyEngine()
    launcher = build_launcher(settings)
    mcp_manager = MCPManager(workspace_dir=settings.workspace_dir)
    runtime = WorkerRuntime(
        store=store,
        policy=policy,
        workspace_dir=settings.workspace_dir,
        launcher=launcher,
        settings=settings,
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

    from octopal.infrastructure.connectors.manager import ConnectorManager

    connector_manager = ConnectorManager(
        config=settings.connectors,
        mcp_manager=mcp_manager,
        octo_config=settings.config_obj,
    )

    octo = Octo(
        provider=provider,
        store=store,
        policy=policy,
        runtime=runtime,
        approvals=approvals,
        memory=memory,
        canon=canon,
        scheduler=scheduler,
        mcp_manager=mcp_manager,
        connector_manager=connector_manager,
    )
    runtime.octo = octo
    return octo
