from __future__ import annotations

import asyncio
from dataclasses import dataclass

from octopal.runtime.octo.delivery import DeliveryMode


@dataclass
class OctoReply:
    immediate: str
    followup: asyncio.Task[str] | None
    followup_required: bool = False
    reaction: str | None = None
    delivery_mode: DeliveryMode = DeliveryMode.IMMEDIATE
