from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RouteMode(StrEnum):
    CONVERSATION = "conversation"
    HEARTBEAT = "heartbeat"
    SCHEDULER = "scheduler"
    PROACTIVE = "proactive"
    WORKER_FOLLOWUP = "worker_followup"
    INTERNAL_MAINTENANCE = "internal_maintenance"


@dataclass(frozen=True)
class RouteRequest:
    mode: RouteMode
    user_text: str
    chat_id: int
    show_typing: bool = True
    include_wakeup: bool = True
    track_progress: bool = True
    background_delivery: bool = False

    @property
    def is_control_plane(self) -> bool:
        return self.mode is not RouteMode.CONVERSATION


def resolve_turn_route_mode(*, track_progress: bool, background_delivery: bool) -> RouteMode:
    if track_progress:
        return RouteMode.CONVERSATION
    if background_delivery:
        return RouteMode.HEARTBEAT
    return RouteMode.INTERNAL_MAINTENANCE
