"""追加式审计日志。"""
from __future__ import annotations

from typing import Any, Callable

from .models import new_id, now_ms


class AuditLog:
    """所有人工与自动动作只追加、不修改。"""

    def __init__(self, clock: Callable[[], int] = now_ms) -> None:
        self._clock = clock
        self._events: list[dict[str, Any]] = []

    def append(
        self,
        action: str,
        actor: str,
        detail: dict[str, Any] | None = None,
        *,
        incident_id: str | None = None,
        ref: str | None = None,
    ) -> dict[str, Any]:
        event = {
            "event_id": new_id("evt"),
            "at": self._clock(),
            "action": action,
            "actor": actor,
            "incident_id": incident_id,
            "ref": ref,
            "detail": detail or {},
        }
        self._events.append(event)
        return event

    def list(
        self,
        *,
        incident_id: str | None = None,
        action: str | None = None,
        actor: str | None = None,
    ) -> list[dict[str, Any]]:
        result = self._events
        if incident_id is not None:
            result = [e for e in result if e["incident_id"] == incident_id]
        if action is not None:
            result = [e for e in result if e["action"] == action]
        if actor is not None:
            result = [e for e in result if e["actor"] == actor]
        return list(result)
