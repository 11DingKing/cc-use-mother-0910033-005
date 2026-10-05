"""追加式审计日志：所有状态变化留痕，可查询、不可篡改（进程内）。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AuditEntry:
    seq: int
    ts: float
    actor: str
    role: str
    action: str
    subject: str
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "seq": self.seq,
            "ts": self.ts,
            "actor": self.actor,
            "role": self.role,
            "action": self.action,
            "subject": self.subject,
            "details": self.details,
        }


class AuditLog:
    def __init__(self, now_fn) -> None:
        self._now = now_fn
        self._entries: list[AuditEntry] = []

    def record(self, actor: str, role: str, action: str, subject: str, **details) -> AuditEntry:
        entry = AuditEntry(
            seq=len(self._entries) + 1,
            ts=self._now(),
            actor=actor,
            role=role,
            action=action,
            subject=subject,
            details=details,
        )
        self._entries.append(entry)
        return entry

    def list(self, action: str | None = None, subject: str | None = None) -> list[AuditEntry]:
        result = self._entries
        if action is not None:
            result = [e for e in result if e.action == action]
        if subject is not None:
            result = [e for e in result if e.subject == subject]
        return list(result)
