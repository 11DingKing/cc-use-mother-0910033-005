"""核心引擎：规则版本、市场状态屏障、撮合事务边界与中止恢复流程。

并发模型：单把可重入锁串行化所有状态变更。撮合成交流程以"事务"为单位：
每笔候选成交先做屏障检查与异常检测，命中即在提交前回滚（订单原样留在
队列），未命中才提交生成待清算成交。市场屏障永远在事务间隙生效，因此
不会留下状态不明的成交。
"""
from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable

from .audit import AuditLog
from .detector import AnomalyDetector
from .models import (
    MarketState,
    Order,
    OrderSide,
    OrderStatus,
    RuleVersion,
    Trade,
    new_id,
    now_ms,
)

TRADING = "TRADING"
CANCEL_ONLY = "CANCEL_ONLY"
LIMIT_ONLY = "LIMIT_ONLY"
# 限价恢复阶段相对最近成交价的允许带宽
LIMIT_PHASE_BAND = 0.02

# 审计动作常量
AUDIT_RULE_PUBLISHED = "RULE_PUBLISHED"
AUDIT_HALT_TRIGGERED = "HALT_TRIGGERED"
AUDIT_REPEATED_TRIGGER = "REPEATED_TRIGGER_IGNORED"
AUDIT_INCIDENT_REVIEW = "INCIDENT_REVIEW"
AUDIT_STAGE_ADVANCE = "STAGE_ADVANCE"
AUDIT_FALSE_POSITIVE = "FALSE_POSITIVE_REVOKED"
AUDIT_TX_ROLLED_BACK = "TX_ROLLED_BACK"
AUDIT_TRADE_SETTLED = "TRADE_SETTLED"
AUDIT_ORDER_CANCELLED = "ORDER_CANCELLED"


class EngineError(Exception):
    """语义明确的业务错误，HTTP 层映射为 4xx。"""

    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


@dataclass
class Incident:
    """一次中止事件及其复核 / 恢复轨迹。"""

    incident_id: str
    opened_at: int
    rule_version: int
    rule_fingerprint: str
    rule_snapshot: dict[str, Any]
    symbol: str
    trigger: dict[str, Any]            # 检测器返回的完整证据
    candidate_order_ids: list[str]
    trigger_tx_id: str
    state: str = "OPEN"                 # OPEN / CONFIRMED / REVOKED_FALSE_POSITIVE / RECOVERED
    review: dict[str, Any] | None = None
    recovery_stage: int = 0             # 0=已中止仅撤单 1=仅撤单(执行中) 2=限价 3=全量
    sealed_at: int | None = None
    audit_event_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "opened_at": self.opened_at,
            "state": self.state,
            "symbol": self.symbol,
            "rule_version": self.rule_version,
            "rule_fingerprint": self.rule_fingerprint,
            "rule_snapshot": self.rule_snapshot,
            "trigger": self.trigger,
            "candidate_order_ids": self.candidate_order_ids,
            "trigger_tx_id": self.trigger_tx_id,
            "review": self.review,
            "recovery_stage": self.recovery_stage,
            "sealed_at": self.sealed_at,
            "audit_event_ids": self.audit_event_ids,
            # 暂停依据（人类可读）
            "halt_reason": explain_halt(self),
        }


def explain_halt(inc: Incident) -> str:
    kinds = "、".join(b["kind"] for b in inc.trigger.get("breaches", []))
    detail = "; ".join(
        f"{b['kind']} 观测={b['observed']} 阈值={b['threshold']}"
        for b in inc.trigger.get("breaches", [])
    )
    return (
        f"规则 v{inc.rule_version}({inc.rule_fingerprint}) 在 {inc.symbol} 上命中 {kinds}；"
        f"{detail}；事务 {inc.trigger_tx_id} 已在提交前回滚，屏障切换为 CANCEL_ONLY。"
    )


class MarketHaltEngine:
    def __init__(self, clock: Callable[[], int] = now_ms) -> None:
        self._clock = clock
        self._lock = threading.RLock()
        self.audit = AuditLog(clock)
        self.detector = AnomalyDetector(clock)

        # 契约五状态描述事件生命周期；无活动事件正常撮合时以 phase=TRADING
        # 表示，market_state 停留在最近一个生命周期状态（初始 草稿）。
        self.market_state: MarketState = MarketState.DRAFT
        self.phase: str = TRADING
        self.rules: list[RuleVersion] = []

        self.orders: dict[str, Order] = {}
        self._buy_book: dict[str, deque[Order]] = {}
        self._sell_book: dict[str, deque[Order]] = {}
        self._seq = 0

        self.trades: dict[str, Trade] = {}
        self.incidents: dict[str, Incident] = {}
        self.active_incident_id: str | None = None

    # ------------------------------------------------------------------ 规则

    def publish_rule(
        self,
        actor: str,
        window_ms: int,
        max_price_move: float,
        max_volume_multiple: float,
        note: str = "",
    ) -> dict[str, Any]:
        with self._lock:
            if window_ms <= 0:
                raise EngineError("INVALID_RULE", "window_ms 必须为正整数")
            if not 0 < max_price_move <= 1:
                raise EngineError("INVALID_RULE", "max_price_move 必须在 (0,1] 区间")
            if max_volume_multiple < 0:
                raise EngineError("INVALID_RULE", "max_volume_multiple 不能为负")
            version = len(self.rules) + 1
            rule = RuleVersion(
                version=version,
                published_at=self._clock(),
                published_by=actor,
                window_ms=window_ms,
                max_price_move=max_price_move,
                max_volume_multiple=max_volume_multiple,
                note=note,
            )
            self.rules.append(rule)
            self._audit(AUDIT_RULE_PUBLISHED, actor, {"rule": rule.to_dict()})
            return rule.to_dict()

    def _audit(self, action: str, actor: str, detail: dict[str, Any], **kw: Any) -> dict[str, Any]:
        event = self.audit.append(action, actor, detail, **kw)
        inc_id = kw.get("incident_id")
        if inc_id and inc_id in self.incidents:
            self.incidents[inc_id].audit_event_ids.append(event["event_id"])
        return event

    def _current_rule(self) -> RuleVersion:
        if not self.rules:
            raise EngineError("NO_RULE", "尚未发布异常检测规则版本", 409)
        return self.rules[-1]

    # ------------------------------------------------------------------ 订单

    def submit_order(
        self, order_id: str, symbol: str, side: str, price: float, quantity: int, actor: str
    ) -> dict[str, Any]:
        with self._lock:
            if order_id in self.orders:
                raise EngineError("DUPLICATE_ORDER", f"订单 {order_id} 已存在", 409)
            if price <= 0 or quantity <= 0:
                raise EngineError("INVALID_ORDER", "价格与数量必须为正")
            try:
                side_enum = OrderSide(side)
            except ValueError:
                raise EngineError("INVALID_ORDER", f"未知方向 {side}")
            self._seq += 1
            order = Order(
                order_id=order_id,
                symbol=symbol,
                side=side_enum,
                price=price,
                quantity=quantity,
                received_at=self._clock(),
                client_seq=self._seq,
            )
            rule_v = self.rules[-1].version if self.rules else None
            order.dispose(
                "ENQUEUED",
                f"订单进入 {symbol} 队列；入队时屏障 {self.phase}，规则版本 v{rule_v or '-'}",
            )
            self.orders[order_id] = order
            book = (
                self._buy_book if side_enum is OrderSide.BUY else self._sell_book
            ).setdefault(symbol, deque())
            book.append(order)
            matches = self._match(symbol) if self.rules else []
            return {
                "order": self.orders[order_id].to_dict(),
                "matches": matches,
                "halted": self.active_incident_id is not None,
                "active_incident_id": self.active_incident_id,
            }

    def _match(self, symbol: str) -> list[dict[str, Any]]:
        """持锁前提下持续撮合，直到不能成交、屏障落下或异常命中。"""
        results: list[dict[str, Any]] = []
        while True:
            buy, sell = self._peek_pair(symbol)
            if buy is None or sell is None or buy.price < sell.price:
                break
            tx_id = new_id("tx")
            resting, _incoming = (
                (sell, buy) if sell.client_seq < buy.client_seq else (buy, sell)
            )
            qty = min(buy.quantity - buy.filled_quantity, sell.quantity - sell.filled_quantity)
            price = resting.price
            if qty <= 0:
                self._discard_filled(symbol)
                continue

            # ---- 事务边界 1：市场状态屏障（一致边界停止新撮合）----
            if self.phase == CANCEL_ONLY:
                detail = f"事务 {tx_id} 因屏障 {self.phase} 未启动，订单保留在队列"
                buy.dispose("TX_GATED", detail, ref=self.active_incident_id)
                sell.dispose("TX_GATED", detail, ref=self.active_incident_id)
                results.append({"tx_id": tx_id, "outcome": "GATED", "phase": self.phase})
                break

            # 限价恢复阶段：候选成交价必须在最近成交价带宽内
            if self.phase == LIMIT_ONLY:
                ref = self.detector.last_price(symbol)
                if ref is not None and abs(price - ref) / ref > LIMIT_PHASE_BAND:
                    detail = (
                        f"事务 {tx_id} 候选价 {price} 超出限价带宽 "
                        f"±{LIMIT_PHASE_BAND:.0%}（参考价 {ref}），订单保留队列"
                    )
                    buy.dispose("TX_GATED", detail, ref=self.active_incident_id)
                    sell.dispose("TX_GATED", detail, ref=self.active_incident_id)
                    results.append(
                        {"tx_id": tx_id, "outcome": "GATED_LIMIT_BAND", "price": price}
                    )
                    break

            # ---- 事务边界 2：异常检测（提交前评估，命中即回滚）----
            rule = self._current_rule()
            verdict = self.detector.evaluate(symbol, price, qty, rule)
            if verdict["breached"]:
                self._rollback_and_halt(tx_id, symbol, price, qty, rule, verdict, [buy, sell])
                results.append(
                    {
                        "tx_id": tx_id,
                        "outcome": "ROLLED_BACK_HALT",
                        "incident_id": self.active_incident_id,
                        "breaches": verdict["breaches"],
                    }
                )
                break

            # ---- 提交：生成待清算成交 ----
            trade = self._commit(tx_id, symbol, price, qty, buy, sell, rule)
            results.append(
                {
                    "tx_id": tx_id,
                    "outcome": "COMMITTED",
                    "trade_id": trade.trade_id,
                    "price": price,
                    "quantity": qty,
                }
            )
            self._discard_filled(symbol)
        return results

    def _peek_pair(self, symbol: str) -> tuple[Order | None, Order | None]:
        buys = self._buy_book.get(symbol)
        sells = self._sell_book.get(symbol)
        return (buys[0] if buys else None), (sells[0] if sells else None)

    def _rollback_and_halt(
        self,
        tx_id: str,
        symbol: str,
        price: float,
        qty: int,
        rule: RuleVersion,
        verdict: dict[str, Any],
        candidates: list[Order],
        *,
        source: str = "MATCH_TX",
    ) -> None:
        """命中异常后的统一收口。

        MATCH_TX：候选成交不落账——不生成 Trade、不记录行情，订单保持
        QUEUED 留在队首；MARKET_SIGNAL：外部行情馈送信号，无候选事务。
        已有活动事件时任何来源都只追加审计（重复触发幂等）。
        """
        ids = [o.order_id for o in candidates]

        if self.active_incident_id is not None:
            # 重复触发：屏障与状态不变，仅追加审计，保证幂等
            inc = self.incidents[self.active_incident_id]
            rollback_detail = (
                f"事务 {tx_id} 命中规则 v{rule.version} 异常 "
                f"{[b['kind'] for b in verdict['breaches']]}，提交前回滚，订单保留队列"
            )
            for o in candidates:
                o.dispose("TX_ROLLED_BACK", rollback_detail, ref=inc.incident_id)
                o.dispose(
                    "REPEATED_TRIGGER_IGNORED",
                    f"事件 {inc.incident_id} 已中止，本次重复触发幂等忽略，仅记录审计",
                    ref=inc.incident_id,
                )
            rollback_audit = (
                [{
                    "action": AUDIT_TX_ROLLED_BACK,
                    "detail": {
                        "tx_id": tx_id,
                        "candidate_order_ids": ids,
                        "rollback_basis": "提交前检测命中",
                    },
                }]
                if source == "MATCH_TX"
                else []
            )
            for item in rollback_audit:
                self._audit(
                    item["action"], "SYSTEM", item["detail"],
                    incident_id=inc.incident_id, ref=tx_id,
                )
            self._audit(
                AUDIT_REPEATED_TRIGGER,
                "SYSTEM",
                {
                    "tx_id": tx_id,
                    "source": source,
                    "symbol": symbol,
                    "candidate_price": price,
                    "candidate_quantity": qty,
                    "candidate_order_ids": ids,
                    "breaches": verdict["breaches"],
                    "rule_version": rule.version,
                    "message": "中止已生效，重复触发被幂等忽略，屏障保持不变",
                },
                incident_id=inc.incident_id,
            )
            return

        inc = Incident(
            incident_id=new_id("inc"),
            opened_at=self._clock(),
            rule_version=rule.version,
            rule_fingerprint=rule.fingerprint,
            rule_snapshot=rule.to_dict(),
            symbol=symbol,
            trigger=verdict,
            candidate_order_ids=ids,
            trigger_tx_id=tx_id,
        )
        self.incidents[inc.incident_id] = inc
        self.active_incident_id = inc.incident_id
        self.market_state = MarketState.PENDING
        self.phase = CANCEL_ONLY
        rollback_detail = (
            f"事务 {tx_id} 命中规则 v{rule.version} 异常 "
            f"{[b['kind'] for b in verdict['breaches']]}，提交前回滚，订单保留队列"
        )
        for o in candidates:
            o.dispose("TX_ROLLED_BACK", rollback_detail, ref=inc.incident_id)
        self._audit(
            AUDIT_HALT_TRIGGERED,
            "SYSTEM",
            {
                "tx_id": tx_id,
                "source": source,
                "symbol": symbol,
                "candidate_price": price,
                "candidate_quantity": qty,
                "candidate_order_ids": ids,
                "verdict": verdict,
                "phase_after": CANCEL_ONLY,
            },
            incident_id=inc.incident_id,
            ref=tx_id,
        )
        if source == "MATCH_TX":
            self._audit(
                AUDIT_TX_ROLLED_BACK,
                "SYSTEM",
                {"tx_id": tx_id, "candidate_order_ids": ids, "rollback_basis": "提交前检测命中"},
                incident_id=inc.incident_id,
                ref=tx_id,
            )

    def ingest_market_signal(
        self, symbol: str, price: float, quantity: int, actor: str = "MARKET_FEED"
    ) -> dict[str, Any]:
        """外部行情馈送的异常信号入口（与撮合计费相互独立）。

        屏障落下后撮合路径无法再产生候选事务，重复触发主要经由本入口：
        评估命中则按与撮合事务相同的收口处理（首次即中止，重复仅审计）。
        评估未命中返回 breached=False，不做任何状态变更。
        """
        with self._lock:
            rule = self._current_rule()
            verdict = self.detector.evaluate(symbol, price, quantity, rule)
            if not verdict["breached"]:
                return {
                    "breached": False,
                    "active_incident_id": self.active_incident_id,
                    "breaches": [],
                }
            tx_id = new_id("sig")
            before = self.active_incident_id
            self._rollback_and_halt(
                tx_id, symbol, price, quantity, rule, verdict, [], source="MARKET_SIGNAL"
            )
            return {
                "breached": True,
                "repeated": before is not None,
                "incident_id": self.active_incident_id,
                "breaches": verdict["breaches"],
            }

    def _commit(
        self,
        tx_id: str,
        symbol: str,
        price: float,
        qty: int,
        buy: Order,
        sell: Order,
        rule: RuleVersion,
    ) -> Trade:
        trade = Trade(
            trade_id=new_id("trd"),
            symbol=symbol,
            price=price,
            quantity=qty,
            buy_order_id=buy.order_id,
            sell_order_id=sell.order_id,
            matched_at=self._clock(),
            rule_version=rule.version,
            rule_fingerprint=rule.fingerprint,
            tx_id=tx_id,
        )
        self.trades[trade.trade_id] = trade
        self.detector.record(symbol, price, qty, at=trade.matched_at)
        for o, counter in ((buy, sell.order_id), (sell, buy.order_id)):
            o.filled_quantity += qty
            o.trade_ids.append(trade.trade_id)
            o.matched_price = price
            o.rule_version_at_match = rule.version
            o.dispose(
                "MATCHED",
                f"事务 {tx_id} 提交，待清算成交 {trade.trade_id}，价格 {price} 数量 {qty}，"
                f"累计成交 {o.filled_quantity}/{o.quantity}，对手单 {counter}",
                ref=trade.trade_id,
            )
            if o.filled_quantity >= o.quantity:
                o.status = OrderStatus.MATCHED
        return trade

    def _discard_filled(self, symbol: str) -> None:
        for book in (self._buy_book.get(symbol), self._sell_book.get(symbol)):
            while book and book[0].filled_quantity >= book[0].quantity:
                book.popleft()

    def cancel_order(self, order_id: str, actor: str, reason: str = "") -> dict[str, Any]:
        with self._lock:
            order = self._get_order(order_id)
            if order.status is not OrderStatus.QUEUED:
                raise EngineError(
                    "ORDER_NOT_CANCELLABLE",
                    f"订单状态 {order.status.value} 不可撤销（仅队列中的未成交 / 部分成交剩余可撤）",
                    409,
                )
            symbol = order.symbol
            book = (
                self._buy_book if order.side is OrderSide.BUY else self._sell_book
            ).get(symbol)
            if book is not None and order in book:
                book.remove(order)
            order.status = OrderStatus.CANCELLED
            order.dispose(
                "CANCELLED",
                f"由 {actor} 撤销，剩余 {order.quantity - order.filled_quantity} 股；"
                f"原因：{reason or '无'}；屏障 {self.phase}",
            )
            self._audit(
                AUDIT_ORDER_CANCELLED,
                actor,
                {"order_id": order_id, "reason": reason, "phase": self.phase},
                incident_id=self.active_incident_id,
            )
            return order.to_dict()

    # ------------------------------------------------------------------ 清算

    def settle_trade(self, trade_id: str, actor: str) -> dict[str, Any]:
        with self._lock:
            trade = self.trades.get(trade_id)
            if trade is None:
                raise EngineError("TRADE_NOT_FOUND", f"成交 {trade_id} 不存在", 404)
            if trade.settled:
                raise EngineError("TRADE_ALREADY_SETTLED", f"成交 {trade_id} 已清算", 409)
            trade.settled = True
            for oid in (trade.buy_order_id, trade.sell_order_id):
                o = self.orders[oid]
                o.dispose("SETTLED", f"成交 {trade_id} 清算完成", ref=trade_id)
                if o.filled_quantity >= o.quantity and all(
                    self.trades[t].settled for t in o.trade_ids
                ):
                    o.status = OrderStatus.SETTLED
            self._audit(
                AUDIT_TRADE_SETTLED,
                actor,
                {"trade_id": trade_id},
                incident_id=self._incident_of_trade(trade),
            )
            return trade.to_dict()

    def _incident_of_trade(self, trade: Trade) -> str | None:
        for inc in self.incidents.values():
            if trade.symbol == inc.symbol and trade.matched_at < inc.opened_at:
                return inc.incident_id
        return None

    # ---------------------------------------------------------- 复核与恢复

    def review_incident(
        self, incident_id: str, actor: str, verdict: str, comment: str = ""
    ) -> dict[str, Any]:
        """人工复核：CONFIRMED 确认异常；FALSE_POSITIVE 判定误报并撤销中止。"""
        with self._lock:
            inc = self._get_incident(incident_id)
            if inc.state != "OPEN":
                raise EngineError(
                    "INCIDENT_NOT_REVIEWABLE",
                    f"事件状态 {inc.state} 不可复核",
                    409,
                )
            if verdict == "CONFIRMED":
                inc.review = {
                    "at": self._clock(),
                    "by": actor,
                    "verdict": verdict,
                    "comment": comment,
                }
                inc.state = "CONFIRMED"
                self.market_state = MarketState.CONFIRMED
                self._audit(
                    AUDIT_INCIDENT_REVIEW,
                    actor,
                    {"verdict": verdict, "comment": comment, "phase_kept": CANCEL_ONLY},
                    incident_id=inc.incident_id,
                )
            elif verdict == "FALSE_POSITIVE":
                self._revoke_false_positive(inc, actor, comment)
            else:
                raise EngineError(
                    "INVALID_VERDICT", "verdict 必须为 CONFIRMED 或 FALSE_POSITIVE"
                )
            return inc.to_dict()

    def _revoke_false_positive(self, inc: Incident, actor: str, comment: str) -> None:
        inc.review = {
            "at": self._clock(),
            "by": actor,
            "verdict": "FALSE_POSITIVE",
            "comment": comment,
        }
        inc.state = "REVOKED_FALSE_POSITIVE"
        inc.sealed_at = self._clock()
        self.market_state = MarketState.SEALED
        self.active_incident_id = None
        self.phase = TRADING
        revived = self._revive_orders(inc.incident_id)
        self._audit(
            AUDIT_FALSE_POSITIVE,
            actor,
            {
                "comment": comment,
                "candidate_order_ids": inc.candidate_order_ids,
                "revived_order_count": len(revived),
                "phase_after": TRADING,
            },
            incident_id=inc.incident_id,
        )

    def _revive_orders(self, incident_id: str) -> list[str]:
        revived: list[str] = []
        for oid, o in self.orders.items():
            if o.status is not OrderStatus.QUEUED:
                continue
            affected = any(d.get("ref") == incident_id for d in o.dispositions)
            revived_already = any(
                d["action"] == "HALT_REVIVED" and d.get("ref") == incident_id
                for d in o.dispositions
            )
            if affected and not revived_already:
                o.dispose(
                    "HALT_REVIVED",
                    f"事件 {incident_id} 结束，订单恢复撮合资格",
                    ref=incident_id,
                )
                revived.append(oid)
        return revived

    def advance_stage(self, incident_id: str, actor: str, comment: str = "") -> dict[str, Any]:
        """分阶段恢复：1=仅撤单(执行中) → 2=限价恢复 → 3=全量恢复并封存。"""
        with self._lock:
            inc = self._get_incident(incident_id)
            if inc.state != "CONFIRMED":
                raise EngineError(
                    "STAGE_ADVANCE_DENIED",
                    f"事件状态 {inc.state}：仅复核确认后的事件可分阶段恢复",
                    409,
                )
            next_stage = inc.recovery_stage + 1
            if next_stage == 1:
                self.phase = CANCEL_ONLY
                self.market_state = MarketState.RESUMING
                message = "阶段 1/3：仅撤单，暂停撮合继续生效"
            elif next_stage == 2:
                self.phase = LIMIT_ONLY
                self.market_state = MarketState.RESUMING
                message = f"阶段 2/3：限价恢复，成交价带宽 ±{LIMIT_PHASE_BAND:.0%}"
            elif next_stage == 3:
                self.phase = TRADING
                inc.state = "RECOVERED"
                inc.sealed_at = self._clock()
                self.market_state = MarketState.SEALED
                self.active_incident_id = None
                revived = self._revive_orders(inc.incident_id)
                message = f"阶段 3/3：全量恢复，事件封存；{len(revived)} 笔队列订单恢复撮合资格"
            else:
                raise EngineError("STAGE_EXHAUSTED", "恢复阶段已完成", 409)
            inc.recovery_stage = next_stage
            self._audit(
                AUDIT_STAGE_ADVANCE,
                actor,
                {
                    "stage": next_stage,
                    "comment": comment,
                    "phase_after": self.phase,
                    "message": message,
                },
                incident_id=inc.incident_id,
            )
            return {"incident": inc.to_dict(), "message": message}

    # ------------------------------------------------------------------ 查询

    def _get_order(self, order_id: str) -> Order:
        order = self.orders.get(order_id)
        if order is None:
            raise EngineError("ORDER_NOT_FOUND", f"订单 {order_id} 不存在", 404)
        return order

    def _get_incident(self, incident_id: str) -> Incident:
        inc = self.incidents.get(incident_id)
        if inc is None:
            raise EngineError("INCIDENT_NOT_FOUND", f"事件 {incident_id} 不存在", 404)
        return inc

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "market_state": self.market_state.value,
                "matching_phase": self.phase,
                "active_incident_id": self.active_incident_id,
                "current_rule_version": self.rules[-1].version if self.rules else None,
                "current_rule_fingerprint": (
                    self.rules[-1].fingerprint if self.rules else None
                ),
                "queued": {
                    sym: {
                        "buy": sum(
                            o.quantity - o.filled_quantity
                            for o in self._buy_book.get(sym, ())
                        ),
                        "sell": sum(
                            o.quantity - o.filled_quantity
                            for o in self._sell_book.get(sym, ())
                        ),
                    }
                    for sym in set(self._buy_book) | set(self._sell_book)
                },
                "counts": {
                    "rules": len(self.rules),
                    "orders": len(self.orders),
                    "trades_pending_settlement": sum(
                        1 for t in self.trades.values() if not t.settled
                    ),
                    "trades_settled": sum(1 for t in self.trades.values() if t.settled),
                    "incidents": len(self.incidents),
                },
            }

    def list_rules(self) -> list[dict[str, Any]]:
        with self._lock:
            return [r.to_dict() for r in self.rules]

    def list_incidents(self) -> list[dict[str, Any]]:
        with self._lock:
            return [inc.to_dict() for inc in self.incidents.values()]

    def list_trades(self, pending_only: bool = False) -> list[dict[str, Any]]:
        with self._lock:
            return [
                t.to_dict()
                for t in self.trades.values()
                if not pending_only or not t.settled
            ]

    def list_audit(self, **filters: Any) -> list[dict[str, Any]]:
        with self._lock:
            return self.audit.list(**filters)

    def order_disposition_report(self, order_id: str) -> dict[str, Any]:
        """单笔订单的完整处置说明（含它经历的暂停依据）。"""
        with self._lock:
            order = self._get_order(order_id)
            refs = {
                d.get("ref")
                for d in order.dispositions
                if isinstance(d.get("ref"), str) and d["ref"].startswith("inc_")
            }
            return {
                "order": order.to_dict(),
                "halt_basis": [
                    {
                        "incident_id": self.incidents[r].incident_id,
                        "state": self.incidents[r].state,
                        "reason": explain_halt(self.incidents[r]),
                    }
                    for r in refs
                    if r in self.incidents
                ],
            }

    def incident_orders_report(self, incident_id: str) -> dict[str, Any]:
        """事件影响的每笔订单及其处置轨迹。"""
        with self._lock:
            inc = self._get_incident(incident_id)
            orders = []
            for oid, o in self.orders.items():
                trail = [d for d in o.dispositions if d.get("ref") == inc.incident_id]
                if oid in inc.candidate_order_ids or trail:
                    orders.append(
                        {
                            "order_id": oid,
                            "status": o.status.value,
                            "filled_quantity": o.filled_quantity,
                            "is_trigger_candidate": oid in inc.candidate_order_ids,
                            "dispositions": trail,
                        }
                    )
            return {
                "incident_id": inc.incident_id,
                "halt_reason": explain_halt(inc),
                "orders": orders,
            }
