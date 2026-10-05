"""市场价格异常中止引擎：状态屏障、撮合事务边界与分阶段恢复。

一致性设计：

- 引擎在同一把锁内完成整个撮合事务，因此人工触发中止总是落在
  事务边界上；自动触发（规则违例）由规则的 BoundaryPolicy 决定
  对当前事务执行提交（COMMIT_CURRENT）还是回滚（ROLLBACK_CURRENT），
  绝不留下状态不明的成交。
- 中止期间停止新撮合，已进入队列的订单保留（PARKED），恢复时按
  原订单序列重新进场。
- 所有状态变化写入审计日志；重复触发（含幂等重放）只记录不新建事件。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from .audit import AuditLog
from .book import OrderBook
from .errors import DomainError
from .models import (
    RECOVERY_PHASES,
    BoundaryPolicy,
    Breach,
    Disposition,
    HaltIncident,
    IncidentState,
    MarketState,
    Order,
    OrderStatus,
    Side,
    Trade,
    TradeStatus,
)
from .rules import Detector, RuleRegistry, RuleSet

#: 各操作允许的角色（角色集合来自领域契约）
ROLE_ORDER = ("企业申报员",)
ROLE_CLEARING = ("核算专员",)
ROLE_OPERATE = ("交易运营员",)
ROLE_REVIEW = ("监管审计员",)
ROLE_RULE_WRITE = ("交易运营员", "监管审计员")
ROLE_HALT_MANUAL = ("交易运营员", "监管审计员")


@dataclass
class _Fill:
    resting: Order
    price: float
    qty: int
    popped: bool


@dataclass
class _Tx:
    """一次撮合事务：记录全部簿内变更，支持整体提交或回滚。"""

    tx_id: str
    incoming: Order
    fills: list[_Fill] = field(default_factory=list)


class _AnomalyAbort(Exception):
    """撮合过程中检测到异常，携带违例证据中断当前事务。"""

    def __init__(self, breach: Breach) -> None:
        super().__init__(breach.detail)
        self.breach = breach


class MarketEngine:
    def __init__(self, now_fn=None) -> None:
        self._now_fn = now_fn or time.time
        self._lock = threading.RLock()
        self.audit = AuditLog(self._now)
        self.rules = RuleRegistry()
        self.detector = Detector(self._now)
        self.market_state = MarketState.OPEN
        self.recovery_phase = 0
        self.active_halt_id: str | None = None
        self._books: dict[str, OrderBook] = {}
        self._orders: dict[str, Order] = {}
        self._client_index: dict[str, str] = {}
        self._parked: list[str] = []  # 滞留订单（保持队列顺序）
        self._trades: dict[str, Trade] = {}
        self._incidents: dict[str, HaltIncident] = {}
        self._halt_idem: dict[str, str] = {}
        self._seq = 0  # 全局事件序列（订单、成交、事件共用）
        self._order_seq = 0
        self._trade_seq = 0
        self._halt_seq = 0
        self._tx_seq = 0

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _now(self) -> float:
        return self._now_fn()

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    @staticmethod
    def _require_actor(actor: str | None, role: str | None) -> None:
        if not actor or not role:
            raise DomainError("ACTOR_REQUIRED", "请求必须携带 actor 与 role")

    @staticmethod
    def _require_role(role: str, allowed: tuple[str, ...], action: str) -> None:
        if role not in allowed:
            raise DomainError("ROLE_FORBIDDEN", f"{action}需要角色：{'/'.join(allowed)}")

    def _dispose(self, order: Order, action: str, detail: str, ref: str | None = None) -> None:
        order.dispositions.append(Disposition(ts=self._now(), action=action, detail=detail, ref=ref))

    def _transition_incident(
        self, halt: HaltIncident, actor: str, role: str, to_state: IncidentState, action: str, detail: str
    ) -> None:
        halt.history.append(
            {
                "ts": self._now(),
                "actor": actor,
                "role": role,
                "action": action,
                "from_state": halt.state.value,
                "to_state": to_state.value,
                "detail": detail,
            }
        )
        halt.state = to_state
        self.audit.record(actor, role, action, halt.halt_id, from_state=halt.history[-1]["from_state"],
                          to_state=to_state.value, detail=detail)

    # ------------------------------------------------------------------
    # 规则版本
    # ------------------------------------------------------------------

    def register_rule(self, *, actor, role, price_deviation_pct=None, window_seconds=60,
                      max_window_volume=None, volume_multiple=None, baseline_windows=5,
                      min_baseline_volume=0, boundary_policy=BoundaryPolicy.ROLLBACK_CURRENT) -> RuleSet:
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_RULE_WRITE, "登记规则")
            rule = self.rules.register(
                price_deviation_pct=price_deviation_pct,
                window_seconds=window_seconds,
                max_window_volume=max_window_volume,
                volume_multiple=volume_multiple,
                baseline_windows=baseline_windows,
                min_baseline_volume=min_baseline_volume,
                boundary_policy=BoundaryPolicy(boundary_policy),
                created_by=actor,
                now=self._now(),
            )
            self.audit.record(actor, role, "RULE_REGISTERED", f"rule-v{rule.version}", **rule.to_dict())
            return rule

    def activate_rule(self, *, actor, role, version: int) -> RuleSet:
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_REVIEW, "激活规则")
            previous = self.rules.active
            rule = self.rules.activate(version, actor, self._now())
            self.audit.record(actor, role, "RULE_ACTIVATED", f"rule-v{rule.version}",
                              superseded=previous.version if previous else None)
            return rule

    def list_rules(self) -> list[RuleSet]:
        with self._lock:
            return self.rules.list()

    def set_reference_price(self, *, actor, role, symbol: str, price: float) -> None:
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_OPERATE, "设定参考价")
            if not symbol or price <= 0:
                raise DomainError("INVALID_REFERENCE", "标的不能为空且参考价必须为正")
            self.detector.set_reference(symbol, price)
            self.audit.record(actor, role, "REFERENCE_SET", symbol, price=price)

    # ------------------------------------------------------------------
    # 报单与撤单
    # ------------------------------------------------------------------

    def submit_order(self, *, actor, role, client_order_id, symbol, side, price, quantity) -> dict:
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_ORDER, "报单")
            if not client_order_id:
                raise DomainError("INVALID_ORDER", "client_order_id 不能为空（幂等键）")
            existing = self._client_index.get(client_order_id)
            if existing is not None:
                # 幂等重放：返回原订单，不重复入账
                return {"order": self._orders[existing], "trades": [], "halt": None, "deduplicated": True}
            try:
                side = Side(side)
            except ValueError:
                raise DomainError("INVALID_ORDER", "side 必须是 BUY 或 SELL") from None
            if not symbol or not isinstance(quantity, int) or quantity <= 0 \
                    or not isinstance(price, (int, float)) or price <= 0:
                raise DomainError("INVALID_ORDER", "标的不能为空，价格与数量必须为正数")

            self._order_seq += 1
            order = Order(
                order_id=f"ORD-{self._order_seq:06d}",
                client_order_id=client_order_id,
                seq=self._next_seq(),
                symbol=symbol,
                side=side,
                price=float(price),
                quantity=quantity,
                remaining=quantity,
                status=OrderStatus.QUEUED,
                actor=actor,
                created_at=self._now(),
            )

            # 市场状态屏障
            if self.market_state is MarketState.HALTED:
                self._register_order(order, role)
                self._park_order(order, self.active_halt_id, "市场中止，订单保留在队列中等待恢复")
                return {"order": order, "trades": [], "halt": self._incidents[self.active_halt_id],
                        "deduplicated": False}
            if self.market_state is MarketState.RECOVERING:
                if self.recovery_phase <= 1:
                    raise DomainError("MARKET_FROZEN", f"恢复阶段「{RECOVERY_PHASES[1]}」不接收新报单")
                self._register_order(order, role)
                self._park_order(order, self.active_halt_id,
                                 f"恢复阶段「{RECOVERY_PHASES[2]}」，订单保留待撮合")
                return {"order": order, "trades": [], "halt": self._incidents[self.active_halt_id],
                        "deduplicated": False}

            # OPEN：先做单前价格检查，再进入撮合事务
            self._register_order(order, role)
            rule = self.rules.active
            if rule is not None:
                breach = self.detector.check_order_price(rule, symbol, order.price)
                if breach is not None:
                    halt = self._trigger_halt_locked(
                        actor="system", role="系统", source="AUTO_RULE",
                        reason=breach.detail, breaches=[breach],
                        rule_version=rule.version, boundary_action="NO_OPEN_TX",
                        tx_id=None, idempotency_key=None,
                    )
                    self._park_order(order, halt.halt_id, "触发价格异常中止，订单保留在队列中")
                    return {"order": order, "trades": [], "halt": halt, "deduplicated": False}
            trades, halt = self._execute_matching(order, rule)
            return {"order": order, "trades": trades, "halt": halt, "deduplicated": False}

    def _register_order(self, order: Order, role: str) -> None:
        self._orders[order.order_id] = order
        self._client_index[order.client_order_id] = order.order_id
        self._books.setdefault(order.symbol, OrderBook(order.symbol))
        self._dispose(order, "ACCEPTED", "订单受理")
        self.audit.record(order.actor, role, "ORDER_ACCEPTED", order.order_id,
                          client_order_id=order.client_order_id, symbol=order.symbol,
                          side=order.side.value, price=order.price, quantity=order.quantity,
                          seq=order.seq)

    def _park_order(self, order: Order, halt_id: str | None, reason: str) -> None:
        order.status = OrderStatus.PARKED
        self._parked.append(order.order_id)
        self._dispose(order, "PARKED", reason, ref=halt_id)
        self.audit.record("system", "系统", "ORDER_PARKED", order.order_id, halt_id=halt_id, reason=reason)

    def cancel_order(self, *, actor, role, order_id: str) -> Order:
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_ORDER, "撤单")
            order = self._orders.get(order_id)
            if order is None:
                raise DomainError("ORDER_NOT_FOUND", f"订单不存在：{order_id}")
            if order.status in (OrderStatus.FILLED, OrderStatus.CANCELLED):
                raise DomainError("INVALID_STATE", f"订单已终结（{order.status.value}），不能撤单")
            if self.market_state is MarketState.HALTED:
                raise DomainError("MARKET_FROZEN", "中止期间冻结撤单，待恢复阶段「仅撤单」开放")
            if order.status is OrderStatus.PARKED:
                self._parked.remove(order.order_id)
            else:
                self._books[order.symbol].remove(order)
            order.status = OrderStatus.CANCELLED
            self._dispose(order, "CANCELLED", f"{actor} 撤单")
            self.audit.record(actor, role, "ORDER_CANCELLED", order.order_id,
                              market_state=self.market_state.value, recovery_phase=self.recovery_phase)
            return order

    # ------------------------------------------------------------------
    # 撮合事务
    # ------------------------------------------------------------------

    def _execute_matching(self, order: Order, rule: RuleSet | None) -> tuple[list[Trade], HaltIncident | None]:
        """为订单执行一次撮合事务；触发异常时在事务边界提交或回滚。"""
        self._tx_seq += 1
        tx = _Tx(tx_id=f"TX-{self._tx_seq:06d}", incoming=order)
        book = self._books[order.symbol]
        self.detector.begin()
        self.audit.record("system", "系统", "TX_BEGIN", tx.tx_id, order_id=order.order_id)
        breach: Breach | None = None
        try:
            book.match(order, lambda resting, price, qty, popped:
                       self._on_fill(tx, rule, resting, price, qty, popped))
        except _AnomalyAbort as exc:
            breach = exc.breach

        if breach is None:
            trades = self._commit_tx(tx)
            if order.remaining > 0:
                book.rest(order)
                order.status = (OrderStatus.PARTIALLY_FILLED if tx.fills else OrderStatus.QUEUED)
                self._dispose(order, "QUEUED", f"剩余 {order.remaining} 进入订单队列", ref=tx.tx_id)
            else:
                order.status = OrderStatus.FILLED
                self._dispose(order, "FILLED", "订单完全成交", ref=tx.tx_id)
            return trades, None

        # 触发中止：在一致边界处置当前事务
        policy = rule.boundary_policy if rule else BoundaryPolicy.ROLLBACK_CURRENT
        if policy is BoundaryPolicy.ROLLBACK_CURRENT:
            self._rollback_tx(tx, breach)
            trades = []
            boundary_action = "ROLLED_BACK_TX"
        else:
            trades = self._commit_tx(tx)
            boundary_action = "COMMITTED_TX"
        halt = self._trigger_halt_locked(
            actor="system", role="系统", source="AUTO_RULE",
            reason=breach.detail, breaches=[breach],
            rule_version=rule.version if rule else None,
            boundary_action=boundary_action, tx_id=tx.tx_id, idempotency_key=None,
        )
        if order.remaining > 0:
            self._park_order(order, halt.halt_id,
                             f"中止触发于事务 {tx.tx_id}（{boundary_action}），剩余量保留在队列中")
        else:
            order.status = OrderStatus.FILLED
            self._dispose(order, "FILLED", "订单完全成交", ref=tx.tx_id)
        return trades, halt

    def _on_fill(self, tx: _Tx, rule: RuleSet | None, resting: Order,
                 price: float, qty: int, popped: bool) -> None:
        tx.fills.append(_Fill(resting=resting, price=price, qty=qty, popped=popped))
        if rule is not None:
            breach = self.detector.observe_fill(rule, tx.incoming.symbol, price, qty)
            if breach is not None:
                raise _AnomalyAbort(breach)

    def _commit_tx(self, tx: _Tx) -> list[Trade]:
        """提交事务：成交进入待清算台账，检测器统计生效。"""
        self.detector.commit()
        incoming = tx.incoming
        trades: list[Trade] = []
        for fill in tx.fills:
            self._trade_seq += 1
            buy_id, sell_id = (
                (incoming.order_id, fill.resting.order_id)
                if incoming.side is Side.BUY
                else (fill.resting.order_id, incoming.order_id)
            )
            trade = Trade(
                trade_id=f"TRD-{self._trade_seq:06d}",
                tx_id=tx.tx_id,
                seq=self._next_seq(),
                symbol=incoming.symbol,
                price=fill.price,
                quantity=fill.qty,
                buy_order_id=buy_id,
                sell_order_id=sell_id,
                status=TradeStatus.PENDING_CLEARING,
                created_at=self._now(),
            )
            self._trades[trade.trade_id] = trade
            trades.append(trade)
            detail = f"成交 {trade.trade_id}：{fill.qty}@{fill.price}"
            self._dispose(fill.resting, "MATCHED", detail, ref=trade.trade_id)
            self._dispose(incoming, "MATCHED", detail, ref=trade.trade_id)
            if fill.resting.remaining == 0:
                fill.resting.status = OrderStatus.FILLED
                self._dispose(fill.resting, "FILLED", "订单完全成交", ref=trade.trade_id)
            else:
                fill.resting.status = OrderStatus.PARTIALLY_FILLED
            self.audit.record("system", "系统", "TRADE_PENDING_CLEARING", trade.trade_id,
                              tx_id=tx.tx_id, symbol=trade.symbol, price=trade.price,
                              quantity=trade.quantity, buy_order_id=buy_id, sell_order_id=sell_id)
        self.audit.record("system", "系统", "TX_COMMIT", tx.tx_id,
                          trades=[t.trade_id for t in trades])
        return trades

    def _rollback_tx(self, tx: _Tx, breach: Breach) -> None:
        """回滚事务：簿内变更与检测器统计全部撤销，不产生任何成交。"""
        book = self._books[tx.incoming.symbol]
        for fill in reversed(tx.fills):
            fill.resting.remaining += fill.qty
            tx.incoming.remaining += fill.qty
            if fill.popped:
                book.reinsert_front(fill.resting)
            self._dispose(fill.resting, "ROLLED_BACK",
                          f"事务 {tx.tx_id} 回滚：{breach.detail}", ref=tx.tx_id)
        if tx.fills:
            self._dispose(tx.incoming, "ROLLED_BACK",
                          f"事务 {tx.tx_id} 回滚：{breach.detail}", ref=tx.tx_id)
        self.detector.rollback()
        self.audit.record("system", "系统", "TX_ROLLBACK", tx.tx_id,
                          fills=len(tx.fills), reason=breach.detail)

    # ------------------------------------------------------------------
    # 中止触发（自动 / 人工 / 重复触发）
    # ------------------------------------------------------------------

    def _trigger_halt_locked(self, *, actor, role, source, reason, breaches,
                             rule_version, boundary_action, tx_id, idempotency_key) -> HaltIncident:
        self._halt_seq += 1
        halt = HaltIncident(
            halt_id=f"HALT-{self._halt_seq:06d}",
            seq=self._next_seq(),
            trigger_source=source,
            state=IncidentState.DRAFT,
            reason=reason,
            actor=actor,
            role=role,
            created_at=self._now(),
            rule_version=rule_version,
            breaches=list(breaches),
            boundary_action=boundary_action,
            boundary_tx_id=tx_id,
            idempotency_key=idempotency_key,
        )
        self._incidents[halt.halt_id] = halt
        if idempotency_key:
            self._halt_idem[idempotency_key] = halt.halt_id
        self.active_halt_id = halt.halt_id
        self.market_state = MarketState.HALTED
        self.recovery_phase = 0
        halt.history.append({
            "ts": self._now(), "actor": actor, "role": role, "action": "HALT_TRIGGERED",
            "from_state": None, "to_state": IncidentState.DRAFT.value, "detail": reason,
        })
        self.audit.record(actor, role, "HALT_TRIGGERED", halt.halt_id,
                          source=source, reason=reason, rule_version=rule_version,
                          breaches=[b.to_dict() for b in breaches],
                          boundary_action=boundary_action, tx_id=tx_id)
        return halt

    def trigger_halt(self, *, actor, role, reason, idempotency_key=None) -> dict:
        """人工触发中止；重复触发（含幂等重放）只审计不新建事件。"""
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_HALT_MANUAL, "人工中止")
            if not reason:
                raise DomainError("INVALID_HALT", "人工中止必须说明原因")
            if idempotency_key and idempotency_key in self._halt_idem:
                halt = self._incidents[self._halt_idem[idempotency_key]]
                self.audit.record(actor, role, "HALT_DUPLICATE", halt.halt_id,
                                  kind="idempotent_replay", reason=reason)
                return {"halt": halt, "duplicate": True}
            if self.active_halt_id is not None:
                halt = self._incidents[self.active_halt_id]
                self.audit.record(actor, role, "HALT_DUPLICATE", halt.halt_id,
                                  kind="concurrent_trigger", reason=reason)
                return {"halt": halt, "duplicate": True}
            halt = self._trigger_halt_locked(
                actor=actor, role=role, source="MANUAL", reason=reason, breaches=[],
                rule_version=self.rules.active.version if self.rules.active else None,
                boundary_action="NO_OPEN_TX", tx_id=None, idempotency_key=idempotency_key,
            )
            return {"halt": halt, "duplicate": False}

    # ------------------------------------------------------------------
    # 人工复核 / 误报撤销 / 核算 / 分阶段恢复
    # ------------------------------------------------------------------

    def review_halt(self, *, actor, role, halt_id, decision, reason="") -> HaltIncident:
        """人工复核：confirm 进入核算；revoke 按误报撤销并立即恢复市场。"""
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_REVIEW, "人工复核")
            halt = self._get_halt(halt_id)
            if decision == "confirm":
                if halt.state is not IncidentState.DRAFT:
                    raise DomainError("INVALID_STATE", f"当前状态 {halt.state.value} 不能复核确认")
                halt.review_decision = "confirm"
                self._transition_incident(halt, actor, role, IncidentState.RECONCILING,
                                          "REVIEW_CONFIRMED", reason or "复核确认，进入核算")
            elif decision == "revoke":
                if halt.state not in (IncidentState.DRAFT, IncidentState.RECONCILING, IncidentState.CONFIRMED):
                    raise DomainError("INVALID_STATE", f"当前状态 {halt.state.value} 不能按误报撤销")
                halt.review_decision = "revoke"
                halt.false_positive = True
                self._transition_incident(halt, actor, role, IncidentState.ARCHIVED,
                                          "HALT_REVOKED", reason or "误报撤销")
                self._reopen_market(actor, role, note=f"误报撤销：{reason}" if reason else "误报撤销")
            else:
                raise DomainError("INVALID_REVIEW", "decision 必须是 confirm 或 revoke")
            halt.review_actor = actor
            halt.review_reason = reason
            halt.reviewed_at = self._now()
            return halt

    def clear_trade(self, *, actor, role, trade_id) -> Trade:
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_CLEARING, "成交清算")
            trade = self._trades.get(trade_id)
            if trade is None:
                raise DomainError("TRADE_NOT_FOUND", f"成交不存在：{trade_id}")
            if trade.status is not TradeStatus.PENDING_CLEARING:
                raise DomainError("INVALID_STATE", "该成交已清算")
            trade.status = TradeStatus.CLEARED
            trade.cleared_at = self._now()
            trade.cleared_by = actor
            self.audit.record(actor, role, "TRADE_CLEARED", trade.trade_id, tx_id=trade.tx_id)
            return trade

    def confirm_reconciliation(self, *, actor, role, halt_id) -> HaltIncident:
        """核算确认：全部待清算成交结清后，事件从待核算进入已确认。"""
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_CLEARING, "核算确认")
            halt = self._get_halt(halt_id)
            if halt.state is not IncidentState.RECONCILING:
                raise DomainError("INVALID_STATE", f"当前状态 {halt.state.value} 不能核算确认")
            pending = [t for t in self._trades.values() if t.status is TradeStatus.PENDING_CLEARING]
            if pending:
                raise DomainError("TRADES_UNCLEARED",
                                  f"仍有 {len(pending)} 笔成交待清算："
                                  + "、".join(t.trade_id for t in pending[:5]))
            self._transition_incident(halt, actor, role, IncidentState.CONFIRMED,
                                      "RECONCILIATION_CONFIRMED", "待清算成交全部结清")
            return halt

    def begin_recovery(self, *, actor, role, halt_id) -> HaltIncident:
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_OPERATE, "开始恢复")
            halt = self._get_halt(halt_id)
            if halt.state is not IncidentState.CONFIRMED:
                raise DomainError("INVALID_STATE", f"当前状态 {halt.state.value} 不能开始恢复")
            self.market_state = MarketState.RECOVERING
            self.recovery_phase = 1
            halt.recovery_phase = 1
            self._transition_incident(halt, actor, role, IncidentState.RECOVERING,
                                      "RECOVERY_PHASE_ADVANCED",
                                      f"进入恢复阶段 1「{RECOVERY_PHASES[1]}」")
            return halt

    def advance_recovery(self, *, actor, role, halt_id) -> HaltIncident:
        with self._lock:
            self._require_actor(actor, role)
            self._require_role(role, ROLE_OPERATE, "推进恢复阶段")
            halt = self._get_halt(halt_id)
            if halt.state is not IncidentState.RECOVERING:
                raise DomainError("INVALID_STATE", f"当前状态 {halt.state.value} 不能推进恢复")
            if self.recovery_phase >= max(RECOVERY_PHASES):
                raise DomainError("INVALID_STATE", "已是最后恢复阶段")
            self.recovery_phase += 1
            halt.recovery_phase = self.recovery_phase
            self._transition_incident(halt, actor, role, IncidentState.RECOVERING,
                                      "RECOVERY_PHASE_ADVANCED",
                                      f"进入恢复阶段 {self.recovery_phase}"
                                      f"「{RECOVERY_PHASES[self.recovery_phase]}」")
            if self.recovery_phase == max(RECOVERY_PHASES):
                self._transition_incident(halt, actor, role, IncidentState.ARCHIVED,
                                          "INCIDENT_ARCHIVED", "恢复完成，事件封存")
                self._reopen_market(actor, role, note="分阶段恢复完成")
            return halt

    def _reopen_market(self, actor: str, role: str, note: str) -> None:
        self.market_state = MarketState.OPEN
        self.recovery_phase = 0
        self.active_halt_id = None
        self.audit.record(actor, role, "MARKET_REOPENED", "market", note=note)
        self._readmit_parked()

    def _readmit_parked(self) -> None:
        """市场重开后，滞留订单按原订单序列重新进场撮合。"""
        parked = [self._orders[oid] for oid in self._parked]
        self._parked.clear()
        for order in sorted(parked, key=lambda o: o.seq):
            if order.status is not OrderStatus.PARKED:
                continue  # 恢复阶段中已被撤单
            if self.market_state is not MarketState.OPEN:
                # 重新进场过程中再次触发中止：继续滞留
                self._park_order(order, self.active_halt_id, "再次中止，订单继续保留在队列中")
                continue
            order.status = OrderStatus.QUEUED
            self._dispose(order, "READMITTED", "市场恢复，订单按原序列重新进场")
            self.audit.record("system", "系统", "ORDER_READMITTED", order.order_id, seq=order.seq)
            self._execute_matching(order, self.rules.active)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def _get_halt(self, halt_id) -> HaltIncident:
        halt = self._incidents.get(halt_id)
        if halt is None:
            raise DomainError("HALT_NOT_FOUND", f"中止事件不存在：{halt_id}")
        return halt

    def get_halt(self, halt_id) -> HaltIncident:
        with self._lock:
            return self._get_halt(halt_id)

    def list_halts(self) -> list[HaltIncident]:
        with self._lock:
            return sorted(self._incidents.values(), key=lambda h: h.seq)

    def get_order(self, order_id) -> Order:
        with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                raise DomainError("ORDER_NOT_FOUND", f"订单不存在：{order_id}")
            return order

    def list_orders(self, status: str | None = None, symbol: str | None = None) -> list[Order]:
        with self._lock:
            orders = sorted(self._orders.values(), key=lambda o: o.seq)
            if status is not None:
                orders = [o for o in orders if o.status.value == status]
            if symbol is not None:
                orders = [o for o in orders if o.symbol == symbol]
            return orders

    def list_trades(self, status: str | None = None) -> list[Trade]:
        with self._lock:
            trades = sorted(self._trades.values(), key=lambda t: t.seq)
            if status is not None:
                trades = [t for t in trades if t.status.value == status]
            return trades

    def list_audit(self, action: str | None = None, subject: str | None = None) -> list:
        with self._lock:
            return self.audit.list(action=action, subject=subject)

    def market_state_view(self) -> dict:
        with self._lock:
            return {
                "state": self.market_state.value,
                "recovery_phase": self.recovery_phase,
                "recovery_phase_name": RECOVERY_PHASES.get(self.recovery_phase),
                "active_halt_id": self.active_halt_id,
                "active_rule_version": self.rules.active.version if self.rules.active else None,
                "queued_orders": sum(1 for o in self._orders.values()
                                     if o.status in (OrderStatus.QUEUED, OrderStatus.PARTIALLY_FILLED)),
                "parked_orders": len(self._parked),
                "pending_clearing_trades": sum(1 for t in self._trades.values()
                                               if t.status is TradeStatus.PENDING_CLEARING),
                "reference_prices": self.detector.reference_prices,
            }
