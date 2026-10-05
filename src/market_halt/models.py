"""市场价格异常中止的核心数据模型。

模型对应领域契约（domain/contract.json）的角色、状态与不变量：

- 异常规则版本：RuleSet 带版本号，激活后不可变，触发中止时记录版本；
- 市场状态屏障：MarketState 决定报单、撤单、撮合是否放行；
- 撮合事务边界：BoundaryPolicy 决定触发中止时当前事务提交或回滚；
- 分阶段恢复审计：HaltIncident 复用契约状态机并记录完整历史。
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class MarketState(str, enum.Enum):
    """市场状态屏障。"""

    OPEN = "OPEN"  # 正常撮合
    HALTED = "HALTED"  # 异常中止：停止新撮合，保留订单队列
    RECOVERING = "RECOVERING"  # 分阶段恢复中


class IncidentState(str, enum.Enum):
    """中止事件生命周期，复用领域契约状态机。"""

    DRAFT = "草稿"  # 已触发，待人工复核
    RECONCILING = "待核算"  # 复核确认，待清算成交核算中
    CONFIRMED = "已确认"  # 核算完成，允许进入恢复
    RECOVERING = "执行中"  # 分阶段恢复执行中
    ARCHIVED = "已封存"  # 恢复完成或误报撤销，终态


#: 分阶段恢复的阶段名称（阶段号从 1 开始）
RECOVERY_PHASES = {1: "仅撤单", 2: "可报单不撮合", 3: "恢复撮合"}

#: 领域契约角色
ROLES = ("企业申报员", "核算专员", "交易运营员", "监管审计员")


class OrderStatus(str, enum.Enum):
    QUEUED = "QUEUED"  # 在订单簿队列中
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    PARKED = "PARKED"  # 中止期间滞留，保留但未进入订单簿
    CANCELLED = "CANCELLED"


class TradeStatus(str, enum.Enum):
    PENDING_CLEARING = "PENDING_CLEARING"  # 待清算
    CLEARED = "CLEARED"


class BoundaryPolicy(str, enum.Enum):
    """触发中止时对当前撮合事务的处置策略。"""

    ROLLBACK_CURRENT = "ROLLBACK_CURRENT"  # 回滚当前事务（默认，价格异常等可疑成交）
    COMMIT_CURRENT = "COMMIT_CURRENT"  # 完成（提交）当前事务（成交量异常等已成交部分有效场景）


class Side(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class Disposition:
    """订单处置记录：每次状态变化追加一条，保证每笔订单处置可说明。"""

    ts: float
    action: str
    detail: str
    ref: str | None = None  # 关联的事务 / 中止事件 / 成交编号

    def to_dict(self) -> dict:
        return {"ts": self.ts, "action": self.action, "detail": self.detail, "ref": self.ref}


@dataclass
class Order:
    order_id: str
    client_order_id: str  # 客户端幂等键
    seq: int  # 订单序列号，全市场单调递增
    symbol: str
    side: Side
    price: float
    quantity: int
    remaining: int
    status: OrderStatus
    actor: str
    created_at: float
    dispositions: list[Disposition] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "seq": self.seq,
            "symbol": self.symbol,
            "side": self.side.value,
            "price": self.price,
            "quantity": self.quantity,
            "remaining": self.remaining,
            "status": self.status.value,
            "actor": self.actor,
            "created_at": self.created_at,
            "dispositions": [d.to_dict() for d in self.dispositions],
        }


@dataclass
class Trade:
    """待清算成交：撮合事务提交后进入清算台账。"""

    trade_id: str
    tx_id: str
    seq: int
    symbol: str
    price: float
    quantity: int
    buy_order_id: str
    sell_order_id: str
    status: TradeStatus
    created_at: float
    cleared_at: float | None = None
    cleared_by: str | None = None

    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "tx_id": self.tx_id,
            "seq": self.seq,
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "buy_order_id": self.buy_order_id,
            "sell_order_id": self.sell_order_id,
            "status": self.status.value,
            "created_at": self.created_at,
            "cleared_at": self.cleared_at,
            "cleared_by": self.cleared_by,
        }


@dataclass
class Breach:
    """一次规则违例的度量证据，构成暂停依据。"""

    metric: str  # price_deviation / window_volume
    symbol: str
    observed: float
    threshold: float
    detail: str

    def to_dict(self) -> dict:
        return {
            "metric": self.metric,
            "symbol": self.symbol,
            "observed": self.observed,
            "threshold": self.threshold,
            "detail": self.detail,
        }


@dataclass
class HaltIncident:
    """一次异常中止事件：暂停依据、人工复核、分阶段恢复与误报撤销历史。"""

    halt_id: str
    seq: int
    trigger_source: str  # AUTO_RULE / MANUAL
    state: IncidentState
    reason: str
    actor: str
    role: str
    created_at: float
    rule_version: int | None = None
    breaches: list[Breach] = field(default_factory=list)
    boundary_action: str = "NO_OPEN_TX"  # ROLLED_BACK_TX / COMMITTED_TX / NO_OPEN_TX
    boundary_tx_id: str | None = None
    idempotency_key: str | None = None
    recovery_phase: int = 0
    false_positive: bool = False
    review_decision: str | None = None
    review_actor: str | None = None
    review_reason: str | None = None
    reviewed_at: float | None = None
    history: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        review = None
        if self.review_decision is not None:
            review = {
                "decision": self.review_decision,
                "actor": self.review_actor,
                "reason": self.review_reason,
                "reviewed_at": self.reviewed_at,
            }
        return {
            "halt_id": self.halt_id,
            "seq": self.seq,
            "state": self.state.value,
            "trigger_source": self.trigger_source,
            "reason": self.reason,
            "rule_version": self.rule_version,
            "breaches": [b.to_dict() for b in self.breaches],
            "boundary_action": self.boundary_action,
            "boundary_tx_id": self.boundary_tx_id,
            "actor": self.actor,
            "role": self.role,
            "created_at": self.created_at,
            "recovery_phase": self.recovery_phase,
            "recovery_phase_name": RECOVERY_PHASES.get(self.recovery_phase),
            "false_positive": self.false_positive,
            "review": review,
            "history": list(self.history),
        }
