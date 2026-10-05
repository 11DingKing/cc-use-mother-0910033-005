"""领域模型与枚举。"""
from __future__ import annotations

import enum
import dataclasses
import time
import uuid
from typing import Any


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class MarketState(str, enum.Enum):
    """市场状态，与领域契约 states 的五状态对齐。

    生命周期（撮合成交屏障由 MarketHaltEngine.phase 细化）：
      草稿      尚未发布异常检测规则，不撮合
      →(发布规则)→ 正常交易（phase=TRADING，market_state 保留 草稿）
      →(检测命中/信号)→ 待核算（phase=CANCEL_ONLY，事务提交前回滚）
      →(人工复核确认)→ 已确认（仍仅撤单）
      →(恢复阶段1 仅撤单 / 阶段2 限价)→ 执行中
      →(阶段3 全量恢复)→ 已封存
      →(复核为误报)→ 已封存（撤销中止，直接恢复 TRADING）
    封存后若再次命中，会开启新的事件并回到 待核算。
    """

    DRAFT = "草稿"
    PENDING = "待核算"
    CONFIRMED = "已确认"
    RESUMING = "执行中"
    SEALED = "已封存"


class OrderSide(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, enum.Enum):
    QUEUED = "QUEUED"                 # 在队列中等待撮合
    MATCHED = "MATCHED"               # 已撮合成交（待清算）
    SETTLED = "SETTLED"               # 已清算
    CANCELLED = "CANCELLED"           # 已撤销（仅撤单阶段 / 操作员撤销）
    ROLLED_BACK = "ROLLED_BACK"       # 撮合事务回滚，退回队列前的中间态记录
    REJECTED = "REJECTED"             # 被屏障拒绝（限价阶段价格越界）


@dataclasses.dataclass
class RuleVersion:
    """异常检测规则版本（不可变）。"""

    version: int
    published_at: int
    published_by: str
    # 短时间窗口（毫秒）
    window_ms: int
    # 窗口内价格最大允许偏移比例（相对窗口首笔，0.05 = 5%）
    max_price_move: float
    # 窗口内成交量最大允许倍数（相对窗口之前同长度窗口的均量，0 表示不校验）
    max_volume_multiple: float
    # 与上一版兼容（False 表示发布即强制全市场重新核算）
    note: str = ""

    @property
    def fingerprint(self) -> str:
        import hashlib

        payload = f"{self.window_ms}|{self.max_price_move}|{self.max_volume_multiple}"
        return hashlib.sha256(payload.encode()).hexdigest()[:12]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "published_at": self.published_at,
            "published_by": self.published_by,
            "window_ms": self.window_ms,
            "max_price_move": self.max_price_move,
            "max_volume_multiple": self.max_volume_multiple,
            "fingerprint": self.fingerprint,
            "note": self.note,
        }


@dataclasses.dataclass
class Order:
    order_id: str
    symbol: str
    side: OrderSide
    price: float
    quantity: int
    received_at: int
    client_seq: int
    status: OrderStatus = OrderStatus.QUEUED
    filled_quantity: int = 0
    trade_ids: list[str] = dataclasses.field(default_factory=list)
    # 处置轨迹：[{"at", "action", "detail", "ref"}]
    dispositions: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    matched_price: float | None = None
    rule_version_at_match: int | None = None

    def dispose(self, action: str, detail: str, ref: str | None = None) -> None:
        self.dispositions.append(
            {"at": now_ms(), "action": action, "detail": detail, "ref": ref}
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "price": self.price,
            "quantity": self.quantity,
            "client_seq": self.client_seq,
            "received_at": self.received_at,
            "status": self.status.value,
            "filled_quantity": self.filled_quantity,
            "remaining_quantity": self.quantity - self.filled_quantity,
            "trade_ids": self.trade_ids,
            "matched_price": self.matched_price,
            "rule_version_at_match": self.rule_version_at_match,
            "dispositions": self.dispositions,
        }


@dataclasses.dataclass
class Trade:
    """一笔成交（撮合事务提交后产生，先待清算）。"""

    trade_id: str
    symbol: str
    price: float
    quantity: int
    buy_order_id: str
    sell_order_id: str
    matched_at: int
    rule_version: int
    rule_fingerprint: str
    tx_id: str
    settled: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "buy_order_id": self.buy_order_id,
            "sell_order_id": self.sell_order_id,
            "matched_at": self.matched_at,
            "rule_version": self.rule_version,
            "rule_fingerprint": self.rule_fingerprint,
            "tx_id": self.tx_id,
            "status": "SETTLED" if self.settled else "PENDING_SETTLEMENT",
        }
