"""短时间窗口异常价格 / 成交量检测。

判定在撮合事务*提交前*执行：候选成交作为窗口内最新一笔行情参与计算，
命中则事务回滚，该笔行情不会进入成交序列。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .models import RuleVersion, now_ms


@dataclass
class Tick:
    at: int
    price: float
    quantity: int


class AnomalyDetector:
    def __init__(self, clock: Callable[[], int] = now_ms) -> None:
        self._clock = clock
        self._ticks: dict[str, list[Tick]] = {}

    def record(self, symbol: str, price: float, quantity: int, at: int | None = None) -> None:
        """成交提交成功后记录行情。"""
        self._ticks.setdefault(symbol, []).append(
            Tick(at if at is not None else self._clock(), price, quantity)
        )

    def last_price(self, symbol: str) -> float | None:
        ticks = self._ticks.get(symbol)
        return ticks[-1].price if ticks else None

    def evaluate(
        self, symbol: str, price: float, quantity: int, rule: RuleVersion, at: int | None = None
    ) -> dict[str, Any]:
        """评估候选成交是否越界（不写入行情）。"""
        ts = at if at is not None else self._clock()
        history = self._ticks.setdefault(symbol, [])
        window_start = ts - rule.window_ms + 1
        prev_start = ts - 2 * rule.window_ms + 1

        current = [t for t in history if t.at >= window_start] + [
            Tick(ts, price, quantity)
        ]
        previous = [t for t in history if prev_start <= t.at < window_start]

        breaches: list[dict[str, Any]] = []

        # 价格：窗口内任意价格相对窗口首笔的最大偏移
        base = current[0].price
        max_dev = max(abs(t.price - base) / base for t in current)
        if max_dev >= rule.max_price_move:
            breaches.append(
                {
                    "kind": "PRICE_MOVE",
                    "observed": round(max_dev, 6),
                    "threshold": rule.max_price_move,
                    "base_price": base,
                    "extreme_price": max(current, key=lambda t: abs(t.price - base)).price,
                }
            )

        # 成交量：当前窗口总量相对前一等长窗口总量的倍数
        if rule.max_volume_multiple > 0 and previous:
            prev_total = sum(t.quantity for t in previous)
            cur_total = sum(t.quantity for t in current)
            if prev_total > 0:
                multiple = cur_total / prev_total
                if multiple > rule.max_volume_multiple:
                    breaches.append(
                        {
                            "kind": "VOLUME_SPIKE",
                            "observed": round(multiple, 6),
                            "threshold": rule.max_volume_multiple,
                            "window_volume": cur_total,
                            "baseline_volume": prev_total,
                        }
                    )

        return {
            "breached": bool(breaches),
            "breaches": breaches,
            "evidence": {
                "symbol": symbol,
                "candidate_price": price,
                "candidate_quantity": quantity,
                "window_ms": rule.window_ms,
                "window_tick_count": len(current),
                "candidate_at": ts,
            },
        }
