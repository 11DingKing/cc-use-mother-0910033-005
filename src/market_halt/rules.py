"""异常检测规则：版本化注册、激活切换与实时检测。

检测器随撮合事务工作：事务内的成交先记入暂存区参与检测，
事务提交时并入正式统计，回滚时整体丢弃，保证检测口径与成交口径一致。
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass

from .errors import DomainError
from .models import BoundaryPolicy, Breach


@dataclass
class RuleSet:
    """异常检测规则的一个版本，激活后不可变。"""

    version: int
    status: str  # DRAFT / ACTIVE / SUPERSEDED
    price_deviation_pct: float | None  # 相对参考价的偏离阈值（百分比）
    window_seconds: int  # 成交量统计窗口（秒）
    max_window_volume: int | None  # 窗口成交量绝对上限
    volume_multiple: float | None  # 相对基线均值的倍数上限
    baseline_windows: int  # 基线取前 N 个窗口
    min_baseline_volume: int  # 基线下限，避免小基数误报
    boundary_policy: BoundaryPolicy  # 触发时对当前撮合事务的处置
    created_by: str
    created_at: float
    activated_by: str | None = None
    activated_at: float | None = None

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "status": self.status,
            "price_deviation_pct": self.price_deviation_pct,
            "window_seconds": self.window_seconds,
            "max_window_volume": self.max_window_volume,
            "volume_multiple": self.volume_multiple,
            "baseline_windows": self.baseline_windows,
            "min_baseline_volume": self.min_baseline_volume,
            "boundary_policy": self.boundary_policy.value,
            "created_by": self.created_by,
            "created_at": self.created_at,
            "activated_by": self.activated_by,
            "activated_at": self.activated_at,
        }


class RuleRegistry:
    """规则版本注册表：草稿登记、激活切换、历史版本留痕。"""

    def __init__(self) -> None:
        self._rules: dict[int, RuleSet] = {}
        self._active: int | None = None
        self._seq = 0

    def register(
        self,
        *,
        price_deviation_pct: float | None,
        window_seconds: int,
        max_window_volume: int | None,
        volume_multiple: float | None,
        baseline_windows: int,
        min_baseline_volume: int,
        boundary_policy: BoundaryPolicy,
        created_by: str,
        now: float,
    ) -> RuleSet:
        if price_deviation_pct is None and max_window_volume is None and volume_multiple is None:
            raise DomainError("INVALID_RULE", "至少配置一项阈值：价格偏离、窗口成交量上限或基线倍数")
        if window_seconds <= 0:
            raise DomainError("INVALID_RULE", "统计窗口必须为正数秒")
        if price_deviation_pct is not None and price_deviation_pct <= 0:
            raise DomainError("INVALID_RULE", "价格偏离阈值必须为正")
        if max_window_volume is not None and max_window_volume <= 0:
            raise DomainError("INVALID_RULE", "窗口成交量上限必须为正")
        if volume_multiple is not None and volume_multiple <= 0:
            raise DomainError("INVALID_RULE", "基线倍数必须为正")
        if baseline_windows <= 0:
            raise DomainError("INVALID_RULE", "基线窗口数必须为正")
        self._seq += 1
        rule = RuleSet(
            version=self._seq,
            status="DRAFT",
            price_deviation_pct=price_deviation_pct,
            window_seconds=window_seconds,
            max_window_volume=max_window_volume,
            volume_multiple=volume_multiple,
            baseline_windows=baseline_windows,
            min_baseline_volume=min_baseline_volume,
            boundary_policy=boundary_policy,
            created_by=created_by,
            created_at=now,
        )
        self._rules[rule.version] = rule
        return rule

    def activate(self, version: int, actor: str, now: float) -> RuleSet:
        rule = self._rules.get(version)
        if rule is None:
            raise DomainError("RULE_NOT_FOUND", f"规则版本不存在：{version}")
        if rule.status == "ACTIVE":
            return rule
        if self._active is not None:
            self._rules[self._active].status = "SUPERSEDED"
        rule.status = "ACTIVE"
        rule.activated_by = actor
        rule.activated_at = now
        self._active = version
        return rule

    @property
    def active(self) -> RuleSet | None:
        return self._rules.get(self._active) if self._active is not None else None

    def get(self, version: int) -> RuleSet:
        rule = self._rules.get(version)
        if rule is None:
            raise DomainError("RULE_NOT_FOUND", f"规则版本不存在：{version}")
        return rule

    def list(self) -> list[RuleSet]:
        return [self._rules[v] for v in sorted(self._rules)]


class Detector:
    """实时异常检测：价格偏离参考价、窗口成交量超限。

    参考价为该标的最近一笔已提交事务的成交价（或人工设定的初始参考价）。
    """

    def __init__(self, now_fn) -> None:
        self._now = now_fn
        self._reference: dict[str, float] = {}
        self._fills: dict[str, deque[tuple[float, float, int]]] = defaultdict(deque)
        self._tentative: list[tuple[str, float, float, int]] | None = None

    @property
    def reference_prices(self) -> dict[str, float]:
        return dict(self._reference)

    def set_reference(self, symbol: str, price: float) -> None:
        self._reference[symbol] = price

    # ---- 事务边界 ----

    def begin(self) -> None:
        if self._tentative is not None:
            raise DomainError("TX_NESTED", "检测器不支持嵌套事务")
        self._tentative = []

    def commit(self) -> None:
        assert self._tentative is not None, "commit 前必须先 begin"
        for symbol, ts, price, qty in self._tentative:
            self._fills[symbol].append((ts, price, qty))
            self._reference[symbol] = price
            self._prune(symbol, ts)
        self._tentative = None

    def rollback(self) -> None:
        assert self._tentative is not None, "rollback 前必须先 begin"
        self._tentative = None

    def _prune(self, symbol: str, now: float) -> None:
        # 只保留基线+当前窗口所需的历史，避免无限增长
        fills = self._fills[symbol]
        # 窗口长度取所有规则中的最大窗口过于耦合，这里保守保留 1 天
        horizon = now - 86400
        while fills and fills[0][0] <= horizon:
            fills.popleft()

    # ---- 检测 ----

    def check_order_price(self, rule: RuleSet, symbol: str, price: float) -> Breach | None:
        """报单进入撮合前的价格偏离检查（不产生任何统计）。"""
        return self._price_breach(rule, symbol, price)

    def observe_fill(self, rule: RuleSet, symbol: str, price: float, qty: int) -> Breach | None:
        """事务内每笔成交的检测：先暂存再评估，违例时由引擎决定提交或回滚。"""
        assert self._tentative is not None, "observe_fill 必须在事务内调用"
        ts = self._now()
        self._tentative.append((symbol, ts, price, qty))
        breach = self._price_breach(rule, symbol, price)
        if breach is not None:
            return breach
        return self._volume_breach(rule, symbol, ts)

    def _price_breach(self, rule: RuleSet, symbol: str, price: float) -> Breach | None:
        if rule.price_deviation_pct is None:
            return None
        reference = self._reference.get(symbol)
        if not reference:
            return None
        deviation = abs(price - reference) / reference * 100
        if deviation > rule.price_deviation_pct:
            return Breach(
                metric="price_deviation",
                symbol=symbol,
                observed=round(deviation, 4),
                threshold=rule.price_deviation_pct,
                detail=(
                    f"{symbol} 价格 {price} 偏离参考价 {reference} "
                    f"{deviation:.2f}%，阈值 {rule.price_deviation_pct}%"
                ),
            )
        return None

    def _volume_breach(self, rule: RuleSet, symbol: str, ts: float) -> Breach | None:
        if rule.max_window_volume is None and rule.volume_multiple is None:
            return None
        window = rule.window_seconds
        events = [(t, q) for t, _, q in self._fills.get(symbol, ())]
        events += [(t, q) for s, t, _, q in (self._tentative or []) if s == symbol]
        current = sum(q for t, q in events if ts - window < t <= ts)
        if rule.max_window_volume is not None and current > rule.max_window_volume:
            return Breach(
                metric="window_volume",
                symbol=symbol,
                observed=current,
                threshold=rule.max_window_volume,
                detail=f"{symbol} {window}秒窗口成交量 {current} 超过上限 {rule.max_window_volume}",
            )
        if rule.volume_multiple is not None:
            span = window * rule.baseline_windows
            base = sum(q for t, q in events if ts - window - span < t <= ts - window)
            baseline = max(base / rule.baseline_windows, rule.min_baseline_volume)
            if baseline > 0 and current > rule.volume_multiple * baseline:
                threshold = rule.volume_multiple * baseline
                return Breach(
                    metric="window_volume",
                    symbol=symbol,
                    observed=current,
                    threshold=threshold,
                    detail=(
                        f"{symbol} {window}秒窗口成交量 {current} 超过基线 "
                        f"{baseline:.0f} 的 {rule.volume_multiple} 倍"
                    ),
                )
        return None
