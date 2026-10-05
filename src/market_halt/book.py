"""价格-时间优先的订单簿。

簿内变更全部发生在撮合事务内：撤单式的反向操作（reinsert_front）
保证事务回滚后订单队列与事务前完全一致。
"""
from __future__ import annotations

import bisect
from collections import deque

from .models import Order, Side


class OrderBook:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._levels: dict[Side, dict[float, deque[Order]]] = {Side.BUY: {}, Side.SELL: {}}
        self._prices: dict[Side, list[float]] = {Side.BUY: [], Side.SELL: []}  # 升序

    def _best(self, side: Side) -> Order | None:
        prices = self._prices[side]
        if not prices:
            return None
        # 买方最优价为最高买价（升序末尾），卖方为最低卖价（升序开头）
        price = prices[-1] if side is Side.BUY else prices[0]
        queue = self._levels[side][price]
        return queue[0] if queue else None

    def best_bid(self) -> Order | None:
        return self._best(Side.BUY)

    def best_ask(self) -> Order | None:
        return self._best(Side.SELL)

    def rest(self, order: Order) -> None:
        """剩余量按价格-时间优先进入队列尾部。"""
        levels = self._levels[order.side]
        if order.price not in levels:
            levels[order.price] = deque()
            bisect.insort(self._prices[order.side], order.price)
        levels[order.price].append(order)

    def reinsert_front(self, order: Order) -> None:
        """事务回滚时把被消耗的挂单恢复到原价位队列头部。"""
        levels = self._levels[order.side]
        if order.price not in levels:
            levels[order.price] = deque()
            bisect.insort(self._prices[order.side], order.price)
        levels[order.price].appendleft(order)

    def remove(self, order: Order) -> None:
        levels = self._levels[order.side]
        queue = levels.get(order.price)
        if queue is None or order not in queue:
            return
        queue.remove(order)
        if not queue:
            del levels[order.price]
            self._prices[order.side].remove(order.price)

    def _pop(self, order: Order) -> None:
        levels = self._levels[order.side]
        queue = levels[order.price]
        queue.popleft()
        if not queue:
            del levels[order.price]
            self._prices[order.side].remove(order.price)

    def match(self, incoming: Order, on_fill) -> None:
        """与簿内对手方撮合，每笔成交回调 on_fill(resting, price, qty, popped)。

        回调可抛出异常中断撮合（如检测到异常），已发生的簿内变更
        由调用方在事务回滚时按相反顺序恢复。
        """
        while incoming.remaining > 0:
            resting = self.best_ask() if incoming.side is Side.BUY else self.best_bid()
            if resting is None:
                break
            if incoming.side is Side.BUY and incoming.price < resting.price:
                break
            if incoming.side is Side.SELL and incoming.price > resting.price:
                break
            qty = min(incoming.remaining, resting.remaining)
            price = resting.price  # 成交价取先进入队列一方的价格
            incoming.remaining -= qty
            resting.remaining -= qty
            popped = resting.remaining == 0
            if popped:
                self._pop(resting)
            on_fill(resting, price, qty, popped)
