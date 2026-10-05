"""市场价格异常中止引擎的行为测试。"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from market_halt import DomainError, MarketEngine, MarketState, OrderStatus


class Clock:
    def __init__(self) -> None:
        self.t = 1_700_000_000.0

    def __call__(self) -> float:
        return self.t

    def tick(self, seconds: float = 1.0) -> None:
        self.t += seconds


def make_engine(clock: Clock, **rule_kw) -> MarketEngine:
    engine = MarketEngine(now_fn=clock)
    rule = engine.register_rule(actor="op", role="交易运营员", **rule_kw)
    engine.activate_rule(actor="aud", role="监管审计员", version=rule.version)
    return engine


def submit(engine, cid, side, price, qty, symbol="XAU"):
    return engine.submit_order(actor="rep", role="企业申报员", client_order_id=cid,
                               symbol=symbol, side=side, price=price, quantity=qty)


def actions(order):
    return [d.action for d in order.dispositions]


class MatchingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()

    def test_matching_commits_to_pending_clearing(self) -> None:
        engine = make_engine(self.clock, price_deviation_pct=5.0)
        engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=100.0)
        submit(engine, "s1", "SELL", 100, 10)
        result = submit(engine, "b1", "BUY", 100, 4)
        self.assertIsNone(result["halt"])
        self.assertEqual(len(result["trades"]), 1)
        trade = result["trades"][0]
        self.assertEqual(trade.status.value, "PENDING_CLEARING")
        self.assertEqual(trade.quantity, 4)
        orders = {o.order_id: o for o in engine.list_orders()}
        self.assertEqual(orders[trade.sell_order_id].remaining, 6)
        self.assertEqual(orders[trade.buy_order_id].status, OrderStatus.FILLED)
        view = engine.market_state_view()
        self.assertEqual(view["pending_clearing_trades"], 1)
        self.assertEqual(view["reference_prices"]["XAU"], 100.0)

    def test_idempotent_order_submission(self) -> None:
        engine = make_engine(self.clock, price_deviation_pct=5.0)
        first = submit(engine, "k1", "SELL", 100, 5)
        second = submit(engine, "k1", "SELL", 100, 5)
        self.assertFalse(first["deduplicated"])
        self.assertTrue(second["deduplicated"])
        self.assertEqual(first["order"].order_id, second["order"].order_id)
        self.assertEqual(len(engine.list_orders()), 1)

    def test_role_forbidden(self) -> None:
        engine = make_engine(self.clock, price_deviation_pct=5.0)
        with self.assertRaises(DomainError) as ctx:
            engine.submit_order(actor="x", role="核算专员", client_order_id="c1",
                                symbol="XAU", side="BUY", price=1, quantity=1)
        self.assertEqual(ctx.exception.code, "ROLE_FORBIDDEN")


class PriceAnomalyRollbackTest(unittest.TestCase):
    """价格异常：陈旧挂单撞上校正后的参考价，当前事务整体回滚。"""

    def setUp(self) -> None:
        self.clock = Clock()
        self.engine = make_engine(self.clock, price_deviation_pct=5.0,
                                  boundary_policy="ROLLBACK_CURRENT")
        self.engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=90.0)
        submit(self.engine, "s1", "SELL", 90, 5)  # 参考价 90 时正常入队
        # 运营员校正参考价后，90 的卖单成为偏离 10% 的陈旧挂单
        self.engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=100.0)
        submit(self.engine, "s2", "SELL", 99, 5)

    def test_rollback_boundary(self) -> None:
        # 买单 104 单前偏离 4% 放行；撮合先成交 90（偏离 10%）触发中止
        result = submit(self.engine, "b1", "BUY", 104, 12)
        halt = result["halt"]
        self.assertIsNotNone(halt)
        self.assertEqual(result["trades"], [])
        self.assertEqual(halt.boundary_action, "ROLLED_BACK_TX")
        self.assertEqual(halt.trigger_source, "AUTO_RULE")
        self.assertEqual(halt.rule_version, 1)
        self.assertEqual(halt.breaches[0].metric, "price_deviation")
        self.assertEqual(halt.breaches[0].observed, 10.0)
        self.assertEqual(halt.state.value, "草稿")
        # 市场进入中止，无任何状态不明的成交
        self.assertEqual(self.engine.market_state, MarketState.HALTED)
        self.assertEqual(self.engine.list_trades(), [])
        # 订单簿恢复原状，队列被保留
        orders = {o.client_order_id: o for o in self.engine.list_orders()}
        self.assertEqual(orders["s1"].remaining, 5)
        self.assertEqual(orders["s1"].status, OrderStatus.QUEUED)
        self.assertEqual(orders["s2"].remaining, 5)
        self.assertEqual(orders["b1"].remaining, 12)
        self.assertEqual(orders["b1"].status, OrderStatus.PARKED)
        self.assertIn("ROLLED_BACK", actions(orders["s1"]))
        self.assertIn("ROLLED_BACK", actions(orders["b1"]))
        # 审计包含回滚与触发记录
        audit = {e.action for e in self.engine.list_audit()}
        self.assertIn("TX_ROLLBACK", audit)
        self.assertIn("HALT_TRIGGERED", audit)

    def test_halt_barrier_parks_orders_and_freezes_cancel(self) -> None:
        submit(self.engine, "b1", "BUY", 120, 8)  # 触发中止
        parked = submit(self.engine, "b2", "BUY", 99, 3)
        self.assertEqual(parked["order"].status, OrderStatus.PARKED)
        self.assertEqual(parked["halt"].halt_id, self.engine.active_halt_id)
        with self.assertRaises(DomainError) as ctx:
            self.engine.cancel_order(actor="rep", role="企业申报员",
                                     order_id=parked["order"].order_id)
        self.assertEqual(ctx.exception.code, "MARKET_FROZEN")
        view = self.engine.market_state_view()
        self.assertEqual(view["state"], "HALTED")
        self.assertEqual(view["parked_orders"], 2)


class CommitPolicyTest(unittest.TestCase):
    """成交量异常：当前事务已成交部分提交，剩余量滞留。"""

    def test_commit_boundary(self) -> None:
        clock = Clock()
        engine = make_engine(clock, max_window_volume=10, window_seconds=10,
                             boundary_policy="COMMIT_CURRENT")
        engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=100.0)
        submit(engine, "s1", "SELL", 100, 8)
        submit(engine, "b1", "BUY", 100, 8)  # 窗口量 8，未超限
        submit(engine, "s2", "SELL", 100, 8)
        result = submit(engine, "b2", "BUY", 100, 20)
        halt = result["halt"]
        self.assertIsNotNone(halt)
        self.assertEqual(halt.boundary_action, "COMMITTED_TX")
        self.assertEqual(halt.breaches[0].metric, "window_volume")
        # 事务内已成交的 8 手提交待清算，剩余 12 手滞留
        self.assertEqual(sum(t.quantity for t in result["trades"]), 8)
        self.assertEqual(len(engine.list_trades(status="PENDING_CLEARING")), 2)
        b2 = engine.get_order(result["order"].order_id)
        self.assertEqual(b2.remaining, 12)
        self.assertEqual(b2.status, OrderStatus.PARKED)

    def test_rollback_policy_undoes_volume_breach(self) -> None:
        clock = Clock()
        engine = make_engine(clock, max_window_volume=10, window_seconds=10,
                             boundary_policy="ROLLBACK_CURRENT")
        engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=100.0)
        submit(engine, "s1", "SELL", 100, 8)
        submit(engine, "b1", "BUY", 100, 8)
        submit(engine, "s2", "SELL", 100, 8)
        result = submit(engine, "b2", "BUY", 100, 20)
        self.assertEqual(result["halt"].boundary_action, "ROLLED_BACK_TX")
        self.assertEqual(len(engine.list_trades()), 1)  # 只有中止前已提交的 8 手
        orders = {o.client_order_id: o for o in engine.list_orders()}
        self.assertEqual(orders["s2"].remaining, 8)
        self.assertEqual(orders["s2"].status, OrderStatus.QUEUED)
        self.assertEqual(orders["b2"].remaining, 20)
        self.assertEqual(orders["b2"].status, OrderStatus.PARKED)


class VolumeBaselineTest(unittest.TestCase):
    def test_volume_multiple_against_baseline(self) -> None:
        clock = Clock()
        engine = make_engine(clock, volume_multiple=3.0, window_seconds=10,
                             baseline_windows=2, min_baseline_volume=10)
        engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=100.0)
        for i in range(2):  # 两个基线窗口，每个成交 10 手
            submit(engine, f"s{i}", "SELL", 100, 10)
            submit(engine, f"b{i}", "BUY", 100, 10)
            clock.tick(10)
        submit(engine, "s9", "SELL", 100, 40)
        result = submit(engine, "b9", "BUY", 100, 40)
        halt = result["halt"]
        self.assertIsNotNone(halt)
        breach = halt.breaches[0]
        self.assertEqual(breach.metric, "window_volume")
        self.assertEqual(breach.observed, 40)
        self.assertEqual(breach.threshold, 30.0)  # 3.0 × max(基线10, 下限10)


class DuplicateTriggerTest(unittest.TestCase):
    def test_duplicate_triggers_are_audited(self) -> None:
        clock = Clock()
        engine = make_engine(clock, price_deviation_pct=5.0)
        first = engine.trigger_halt(actor="op", role="交易运营员",
                                    reason="行情抖动，人工中止", idempotency_key="req-1")
        self.assertFalse(first["duplicate"])
        # 并发重复触发：不同幂等键，指向同一事件
        second = engine.trigger_halt(actor="op2", role="监管审计员",
                                     reason="重复触发", idempotency_key="req-2")
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["halt"].halt_id, first["halt"].halt_id)
        # 幂等重放：同一幂等键返回原事件
        replay = engine.trigger_halt(actor="op", role="交易运营员",
                                     reason="行情抖动，人工中止", idempotency_key="req-1")
        self.assertTrue(replay["duplicate"])
        self.assertEqual(len(engine.list_halts()), 1)
        duplicates = engine.list_audit(action="HALT_DUPLICATE")
        self.assertEqual({e.details["kind"] for e in duplicates},
                         {"concurrent_trigger", "idempotent_replay"})


class RecoveryFlowTest(unittest.TestCase):
    """完整流程：触发 → 复核 → 核算 → 分阶段恢复 → 滞留订单重新进场。"""

    def test_full_recovery_flow(self) -> None:
        clock = Clock()
        engine = make_engine(clock, price_deviation_pct=5.0)
        engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=90.0)
        submit(engine, "s1", "SELL", 90, 5)  # 参考价 90 时入队的陈旧卖单
        engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=100.0)
        submit(engine, "s2", "SELL", 104, 5)
        submit(engine, "s3", "SELL", 103, 5)
        # 买单 105 单前偏离 5% 未越线；撮合在 90 处偏离 10% 触发，事务回滚
        result = submit(engine, "b1", "BUY", 105, 12)
        halt = result["halt"]
        self.assertEqual(halt.boundary_action, "ROLLED_BACK_TX")
        b1_id = result["order"].order_id
        submit(engine, "b2", "BUY", 105, 3)  # 中止期间滞留

        # 复核确认 → 待核算；回滚后无待清算成交，可直接核算确认
        engine.review_halt(actor="aud", role="监管审计员",
                           halt_id=halt.halt_id, decision="confirm", reason="价格确实异常")
        self.assertEqual(engine.get_halt(halt.halt_id).state.value, "待核算")
        engine.confirm_reconciliation(actor="acc", role="核算专员", halt_id=halt.halt_id)
        self.assertEqual(engine.get_halt(halt.halt_id).state.value, "已确认")

        # 阶段一「仅撤单」：撤掉异常买单与陈旧卖单，新报单被拒
        engine.begin_recovery(actor="op", role="交易运营员", halt_id=halt.halt_id)
        self.assertEqual(engine.market_state, MarketState.RECOVERING)
        engine.cancel_order(actor="rep", role="企业申报员", order_id=b1_id)
        s1_id = next(o.order_id for o in engine.list_orders() if o.client_order_id == "s1")
        engine.cancel_order(actor="rep", role="企业申报员", order_id=s1_id)
        with self.assertRaises(DomainError) as ctx:
            submit(engine, "b9", "BUY", 100, 1)
        self.assertEqual(ctx.exception.code, "MARKET_FROZEN")

        # 阶段二「可报单不撮合」
        engine.advance_recovery(actor="op", role="交易运营员", halt_id=halt.halt_id)
        parked = submit(engine, "b3", "BUY", 100, 2)
        self.assertEqual(parked["order"].status, OrderStatus.PARKED)

        # 阶段三「恢复撮合」：市场重开，滞留订单按原序列重新进场
        engine.advance_recovery(actor="op", role="交易运营员", halt_id=halt.halt_id)
        self.assertEqual(engine.market_state, MarketState.OPEN)
        self.assertEqual(engine.get_halt(halt.halt_id).state.value, "已封存")
        orders = {o.client_order_id: o for o in engine.list_orders()}
        # b2(105×3) 重新进场后与 s3(103) 成交 3 手
        self.assertEqual(orders["b2"].status, OrderStatus.FILLED)
        self.assertEqual(orders["s3"].remaining, 2)
        self.assertEqual(orders["b3"].status, OrderStatus.QUEUED)  # 100 不交叉，留在簿内
        trades = engine.list_trades(status="PENDING_CLEARING")
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].quantity, 3)
        self.assertEqual(trades[0].price, 103.0)
        self.assertIn("READMITTED", actions(orders["b2"]))
        # 审计覆盖每一次阶段推进与重开
        advances = [e for e in engine.list_audit(action="RECOVERY_PHASE_ADVANCED")]
        self.assertEqual(len(advances), 3)
        self.assertIsNotNone(engine.list_audit(action="MARKET_REOPENED"))
        self.assertIsNotNone(engine.list_audit(action="INCIDENT_ARCHIVED"))

    def test_reconciliation_blocked_by_uncleared_trades(self) -> None:
        clock = Clock()
        engine = make_engine(clock, price_deviation_pct=5.0)
        engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=100.0)
        submit(engine, "s1", "SELL", 100, 5)
        submit(engine, "b1", "BUY", 100, 5)  # 产生一笔待清算成交
        halt = engine.trigger_halt(actor="op", role="交易运营员", reason="人工中止")["halt"]
        engine.review_halt(actor="aud", role="监管审计员", halt_id=halt.halt_id, decision="confirm")
        with self.assertRaises(DomainError) as ctx:
            engine.confirm_reconciliation(actor="acc", role="核算专员", halt_id=halt.halt_id)
        self.assertEqual(ctx.exception.code, "TRADES_UNCLEARED")
        trade = engine.list_trades(status="PENDING_CLEARING")[0]
        engine.clear_trade(actor="acc", role="核算专员", trade_id=trade.trade_id)
        engine.confirm_reconciliation(actor="acc", role="核算专员", halt_id=halt.halt_id)
        self.assertEqual(engine.get_halt(halt.halt_id).state.value, "已确认")


class FalsePositiveTest(unittest.TestCase):
    def test_revoke_reopens_market_and_readmits(self) -> None:
        clock = Clock()
        engine = make_engine(clock, price_deviation_pct=5.0)
        engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=100.0)
        submit(engine, "s1", "SELL", 101, 5)
        halt = engine.trigger_halt(actor="op", role="交易运营员",
                                   reason="疑似异常，先行中止")["halt"]
        parked = submit(engine, "b1", "BUY", 101, 5)
        self.assertEqual(parked["order"].status, OrderStatus.PARKED)
        engine.review_halt(actor="aud", role="监管审计员", halt_id=halt.halt_id,
                           decision="revoke", reason="确认为数据源误报")
        incident = engine.get_halt(halt.halt_id)
        self.assertEqual(incident.state.value, "已封存")
        self.assertTrue(incident.false_positive)
        self.assertEqual(engine.market_state, MarketState.OPEN)
        self.assertIsNone(engine.active_halt_id)
        # 滞留订单重新进场并成交
        order = engine.get_order(parked["order"].order_id)
        self.assertEqual(order.status, OrderStatus.FILLED)
        self.assertEqual(len(engine.list_trades(status="PENDING_CLEARING")), 1)
        self.assertIsNotNone(engine.list_audit(action="HALT_REVOKED"))

    def test_review_requires_auditor_role(self) -> None:
        clock = Clock()
        engine = make_engine(clock, price_deviation_pct=5.0)
        halt = engine.trigger_halt(actor="op", role="交易运营员", reason="中止")["halt"]
        with self.assertRaises(DomainError) as ctx:
            engine.review_halt(actor="op", role="交易运营员",
                               halt_id=halt.halt_id, decision="confirm")
        self.assertEqual(ctx.exception.code, "ROLE_FORBIDDEN")


class RuleVersionTest(unittest.TestCase):
    def test_versions_and_halt_basis(self) -> None:
        clock = Clock()
        engine = make_engine(clock, price_deviation_pct=5.0)
        v2 = engine.register_rule(actor="op", role="交易运营员", price_deviation_pct=8.0)
        engine.activate_rule(actor="aud", role="监管审计员", version=v2.version)
        rules = {r.version: r for r in engine.list_rules()}
        self.assertEqual(rules[1].status, "SUPERSEDED")
        self.assertEqual(rules[2].status, "ACTIVE")
        engine.set_reference_price(actor="op", role="交易运营员", symbol="XAU", price=100.0)
        result = submit(engine, "b1", "BUY", 120, 1)  # 偏离 20% > 8%
        halt = result["halt"]
        self.assertIsNotNone(halt)
        self.assertEqual(halt.rule_version, 2)  # 暂停依据记录触发时的规则版本
        self.assertEqual(halt.boundary_action, "NO_OPEN_TX")  # 单前检查触发，无在途事务


if __name__ == "__main__":
    unittest.main()
