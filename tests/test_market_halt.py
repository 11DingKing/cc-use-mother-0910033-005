"""异常中止服务端端到端测试：触发、回滚、重复触发、复核、分阶段恢复、误报、清算。"""
from __future__ import annotations

import json
import sys
import threading
import unittest
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from market_halt.engine import (
    CANCEL_ONLY,
    LIMIT_ONLY,
    TRADING,
    MarketHaltEngine,
)
from market_halt.models import MarketState
from market_halt.api import create_server


class FakeClock:
    def __init__(self) -> None:
        self.t = 0

    def __call__(self) -> int:
        return self.t

    def advance(self, ms: int) -> None:
        self.t += ms


def pair(engine: MarketHaltEngine, n: int, price: float, qty: int = 10, symbol: str = "600000"):
    """提交一对交叉订单：卖单先挂、买单吃单，成交价 = 挂单价。"""
    engine.submit_order(f"S{n}", symbol, "SELL", price, qty, "tester")
    return engine.submit_order(f"B{n}", symbol, "BUY", price, qty, "tester")


def rule(engine: MarketHaltEngine, **kw):
    opts = dict(window_ms=1000, max_price_move=0.05, max_volume_multiple=0.0, actor="风控管理员")
    opts.update(kw)
    return engine.publish_rule(**opts)


class HaltLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.engine = MarketHaltEngine(clock=self.clock)
        rule(self.engine)

    def test_price_spike_rolls_back_tx_and_halts_at_consistent_boundary(self) -> None:
        # 两笔正常成交形成 100 元基准
        r1 = pair(self.engine, 1, 100.0)
        r2 = pair(self.engine, 2, 100.0)
        self.assertEqual([m["outcome"] for m in r1["matches"]], ["COMMITTED"])
        trades = self.engine.list_trades()
        self.assertEqual(len(trades), 2)
        self.assertTrue(all(t["status"] == "PENDING_SETTLEMENT" for t in trades))
        # 成交永久记录命中规则版本与指纹
        self.assertEqual(trades[0]["rule_version"], 1)
        self.assertTrue(trades[0]["rule_fingerprint"])

        # 第三笔报价 106（+6%）→ 命中，事务提交前回滚并中止
        self.clock.advance(200)
        r3 = pair(self.engine, 3, 106.0)
        self.assertEqual(r3["matches"][0]["outcome"], "ROLLED_BACK_HALT")
        inc_id = r3["matches"][0]["incident_id"]

        # 没有产生状态不明的成交：成交仍是前两笔
        self.assertEqual(len(self.engine.list_trades()), 2)

        status = self.engine.status()
        self.assertEqual(status["matching_phase"], CANCEL_ONLY)
        self.assertEqual(status["market_state"], MarketState.PENDING.value)
        self.assertEqual(status["active_incident_id"], inc_id)
        # 队列保留：候选买卖单仍在队列且数量未被吞掉
        self.assertEqual(status["queued"]["600000"], {"buy": 10, "sell": 10})

        # 候选单仍是 QUEUED，带提交前回滚轨迹；它们从未进入成交
        report = self.engine.order_disposition_report("S3")
        self.assertEqual(report["order"]["status"], "QUEUED")
        actions = [d["action"] for d in report["order"]["dispositions"]]
        self.assertIn("TX_ROLLED_BACK", actions)
        self.assertEqual(report["halt_basis"][0]["incident_id"], inc_id)
        self.assertIn("PRICE_MOVE", report["halt_basis"][0]["reason"])

        # 暂停依据：命中版本、指纹、观测值、阈值、回滚事务
        inc = next(i for i in self.engine.list_incidents() if i["incident_id"] == inc_id)
        self.assertEqual(inc["rule_version"], 1)
        breach = inc["trigger"]["breaches"][0]
        self.assertEqual(breach["kind"], "PRICE_MOVE")
        self.assertGreaterEqual(breach["observed"], 0.06)
        self.assertTrue(inc["trigger_tx_id"].startswith("tx_"))
        # 命中规则的不可变快照随事件留存
        self.assertEqual(inc["rule_snapshot"]["window_ms"], 1000)

        # 中止期间新撮合在事务边界被一致拦截，订单保留队列
        self.clock.advance(10)
        r4 = pair(self.engine, 4, 100.0)
        self.assertEqual(r4["matches"][0]["outcome"], "GATED")
        # 队首候选单（S3/B3）记录了屏障拦截，后续单仍在队列等待
        self.assertTrue(
            any(d["action"] == "TX_GATED" for d in self.engine.orders["B3"].dispositions)
        )

    def test_repeated_trigger_is_idempotent_and_audited(self) -> None:
        pair(self.engine, 1, 100.0)
        pair(self.engine, 2, 100.0)
        self.clock.advance(200)
        r3 = pair(self.engine, 3, 106.0)
        inc_id = r3["matches"][0]["incident_id"]

        # 行情馈送再次上报异常价格：重复触发，屏障不变
        sig = self.engine.ingest_market_signal("600000", 107.0, 5)
        self.assertTrue(sig["breached"])
        self.assertTrue(sig["repeated"])
        self.assertEqual(sig["incident_id"], inc_id)
        self.assertEqual(self.engine.status()["matching_phase"], CANCEL_ONLY)

        # 再来一次：仍然只审计
        self.engine.ingest_market_signal("600000", 108.0, 5)

        events = self.engine.list_audit(incident_id=inc_id, action="REPEATED_TRIGGER_IGNORED")
        self.assertEqual(len(events), 2)
        self.assertTrue(all(e["actor"] == "SYSTEM" for e in events))
        # 没有新建第二个事件
        self.assertEqual(self.engine.status()["counts"]["incidents"], 1)

        # 非异常信号不误伤
        clean = self.engine.ingest_market_signal("600000", 100.0, 1)
        self.assertFalse(clean["breached"])

    def test_manual_review_confirm_and_phased_recovery(self) -> None:
        pair(self.engine, 1, 100.0)
        pair(self.engine, 2, 100.0)
        self.clock.advance(200)
        inc_id = pair(self.engine, 3, 106.0)["matches"][0]["incident_id"]

        # 仅撤单阶段允许撤单
        self.engine.cancel_order("S3", "交易运营员", "中止后撤单")
        self.assertEqual(self.engine.orders["S3"].status.value, "CANCELLED")

        # 复核确认异常属实
        inc = self.engine.review_incident(inc_id, "交易运营员", "CONFIRMED", "确认价格异动")
        self.assertEqual(inc["state"], "CONFIRMED")
        self.assertEqual(self.engine.status()["market_state"], MarketState.CONFIRMED.value)

        # 阶段 1：仅撤单（执行中），撮合仍被屏障拦截
        a1 = self.engine.advance_stage(inc_id, "交易运营员")
        self.assertEqual(a1["incident"]["recovery_stage"], 1)
        self.assertEqual(self.engine.status()["matching_phase"], CANCEL_ONLY)
        self.assertEqual(self.engine.status()["market_state"], MarketState.RESUMING.value)

        # 阶段 2：限价恢复。106 超出 ±2% 带宽被拦，100 可成交
        a2 = self.engine.advance_stage(inc_id, "交易运营员")
        self.assertEqual(self.engine.status()["matching_phase"], LIMIT_ONLY)
        # 队首 B3@106 与新卖单 S4@106 候选价超带宽
        self.engine.submit_order("S4", "600000", "SELL", 106.0, 10, "tester")
        gated = self.engine.submit_order("B4", "600000", "BUY", 106.0, 10, "tester")
        self.assertEqual(gated["matches"][0]["outcome"], "GATED_LIMIT_BAND")
        # 清掉队首后，100 元的单可以在带宽内成交
        self.engine.cancel_order("B3", "交易运营员")
        self.engine.cancel_order("S4", "交易运营员")
        self.engine.cancel_order("B4", "交易运营员")
        self.clock.advance(10)
        ok = pair(self.engine, 5, 100.0)
        self.assertEqual(ok["matches"][0]["outcome"], "COMMITTED")

        # 阶段 3：全量恢复并封存
        a3 = self.engine.advance_stage(inc_id, "交易运营员", "市场恢复正常")
        self.assertEqual(a3["incident"]["state"], "RECOVERED")
        self.assertEqual(self.engine.status()["matching_phase"], TRADING)
        self.assertEqual(self.engine.status()["market_state"], MarketState.SEALED.value)
        self.assertIsNone(self.engine.status()["active_incident_id"])

        # 恢复后不能再推进阶段
        with self.assertRaises(Exception):
            self.engine.advance_stage(inc_id, "交易运营员")

        # 复核与每次阶段推进都有审计
        actions = [e["action"] for e in self.engine.list_audit(incident_id=inc_id)]
        self.assertEqual(actions.count("INCIDENT_REVIEW"), 1)
        self.assertEqual(actions.count("STAGE_ADVANCE"), 3)
        self.assertEqual(actions.count("HALT_TRIGGERED"), 1)
        self.assertEqual(actions.count("TX_ROLLED_BACK"), 1)

        # 事件订单报告覆盖候选单与被屏障拦截的每笔单
        report = self.engine.incident_orders_report(inc_id)
        order_ids = {o["order_id"] for o in report["orders"]}
        self.assertIn("B3", order_ids)
        self.assertIn("S3", order_ids)
        for o in report["orders"]:
            self.assertTrue(o["dispositions"])

    def test_false_positive_revokes_halt_and_revives_orders(self) -> None:
        pair(self.engine, 1, 100.0)
        pair(self.engine, 2, 100.0)
        self.clock.advance(200)
        inc_id = pair(self.engine, 3, 106.0)["matches"][0]["incident_id"]

        # 行情冷却后复核为误报：撤销中止、事件封存、屏障恢复 TRADING
        self.clock.advance(5000)
        inc = self.engine.review_incident(inc_id, "监管审计员", "FALSE_POSITIVE", "馈送重影")
        self.assertEqual(inc["state"], "REVOKED_FALSE_POSITIVE")
        self.assertEqual(self.engine.status()["matching_phase"], TRADING)
        self.assertEqual(self.engine.status()["market_state"], MarketState.SEALED.value)

        # 候选单获得恢复标记，并能在 100 元价位重新撮合（改价需先撤再挂，
        # 这里直接验证队列订单恢复资格：撤旧单、按基准价重新挂）
        self.assertIn(
            "HALT_REVIVED",
            [d["action"] for d in self.engine.orders["S3"].dispositions],
        )
        self.engine.cancel_order("S3", "tester")
        self.engine.cancel_order("B3", "tester")
        r = pair(self.engine, 6, 100.0)
        self.assertEqual(r["matches"][0]["outcome"], "COMMITTED")

        events = self.engine.list_audit(incident_id=inc_id, action="FALSE_POSITIVE_REVOKED")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["actor"], "监管审计员")

    def test_settlement_completes_trades_and_orders(self) -> None:
        pair(self.engine, 1, 100.0)
        trade_id = self.engine.list_trades()[0]["trade_id"]
        settled = self.engine.settle_trade(trade_id, "核算专员")
        self.assertEqual(settled["status"], "SETTLED")
        self.assertEqual(self.engine.orders["B1"].status.value, "SETTLED")
        self.assertEqual(self.engine.orders["S1"].status.value, "SETTLED")

        # 完成或回滚语义明确：重复清算被拒
        with self.assertRaises(Exception):
            self.engine.settle_trade(trade_id, "核算专员")

        events = self.engine.list_audit(action="TRADE_SETTLED")
        self.assertEqual(len(events), 1)

    def test_rule_versions_are_immutable_history(self) -> None:
        rule(self.engine, window_ms=2000, max_price_move=0.03, actor="风控管理员", note="收紧")
        versions = self.engine.list_rules()
        self.assertEqual([v["version"] for v in versions], [1, 2])
        self.assertNotEqual(versions[0]["fingerprint"], versions[1]["fingerprint"])
        # 旧版本对象不可变（快照被事件永久持有）
        first = versions[0]
        self.assertEqual(first["window_ms"], 1000)
        self.assertEqual(self.engine.status()["current_rule_version"], 2)
        with self.assertRaises(Exception):
            rule(self.engine, window_ms=0, max_price_move=0.05)

    def test_orders_queued_before_rule_published_do_not_match(self) -> None:
        engine = MarketHaltEngine(clock=self.clock)
        res = engine.submit_order("X1", "600000", "BUY", 10.0, 1, "tester")
        self.assertEqual(res["matches"], [])
        rule(engine)
        # 发布规则后新单进入才撮合；无对手方时保持排队
        res2 = engine.submit_order("X2", "600000", "SELL", 10.0, 1, "tester")
        self.assertEqual(res2["matches"][0]["outcome"], "COMMITTED")


class VolumeSpikeTest(unittest.TestCase):
    def test_volume_multiple_breach(self) -> None:
        clock = FakeClock()
        engine = MarketHaltEngine(clock=clock)
        rule(engine, max_volume_multiple=2.0)
        # 布局：t=0 成交 10（落入评估时的前一窗口）；t=950 成交 10（当前窗口）
        pair(engine, 1, 100.0, qty=10)
        clock.advance(950)
        pair(engine, 2, 100.0, qty=10)
        # t=1050 候选 25 股 → 当前窗口 35 / 前窗 10 = 3.5x > 2x
        clock.advance(100)
        r = pair(engine, 3, 100.0, qty=25)
        self.assertEqual(r["matches"][0]["outcome"], "ROLLED_BACK_HALT")
        inc = engine.list_incidents()[0]
        self.assertEqual(inc["trigger"]["breaches"][0]["kind"], "VOLUME_SPIKE")


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.server = create_server("127.0.0.1", 0, MarketHaltEngine(clock=self.clock))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()

    def _call(self, method: str, path: str, payload: dict | None = None, actor: str | None = None):
        import urllib.error

        url = f"http://127.0.0.1:{self.port}{path}"
        if actor and payload is not None:
            payload = {**payload, "actor": actor}
        data = json.dumps(payload).encode() if payload is not None else None
        headers = {"Content-Type": "application/json"}
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def test_full_flow_over_http(self) -> None:
        status, body = self._call("GET", "/status")
        self.assertEqual(status, 200)
        self.assertIsNone(body["current_rule_version"])

        status, body = self._call(
            "POST",
            "/admin/rules",
            {"window_ms": 1000, "max_price_move": 0.05},
            actor="风控管理员",
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["rule"]["version"], 1)

        for n, price in ((1, 100.0), (2, 100.0)):
            self._call("POST", "/orders", {
                "order_id": f"S{n}", "symbol": "600000", "side": "SELL",
                "price": price, "quantity": 10,
            })
            _, body = self._call("POST", "/orders", {
                "order_id": f"B{n}", "symbol": "600000", "side": "BUY",
                "price": price, "quantity": 10,
            })
            self.assertEqual(body["matches"][0]["outcome"], "COMMITTED")

        self.clock.advance(200)
        self._call("POST", "/orders", {
            "order_id": "S3", "symbol": "600000", "side": "SELL", "price": 106.0, "quantity": 10,
        })
        _, body = self._call("POST", "/orders", {
            "order_id": "B3", "symbol": "600000", "side": "BUY", "price": 106.0, "quantity": 10,
        })
        inc_id = body["matches"][0]["incident_id"]

        # API 能说明暂停依据
        status, inc = self._call("GET", f"/incidents/{inc_id}")
        self.assertEqual(status, 200)
        self.assertIn("halt_reason", inc)

        # API 能说明每笔订单处置
        status, report = self._call("GET", f"/incidents/{inc_id}/orders")
        self.assertEqual(status, 200)
        self.assertEqual({o["order_id"] for o in report["orders"]}, {"S3", "B3"})

        # 审计可查
        status, audit = self._call("GET", f"/audit?incident_id={inc_id}")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(audit["events"]), 2)

        # 错误映射
        status, body = self._call("GET", "/orders/NOPE")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"], "ORDER_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
