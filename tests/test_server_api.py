"""HTTP API 冒烟测试：真实起服务，走一遍触发-复核-撤销流程。"""
from __future__ import annotations

import http.client
import json
import sys
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from market_halt.engine import MarketEngine
from market_halt.server import create_server


class ServerApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.engine = MarketEngine()
        cls.server = create_server("127.0.0.1", 0, cls.engine)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def call(self, method: str, path: str, body: dict | None = None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Content-Type": "application/json"} if body is not None else {}
        conn.request(method, path, body=payload, headers=headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        conn.close()
        return response.status, data

    def test_full_api_flow(self) -> None:
        status, _ = self.call("GET", "/health")
        self.assertEqual(status, 200)

        # 规则版本：登记并激活
        status, data = self.call("POST", "/rules", {
            "actor": "op", "role": "交易运营员", "price_deviation_pct": 5.0,
        })
        self.assertEqual(status, 201)
        version = data["rule"]["version"]
        status, data = self.call("POST", f"/rules/{version}/activate",
                                 {"actor": "aud", "role": "监管审计员"})
        self.assertEqual(data["rule"]["status"], "ACTIVE")

        # 参考价与正常成交
        self.call("POST", "/market/reference",
                  {"actor": "op", "role": "交易运营员", "symbol": "XAU", "price": 100.0})
        status, _ = self.call("POST", "/orders", {
            "actor": "rep", "role": "企业申报员", "client_order_id": "s1",
            "symbol": "XAU", "side": "SELL", "price": 100, "quantity": 5,
        })
        self.assertEqual(status, 201)
        status, data = self.call("POST", "/orders", {
            "actor": "rep", "role": "企业申报员", "client_order_id": "b1",
            "symbol": "XAU", "side": "BUY", "price": 100, "quantity": 5,
        })
        self.assertEqual(len(data["trades"]), 1)
        self.assertEqual(data["trades"][0]["status"], "PENDING_CLEARING")

        # 异常价格触发中止：偏离 20% > 5%，单前触发，无在途事务
        status, data = self.call("POST", "/orders", {
            "actor": "rep", "role": "企业申报员", "client_order_id": "b2",
            "symbol": "XAU", "side": "BUY", "price": 120, "quantity": 1,
        })
        halt = data["halt"]
        self.assertEqual(halt["rule_version"], version)
        self.assertEqual(halt["boundary_action"], "NO_OPEN_TX")
        self.assertEqual(halt["breaches"][0]["metric"], "price_deviation")
        self.assertEqual(data["order"]["status"], "PARKED")

        # 市场状态与暂停依据可查
        status, data = self.call("GET", "/market/state")
        self.assertEqual(data["state"], "HALTED")
        self.assertEqual(data["active_halt_id"], halt["halt_id"])
        status, data = self.call("GET", f"/halts/{halt['halt_id']}")
        self.assertEqual(data["halt"]["state"], "草稿")
        self.assertIn("偏离", data["halt"]["reason"])

        # 每笔订单处置可说明
        status, _ = self.call("GET", "/orders/nonexistent")
        self.assertEqual(status, 404)
        status, data = self.call("GET", "/orders?status=PARKED")
        self.assertEqual(len(data["orders"]), 1)
        actions = [d["action"] for d in data["orders"][0]["dispositions"]]
        self.assertEqual(actions, ["ACCEPTED", "PARKED"])

        # 误报撤销：市场恢复，滞留订单重新进场
        status, data = self.call("POST", f"/halts/{halt['halt_id']}/review", {
            "actor": "aud", "role": "监管审计员", "decision": "revoke",
            "reason": "确认为误报",
        })
        self.assertEqual(data["halt"]["state"], "已封存")
        self.assertTrue(data["halt"]["false_positive"])
        status, data = self.call("GET", "/market/state")
        self.assertEqual(data["state"], "OPEN")

        # 审计轨迹完整
        status, data = self.call("GET", "/audit")
        recorded = {e["action"] for e in data["audit"]}
        self.assertTrue({"RULE_REGISTERED", "RULE_ACTIVATED", "ORDER_ACCEPTED",
                         "TX_COMMIT", "TRADE_PENDING_CLEARING", "HALT_TRIGGERED",
                         "HALT_REVOKED", "MARKET_REOPENED"} <= recorded)

        # 角色越权被拒绝
        status, data = self.call("POST", "/halts", {
            "actor": "rep", "role": "企业申报员", "reason": "越权尝试",
        })
        self.assertEqual(status, 403)
        self.assertEqual(data["error"]["code"], "ROLE_FORBIDDEN")


if __name__ == "__main__":
    unittest.main()
