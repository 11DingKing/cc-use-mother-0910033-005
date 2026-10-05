"""HTTP API（标准库实现，零第三方依赖）。

路由：
  GET  /status                         市场状态与计数
  POST /admin/rules                    发布异常检测规则版本
  GET  /admin/rules                    规则版本历史
  POST /orders                         提交订单（立即尝试撮合）
  POST /orders/{id}/cancel             撤销队列中的订单
  POST /market/signals                 外部行情异常信号（重复触发经此入口审计）
  GET  /orders/{id}                    单笔订单处置说明（含暂停依据）
  GET  /trades?pending=1               待清算成交
  POST /trades/{id}/settle             清算成交
  GET  /incidents                      中止事件列表
  GET  /incidents/{id}                 暂停依据（命中版本、证据、事务）
  GET  /incidents/{id}/orders          事件影响的每笔订单处置
  POST /incidents/{id}/review          人工复核 CONFIRMED / FALSE_POSITIVE
  POST /incidents/{id}/advance         分阶段恢复推进一阶段
  GET  /audit?incident_id=&action=&actor=   追加式审计查询

操作人身份取 JSON 体中的 actor，缺省取 X-Actor 头，再缺省为 anonymous。
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from .engine import EngineError, MarketHaltEngine


def create_server(
    host: str = "127.0.0.1",
    port: int = 8080,
    engine: MarketHaltEngine | None = None,
) -> ThreadingHTTPServer:
    engine = engine or MarketHaltEngine()

    class Handler(BaseHTTPRequestHandler):
        server_version = "MarketHalt/0.1"

        def log_message(self, fmt: str, *args: Any) -> None:  # 静默，测试不刷屏
            return

        # ------------------------------------------------------------ 工具

        def _send(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                value = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise EngineError("BAD_JSON", f"请求体不是合法 JSON：{exc}", 400)
            if not isinstance(value, dict):
                raise EngineError("BAD_JSON", "请求体必须是 JSON 对象", 400)
            return value

        def _actor(self, body: dict[str, Any]) -> str:
            return str(body.get("actor") or self.headers.get("X-Actor") or "anonymous")

        def _handle(self, fn: Callable[[], Any]) -> None:
            try:
                result = fn()
            except EngineError as exc:
                self._send(exc.status, {"error": exc.code, "message": str(exc)})
            except KeyError as exc:
                self._send(400, {"error": "MISSING_FIELD", "message": f"缺少字段 {exc}"})
            except (TypeError, ValueError) as exc:
                self._send(400, {"error": "BAD_REQUEST", "message": str(exc)})
            else:
                self._send(200, result if result is not None else {"ok": True})

        # ------------------------------------------------------------ 动词

        def do_GET(self) -> None:  # noqa: N802
            self._handle(lambda: self._route_get())

        def do_POST(self) -> None:  # noqa: N802
            self._handle(lambda: self._route_post())

        # ------------------------------------------------------------ 路由

        def _route_get(self) -> Any:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}

            if path == "/status":
                return engine.status()
            if path == "/admin/rules":
                return {"rules": engine.list_rules()}
            if path == "/trades":
                return {"trades": engine.list_trades(pending_only=qs.get("pending") == "1")}
            if path == "/incidents":
                return {"incidents": engine.list_incidents()}
            if path == "/audit":
                return {"events": engine.list_audit(**{k: qs[k] for k in qs if k in {"incident_id", "action", "actor"}})}

            m = re.fullmatch(r"/orders/([^/]+)", path)
            if m:
                return engine.order_disposition_report(m.group(1))
            m = re.fullmatch(r"/incidents/([^/]+)/orders", path)
            if m:
                return engine.incident_orders_report(m.group(1))
            m = re.fullmatch(r"/incidents/([^/]+)", path)
            if m:
                incidents = {i["incident_id"]: i for i in engine.list_incidents()}
                inc = incidents.get(m.group(1))
                if inc is None:
                    raise EngineError("INCIDENT_NOT_FOUND", "事件不存在", 404)
                return inc
            raise EngineError("NOT_FOUND", f"无此路由：{path}", 404)

        def _route_post(self) -> Any:
            path = urlparse(self.path).path.rstrip("/") or "/"
            body = self._body()
            actor = self._actor(body)

            if path == "/admin/rules":
                return {
                    "rule": engine.publish_rule(
                        actor=actor,
                        window_ms=int(body["window_ms"]),
                        max_price_move=float(body["max_price_move"]),
                        max_volume_multiple=float(body.get("max_volume_multiple", 0)),
                        note=str(body.get("note", "")),
                    )
                }
            if path == "/orders":
                return engine.submit_order(
                    order_id=str(body["order_id"]),
                    symbol=str(body["symbol"]),
                    side=str(body["side"]),
                    price=float(body["price"]),
                    quantity=int(body["quantity"]),
                    actor=actor,
                )
            m = re.fullmatch(r"/orders/([^/]+)/cancel", path)
            if m:
                return {"order": engine.cancel_order(m.group(1), actor, str(body.get("reason", "")))}
            if path == "/market/signals":
                return engine.ingest_market_signal(
                    symbol=str(body["symbol"]),
                    price=float(body["price"]),
                    quantity=int(body["quantity"]),
                    actor=actor,
                )
            m = re.fullmatch(r"/trades/([^/]+)/settle", path)
            if m:
                return {"trade": engine.settle_trade(m.group(1), actor)}
            m = re.fullmatch(r"/incidents/([^/]+)/review", path)
            if m:
                return engine.review_incident(
                    m.group(1), actor, str(body["verdict"]), str(body.get("comment", ""))
                )
            m = re.fullmatch(r"/incidents/([^/]+)/advance", path)
            if m:
                return engine.advance_stage(m.group(1), actor, str(body.get("comment", "")))
            raise EngineError("NOT_FOUND", f"无此路由：{path}", 404)

    server = ThreadingHTTPServer((host, port), Handler)
    server.engine = engine  # type: ignore[attr-defined]
    return server


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="市场价格异常中止服务端")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    server = create_server(args.host, args.port)
    print(f"市场异常中止服务监听 http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
