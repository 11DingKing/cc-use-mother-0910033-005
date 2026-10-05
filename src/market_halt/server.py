"""基于标准库的 HTTP API：暂停依据与每笔订单处置均可查询。

错误响应统一为 {"error": {"code", "message"}}；领域错误码映射到
HTTP 状态码，未识别异常返回 500。
"""
from __future__ import annotations

import enum
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import MarketEngine
from .errors import DomainError

_ERROR_STATUS = {
    "ORDER_NOT_FOUND": 404,
    "HALT_NOT_FOUND": 404,
    "TRADE_NOT_FOUND": 404,
    "RULE_NOT_FOUND": 404,
    "ROLE_FORBIDDEN": 403,
    "ACTOR_REQUIRED": 400,
    "INVALID_ORDER": 400,
    "INVALID_RULE": 400,
    "INVALID_HALT": 400,
    "INVALID_REVIEW": 400,
    "INVALID_REFERENCE": 400,
    "INVALID_STATE": 409,
    "MARKET_FROZEN": 409,
    "TRADES_UNCLEARED": 409,
}


def _dump(obj):
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, dict):
        return {k: _dump(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_dump(v) for v in obj]
    return obj


# ----------------------------------------------------------------------
# 路由处理：每个函数返回 (status, payload)
# ----------------------------------------------------------------------

def _health(engine, body, query):
    return 200, {"status": "ok"}


def _create_rule(engine, body, query):
    rule = engine.register_rule(
        actor=body.get("actor"), role=body.get("role"),
        price_deviation_pct=body.get("price_deviation_pct"),
        window_seconds=body.get("window_seconds", 60),
        max_window_volume=body.get("max_window_volume"),
        volume_multiple=body.get("volume_multiple"),
        baseline_windows=body.get("baseline_windows", 5),
        min_baseline_volume=body.get("min_baseline_volume", 0),
        boundary_policy=body.get("boundary_policy", "ROLLBACK_CURRENT"),
    )
    return 201, {"rule": rule}


def _list_rules(engine, body, query):
    return 200, {"rules": engine.list_rules()}


def _activate_rule(engine, body, query, version):
    try:
        version = int(version)
    except ValueError:
        raise DomainError("INVALID_RULE", f"规则版本必须是整数：{version}") from None
    rule = engine.activate_rule(actor=body.get("actor"), role=body.get("role"), version=version)
    return 200, {"rule": rule}


def _set_reference(engine, body, query):
    engine.set_reference_price(actor=body.get("actor"), role=body.get("role"),
                               symbol=body.get("symbol"), price=body.get("price"))
    return 200, {"reference_prices": engine.detector.reference_prices}


def _market_state(engine, body, query):
    return 200, engine.market_state_view()


def _submit_order(engine, body, query):
    result = engine.submit_order(
        actor=body.get("actor"), role=body.get("role"),
        client_order_id=body.get("client_order_id"), symbol=body.get("symbol"),
        side=body.get("side"), price=body.get("price"), quantity=body.get("quantity"),
    )
    return 201, result


def _list_orders(engine, body, query):
    status = query.get("status", [None])[0]
    symbol = query.get("symbol", [None])[0]
    return 200, {"orders": engine.list_orders(status=status, symbol=symbol)}


def _get_order(engine, body, query, order_id):
    return 200, {"order": engine.get_order(order_id)}


def _cancel_order(engine, body, query, order_id):
    order = engine.cancel_order(actor=body.get("actor"), role=body.get("role"), order_id=order_id)
    return 200, {"order": order}


def _trigger_halt(engine, body, query):
    result = engine.trigger_halt(actor=body.get("actor"), role=body.get("role"),
                                 reason=body.get("reason"),
                                 idempotency_key=body.get("idempotency_key"))
    return 201, result


def _list_halts(engine, body, query):
    return 200, {"halts": engine.list_halts()}


def _get_halt(engine, body, query, halt_id):
    return 200, {"halt": engine.get_halt(halt_id)}


def _review_halt(engine, body, query, halt_id):
    halt = engine.review_halt(actor=body.get("actor"), role=body.get("role"),
                              halt_id=halt_id, decision=body.get("decision"),
                              reason=body.get("reason", ""))
    return 200, {"halt": halt}


def _confirm_reconciliation(engine, body, query, halt_id):
    halt = engine.confirm_reconciliation(actor=body.get("actor"), role=body.get("role"), halt_id=halt_id)
    return 200, {"halt": halt}


def _begin_recovery(engine, body, query, halt_id):
    halt = engine.begin_recovery(actor=body.get("actor"), role=body.get("role"), halt_id=halt_id)
    return 200, {"halt": halt}


def _advance_recovery(engine, body, query, halt_id):
    halt = engine.advance_recovery(actor=body.get("actor"), role=body.get("role"), halt_id=halt_id)
    return 200, {"halt": halt}


def _list_trades(engine, body, query):
    status = query.get("status", [None])[0]
    return 200, {"trades": engine.list_trades(status=status)}


def _clear_trade(engine, body, query, trade_id):
    trade = engine.clear_trade(actor=body.get("actor"), role=body.get("role"), trade_id=trade_id)
    return 200, {"trade": trade}


def _list_audit(engine, body, query):
    action = query.get("action", [None])[0]
    subject = query.get("subject", [None])[0]
    return 200, {"audit": engine.list_audit(action=action, subject=subject)}


ROUTES = [
    ("GET", re.compile(r"^/health$"), _health),
    ("POST", re.compile(r"^/rules$"), _create_rule),
    ("GET", re.compile(r"^/rules$"), _list_rules),
    ("POST", re.compile(r"^/rules/(?P<version>[^/]+)/activate$"), _activate_rule),
    ("POST", re.compile(r"^/market/reference$"), _set_reference),
    ("GET", re.compile(r"^/market/state$"), _market_state),
    ("POST", re.compile(r"^/orders$"), _submit_order),
    ("GET", re.compile(r"^/orders$"), _list_orders),
    ("GET", re.compile(r"^/orders/(?P<order_id>[^/]+)$"), _get_order),
    ("POST", re.compile(r"^/orders/(?P<order_id>[^/]+)/cancel$"), _cancel_order),
    ("POST", re.compile(r"^/halts$"), _trigger_halt),
    ("GET", re.compile(r"^/halts$"), _list_halts),
    ("GET", re.compile(r"^/halts/(?P<halt_id>[^/]+)$"), _get_halt),
    ("POST", re.compile(r"^/halts/(?P<halt_id>[^/]+)/review$"), _review_halt),
    ("POST", re.compile(r"^/halts/(?P<halt_id>[^/]+)/confirm-reconciliation$"), _confirm_reconciliation),
    ("POST", re.compile(r"^/halts/(?P<halt_id>[^/]+)/begin-recovery$"), _begin_recovery),
    ("POST", re.compile(r"^/halts/(?P<halt_id>[^/]+)/advance$"), _advance_recovery),
    ("GET", re.compile(r"^/trades$"), _list_trades),
    ("POST", re.compile(r"^/trades/(?P<trade_id>[^/]+)/clear$"), _clear_trade),
    ("GET", re.compile(r"^/audit$"), _list_audit),
]


def create_server(host: str, port: int, engine: MarketEngine | None = None) -> ThreadingHTTPServer:
    engine = engine or MarketEngine()

    class Handler(BaseHTTPRequestHandler):
        def _dispatch(self, method: str) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            body: dict = {}
            if method == "POST":
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    try:
                        body = json.loads(self.rfile.read(length))
                    except json.JSONDecodeError:
                        return self._send(400, {"error": {"code": "BAD_JSON",
                                                          "message": "请求体不是合法 JSON"}})
                    if not isinstance(body, dict):
                        return self._send(400, {"error": {"code": "BAD_JSON",
                                                          "message": "请求体必须是 JSON 对象"}})
            for route_method, pattern, handler in ROUTES:
                if route_method != method:
                    continue
                match = pattern.match(parsed.path)
                if match is None:
                    continue
                try:
                    status, payload = handler(engine, body, query, **match.groupdict())
                except DomainError as exc:
                    status = _ERROR_STATUS.get(exc.code, 400)
                    payload = {"error": {"code": exc.code, "message": exc.message}}
                except Exception as exc:  # noqa: BLE001 - 兜底，避免连接悬挂
                    status, payload = 500, {"error": {"code": "INTERNAL", "message": str(exc)}}
                return self._send(status, payload)
            self._send(404, {"error": {"code": "ROUTE_NOT_FOUND",
                                       "message": f"{method} {parsed.path}"}})

        def _send(self, status: int, payload) -> None:
            data = json.dumps(_dump(payload), ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            self._dispatch("GET")

        def do_POST(self) -> None:
            self._dispatch("POST")

        def log_message(self, *args) -> None:  # 静默访问日志
            pass

    return ThreadingHTTPServer((host, port), Handler)
