"""启动市场价格异常中止 HTTP 服务。"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from market_halt.engine import MarketEngine
from market_halt.server import create_server


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="市场价格异常中止服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    server = create_server(args.host, args.port, MarketEngine())
    print(f"listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
