"""市场价格异常中止服务端。"""

from .engine import MarketHaltEngine

__all__ = ["MarketHaltEngine", "create_server"]


def create_server(*args, **kwargs):
    """延迟导入，避免以 `python -m market_halt.api` 启动时重复加载子模块。"""
    from .api import create_server as _create_server

    return _create_server(*args, **kwargs)
