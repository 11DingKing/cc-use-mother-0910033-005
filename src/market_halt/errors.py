"""领域错误：携带稳定错误码，供 HTTP 层映射状态码。"""
from __future__ import annotations


class DomainError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
