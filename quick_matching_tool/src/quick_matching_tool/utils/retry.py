"""Retry helpers."""

from __future__ import annotations

import time
from typing import Callable, TypeVar

F = TypeVar("F", bound=Callable[..., object])


def retry(**kw):
    def warpp(func: F) -> F:
        def inner(*args, **kwargs):
            sleep_time = kw.get("sleep_time") or kwargs.get("sleep_time", 3)
            max_retry = kw.get("max_retry") or kwargs.get("max_retry", 5)
            for attempt in range(1, max_retry + 1):
                try:
                    result = func(*args, **kwargs)
                    break
                except Exception as exc:
                    print(f"{func.__name__} 尝试第 {attempt}次, {exc}, 等待{sleep_time}秒后重试")
                    if kw.get("is_need_remind_error") and attempt == max_retry:
                        raise Exception(f"{func.__name__} 尝试{max_retry}次后失败") from exc
                    time.sleep(sleep_time)
            return result

        return inner

    return warpp
