from collections.abc import Callable
from typing import Any
import coil_core as cc, atexit, abc


def _new_thread_wrapper(info: dict[str, Any]) -> None:
    info["func"](*info["args"], **info["kwargs"])


def new_thread(
    function: Callable[..., None], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> None:
    cc.new_thread(
        _new_thread_wrapper,
        {
            "func": function,
            "args": args,
            "kwargs": kwargs,
        },
    )


class Metrics:
    def __init__(self) -> None:
        metrics: dict[str, int] = cc.fetch_metrics()

        self.queue_global_depth: int = metrics["queue_global_depth"]
        self.num_alive_tasks: int = metrics["num_alive_tasks"]
        self.num_workers: int = metrics["num_workers"]


class Trigger(abc.ABC):
    @abc.abstractmethod
    def __init__(self) -> None:
        pass

    @property
    @abc.abstractmethod
    def int_repr(self) -> list[int | str]:
        raise NotImplementedError("Cannot use the default _Trigger as a trigger.")


def wait_until_trigger(trigger: list[int | str]) -> None:
    cc.wait_for_event(trigger)


class Lock:
    def __init__(self) -> None:
        self._inner = cc.MutexLock()

    def acquire(self) -> None:
        self._inner.acquire()

    def release(self) -> None:
        if not self.get_locked:
            return

        self._inner.release()

    def get_locked(self) -> bool:
        return self._inner.get_locked()
