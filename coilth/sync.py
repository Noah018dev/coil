from collections.abc import Callable
from typing import Any, Literal
from .cogs import Lock as _Lock
from collections import deque


class Lock(_Lock):
    def __init__(self) -> None:
        super().__init__()

    @property
    def locked(self) -> bool:
        return self.get_locked()

    def __enter__(self) -> "Lock":
        self.acquire()

        return self

    def __exit__(self, *_) -> bool:
        self.release()

        return False


class Notification:
    def __init__(self) -> None:
        self._waiters: deque[Lock] = deque([])

    def wait(self) -> None:
        lock = Lock()
        lock.acquire()

        self._waiters.append(lock)

        lock.acquire()

    def notify(self, count: int) -> None:
        if len(self._waiters) == 0:
            return None

        for _ in range(count):
            self._waiters.popleft().release()

    def notify_one(self) -> None:
        self.notify(1)

    def notify_all(self) -> None:
        self.notify(len(self._waiters))


class Event:
    def __init__(self) -> None:
        self._lock = Lock()
        self._lock.acquire()

    def set(self) -> None:
        if not self._lock.locked:
            raise RuntimeError("The event is already set.")

        self._lock.release()

    def clear(self) -> None:
        if self._lock.locked:
            raise RuntimeError("The event isn't set.")

        self._lock.acquire()

    def wait(self) -> None:
        self._lock.acquire()
        self._lock.release()

    @property
    def is_set(self) -> bool:
        return not self._lock.locked


class lockedproperty:
    def __init__(
        self,
        fget: Callable[..., Any],
        fset: Callable[..., Any] | None = None,
        fdel: Callable[..., Any] | None = None,
        doc: str | None = None,
    ) -> None:
        self.fget = fget
        self.fset = fset
        self.fdel = fdel

        if doc is None and fget is not None:
            doc = fget.__doc__  # type: ignore

        self.__doc__ = doc

        self._name: str = fget.__name__

    def __set__(self, instance: Any, value: Any) -> None:
        if self.fset is None:
            raise AttributeError("Cannot set attribute.")

        if not instance._property_lock:
            instance._property_lock = Lock()

        with instance._property_lock:
            self.fset(instance, value)

    def __get__(self, instance: Any, value: Any) -> None:
        if not instance._property_lock:
            instance._property_lock = Lock()

        with instance._property_lock:
            self.fget(instance, value)

    def setter(self, fset: Callable[..., Any]) -> "lockedproperty":
        return type(self)(self.fget, fset, self.fdel, self.__doc__)

    def getter(self, fget: Callable[..., Any]) -> "lockedproperty":
        return type(self)(fget, self.fset, self.fdel, self.__doc__)


class Semaphore:
    def __init__(self, limit: int) -> None:
        self._remaining = limit
        self._lock = Lock()
        self._waiters = Notification()

    def acquire(self) -> None:
        while True:
            with self._lock:
                if self._remaining > 0:
                    self._remaining -= 1
                    return

                self._waiters.wait()

    def release(self) -> None:
        with self._lock:
            self._remaining += 1

        self._waiters.notify_one()

    def __enter__(self) -> "Semaphore":
        self.acquire()

        return self

    def __exit__(self, *_) -> bool:
        self.release()

        return False


class Barrier:
    def __init__(self, waiters: int) -> None:
        self._waiters_needed = waiters
        self._waiters = 0
        self._broken_notification = Notification()
        self._finished_notification = Notification()
        self._inc_lock = Lock()

    def push(self) -> None:
        waiter = False

        with self._inc_lock:
            self._waiters += 1

            if self._waiters == self._waiters_needed:
                self._broken_notification.notify_all()
            else:
                waiter = True

        if waiter:
            self._broken_notification.wait()

        waiter = False

        with self._inc_lock:
            self._waiters -= 1

            if self._waiters == 0:
                self._finished_notification.notify_all()
            else:
                waiter = True

        if waiter:
            self._finished_notification.wait()


class Queue:
    def __init__(
        self,
        max_size: int | None = None,
        overflow_behavior: Literal["RAISE", "BLOCK", "DROP"] = "RAISE",
    ) -> None:
        self._inner: deque[Any] = deque([], max_size)
        self._overflow_behavior: Literal["RAISE", "BLOCK", "DROP"] = overflow_behavior
        self._added_element = Notification()
        self._popped_element = Notification()
        self._lock = Lock()

    def add(self, item: Any) -> None:
        with self._lock:
            if len(self._inner) == self._inner.maxlen:
                match self._overflow_behavior:
                    case "BLOCK":
                        self._popped_element.wait()
                    case "RAISE":
                        raise ValueError(
                            "Tried to add another element while the queue was already full."
                        )
                    case _:
                        pass

            self._inner.append(item)

            self._added_element.notify_one()

    def pop(self) -> Any:
        if len(self._inner) == 0:
            self._added_element.wait()

        obj = self._inner.popleft()

        self._popped_element.notify_one()

        return obj
