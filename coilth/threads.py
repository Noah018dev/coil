import asyncio
from collections.abc import Callable, Generator, Iterable, Mapping
from collections import deque
import contextlib
from dataclasses import dataclass
from typing import Any, Literal, Self, cast
from .cogs import new_thread
from .sync import Event, Notification, Lock


@dataclass
class ThreadResult:
    result: Any
    exception: BaseException | BaseExceptionGroup | None
    crashed: bool


@dataclass
class ThreadStatus:
    status_string: Literal["NOT_STARTED", "RUNNING", "FINISHED"]
    result: None | ThreadResult


@dataclass
class Task:
    function: Callable[..., Any]
    args: tuple[Any]
    kwargs: dict[str, Any]
    promise: "Promise"


class Thread:
    def __init__(
        self,
        function: Callable[..., Any],
        args: Iterable[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        self._function = function
        self._args = args if args else ()
        self._kwargs = kwargs if kwargs else cast(Mapping[str, Any], {})
        self._status = ThreadStatus("NOT_STARTED", None)
        self._finished = Event()

    def __worker_single(self) -> None:
        self._status = ThreadStatus("RUNNING", None)

        try:
            result = self._function(*self._args, **self._kwargs)

            self._status = ThreadStatus("FINISHED", ThreadResult(result, None, False))
        except BaseException as e:
            self._status = ThreadStatus("FINISHED", ThreadResult(None, e, True))
        finally:
            self._finished.set()

    def start(self) -> Self:
        new_thread(self.__worker_single, (), {})

        return self

    @property
    def running(self) -> bool:
        return self._status.status_string == "RUNNING"

    @property
    def finished(self) -> bool:
        return self._status.status_string == "FINISHED"

    @property
    def result(self) -> Any:
        if not self.finished or self._status.result is None:
            raise RuntimeError("Thread has not finished, cannot access result.")

        return self._status.result.result

    def join(self) -> Any:
        if self._status.status_string == "NOT_STARTED":
            raise RuntimeError("Thread has not started, cannot join it.")

        self._finished.wait()

        return self.result

    @property
    def exception(self) -> BaseException | None:
        if not self.finished or self._status.result is None:
            raise RuntimeError("Thread has not finished, cannot access exception.")

        return self._status.result.exception

    def __await__(self) -> Generator[Any, None, Any]:
        return Promise(self).__await__()


class Pool:
    _stack: "list[Pool]" = []

    def __init__(self, workers: int = 4) -> None:
        self._tasks: deque[Task] = deque([])
        self._workers: dict[int, Task | None] = {}
        self._task_notification = Notification()
        self._task_array_lock = Lock()
        self._task_processed = Notification()
        self._shutting_down = False
        self.active = True

        for i in range(workers):
            submit_global(self.__worker, i)

    def __enter__(self) -> None:
        self._stack_append(self)

    def __exit__(self, *_) -> bool:
        self._stack_pop()
        self.shutdown()

        return False

    def shutdown(self) -> None:
        self._shutting_down = True

        while self._tasks:
            self._task_processed.wait()

        self.active = False

    def __worker(self, id: int) -> None:
        while self.active:
            self._workers[id] = None

            if not self._tasks and not self._task_array_lock.locked:
                self._task_notification.wait()

            task = None

            with self._task_array_lock:
                try:
                    task = self._tasks.popleft()
                except IndexError:
                    continue

            assert task  # satisfy the type checker

            updater: Callable[[ThreadStatus], None] = lambda info: task.promise._update(info)  # type: ignore

            updater(ThreadStatus("RUNNING", None))
            self._workers[id] = task

            try:
                result = task.function(*task.args, **task.kwargs)

                updater(ThreadStatus("FINISHED", ThreadResult(result, None, False)))
            except BaseException as e:
                updater(ThreadStatus("FINISHED", ThreadResult(None, e, True)))
            finally:
                self._task_processed.notify_all()

    def submit(
        self, function: Callable[..., Any], *args: Any, **kwargs: dict[str, Any]
    ) -> "Promise":
        if self._shutting_down:
            raise RuntimeError("Pool is shutting down, cannot add more tasks.")

        promise = Promise(None)

        with self._task_array_lock:
            self._tasks.append(Task(function, args, kwargs, promise))

        with contextlib.suppress(RuntimeError):
            self._task_notification.notify_one()

        return promise

    @classmethod
    def _stack_append(cls, pool: "Pool") -> None:
        cls._stack.append(pool)

    @classmethod
    def _stack_pop(cls) -> None:
        cls._stack.pop()

    def imap(self, func: Callable[..., Any], arr: list[Any]) -> "list[Promise]":
        return [self.submit(func, item) for item in arr]

    def map(self, func: Callable[..., Any], arr: list[Any]) -> list[Any]:
        promises = self.imap(func, arr)

        return [promise.result() for promise in promises]


class Promise:
    def __init__(self, parent: Thread | None) -> None:
        self._p = parent
        self._new_info = ThreadStatus("NOT_STARTED", None)
        self._finished_event = Event()

    @property
    def _info(self) -> ThreadStatus:
        if self._p:
            return self._p._status  # type: ignore
        else:
            return self._new_info

    @property
    def started(self) -> bool:
        return self._info.status_string != "NOT_STARTED"

    @property
    def finished(self) -> bool:
        return self._info.status_string == "FINISHED"

    def _update(self, info: ThreadStatus) -> None:
        self._new_info = info

        if info.status_string == "FINISHED":
            self._finished_event.set()

    def result(self) -> Any:
        if self._p:
            self._p.join()
        else:
            self._finished_event.wait()

        assert self._info.result is not None  # satisfy the type checker

        if self._info.result.exception:
            raise self._info.result.exception
        else:
            return self._info.result.result

    def __await__(self) -> Generator[Any, None, Any]:
        return asyncio.to_thread(self.result).__await__()

    def __call__(self) -> Any:
        return self.result()


def submit(
    function: Callable[..., Any], *args: Any, **kwargs: dict[str, Any]
) -> Promise:
    if len(Pool._stack) > 0:  # type: ignore
        return Pool._stack[-1].submit(function, *args, **kwargs)  # type: ignore

    return submit_global(function, *args, **kwargs)


def submit_global(
    function: Callable[..., Any], *args: Any, **kwargs: dict[str, Any]
) -> Promise:
    thread = Thread(function, args, kwargs)
    thread.start()

    return Promise(thread)
