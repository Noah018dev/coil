import abc
import contextlib
from collections.abc import Callable, Generator
from typing import Any, NoReturn
from .threads import Promise, submit_global, Pool


class Supervisor:
    def __init__(self, pool_workers: int = 0) -> None:
        self._pool = None

        if pool_workers:
            self._pool = Pool(pool_workers)
            self._submit = self._pool.submit
        else:
            self._submit = submit_global

        self._jobs: dict[str, Promise] = {}

    def start_service(
        self, name: str, job: "Job", *args: Any, **kwargs: dict[str, Any]
    ) -> None:
        self._jobs[name] = job.promise(self._submit, *args, **kwargs)  # type: ignore

    def shutdown(self) -> None:
        def exits(**_) -> Promise:
            raise SystemExit("Shutting down.")

        self._submit = exits  # type: ignore

        for job in self._jobs.values():
            with contextlib.suppress(SystemExit):
                job.result()
        if self._pool:
            self._pool.shutdown()

    def __enter__(self) -> "Supervisor":
        return self

    def __exit__(self, *_) -> bool:
        self.shutdown()

        return False


class Job(abc.ABC):
    @abc.abstractmethod
    def __init__(
        self, func: Callable[..., Any], *args: Any, **kwargs: dict[str, Any]
    ) -> None: ...

    @abc.abstractmethod
    def promise(
        self,
        submit: Callable[[Callable[..., Any]], Promise],
        *args: Any,
        **kwargs: dict[str, Any],
    ) -> Promise: ...


class Once(Job):
    def __init__(
        self, func: Callable[..., Any], crash_manager: "Once | None" = None
    ) -> None:
        self._func, self._cmgr = func, crash_manager

    def promise(
        self,
        submit: Callable[[Callable[..., Any]], Promise],
        *args: Any,
        **kwargs: dict[str, Any],
    ) -> Promise:
        def run() -> None:
            try:
                return self._func(*args, **kwargs)
            except BaseException as e:
                if not self._cmgr:
                    raise

                crash_manager_result = self._cmgr.promise(submit, e).result()

                if crash_manager_result is None:
                    raise
                else:
                    return crash_manager_result

        return submit(run)


class Retry(Once):
    def __init__(
        self,
        func: Callable[..., Any],
        crash_manager: "Once | None" = None,
        max_consecutive_fails: int | None = None,
        max_total_fails: int | None = None,
    ) -> None:
        self._func, self._cmgr = func, crash_manager
        self._max_fails_total, self._max_consecutive_fails = (
            max_consecutive_fails if max_consecutive_fails else float("inf")
        ), (max_total_fails if max_total_fails else float("inf"))

    def promise(
        self,
        submit: Callable[[Callable[..., Any]], Promise],
        *args: Any,
        **kwargs: dict[str, Any],
    ) -> Promise:
        def run() -> None:
            max_fails = max(self._max_consecutive_fails, self._max_fails_total)
            fails = 0

            while True:

                try:
                    return self._func(*args, **kwargs)
                except BaseException as e:
                    if not self._cmgr:
                        fails += 1

                        if fails > max_fails:
                            raise

                        continue

                    crash_manager_result = self._cmgr.promise(submit, e).result()

                    if crash_manager_result is None:
                        fails += 1

                        if fails > max_fails:
                            raise

                        continue
                    else:
                        return crash_manager_result

        return submit(run)

    def loop_generator(
        self,
        submit: Callable[[Callable[..., Any]], Promise],
        *args: Any,
        **kwargs: dict[str, Any],
    ) -> Generator[Promise, Any, NoReturn]:
        consecutive_fails = 0
        total_fails = 0

        while True:
            try:
                yield submit(*args, **kwargs)
                consecutive_fails = 0
            except BaseException as e:
                if not self._cmgr:
                    consecutive_fails += 1
                    total_fails += 1

                    if (
                        consecutive_fails > self._max_consecutive_fails
                        or total_fails > self._max_fails_total
                    ):
                        raise

                    continue

                crash_manager_result = self._cmgr.promise(submit, e).result()

                if crash_manager_result is None:
                    consecutive_fails += 1
                    total_fails += 1

                    if (
                        consecutive_fails > self._max_consecutive_fails
                        or total_fails > self._max_fails_total
                    ):
                        raise
                else:
                    yield crash_manager_result
                    consecutive_fails = 0


class Loop(Job):
    def __init__(self, job: Job) -> None:
        self._job = job

    def promise(
        self,
        submit: Callable[[Callable[..., Any]], Promise],
        *args: Any,
        **kwargs: dict[str, Any],
    ) -> Promise:
        def run() -> None:
            if isinstance(self._job, Retry):
                for promise in self._job.loop_generator(submit, *args, **kwargs):
                    promise.result()
            else:
                while True:
                    self._job.promise(submit, *args, **kwargs).result()

        return submit(run)
