from typing import Never
from .events import TimeTrigger, wait_until_trigger
from .threads import Thread, Promise, submit, submit_global, Pool
from .sync import Lock, Notification, Event
from .mailbox import Mailbox, Message, Group
from .jobs import Supervisor


def sleep(time: float) -> None:
    wait_until_trigger(TimeTrigger(time))


def sleep_indefinitely() -> Never:
    while True:
        sleep(3600)


class jobs:
    from .jobs import Job, Once, Retry, Loop


__all__ = [
    "TimeTrigger",
    "wait_until_trigger",
    "Thread",
    "Lock",
    "Notification",
    "sleep",
    "Event",
    "Promise",
    "submit",
    "submit_global",
    "Mailbox",
    "Message",
    "Group",
    "Pool",
    "Supervisor",
    "jobs",
]
