from .cogs import Trigger, wait_until_trigger as _wait_until_trigger


class TimeTrigger(Trigger):
    id = 0x00

    def __init__(self, time: float) -> None:
        self._time = time

    @property
    def int_repr(self) -> list[int | str]:
        return [self.id, round(self._time * 10**9)]


def wait_until_trigger(trigger: Trigger) -> None:
    _wait_until_trigger(trigger.int_repr)
