from collections.abc import Callable
from collections import deque
from typing import Self, Any
from dataclasses import dataclass
from .sync import Notification
from .jobs import Supervisor, Loop, Retry
import abc


@dataclass
class Message:
    group: "Group"
    content: Any


@dataclass
class MessageSentInGroup:
    cancel: bool = False
    forward_to: "set[Group]" = set()
    data_transform: Callable[[Any], Any] = lambda x: x


class Group:
    groups: "dict[str, Group]" = {}
    extensions: "dict[Group, Extension]" = {}

    def __new__(cls, name: str) -> "Group":
        if name in cls.groups:
            return cls.groups[name]

        instance = super().__new__(cls)

        return instance

    def __init__(self, name: str) -> None:
        if hasattr(self, "initialized"):
            return

        self.initialized = None

        parts = list(filter(None, name.split("/")))
        name = "/".join(parts)

        self.id = hash(name)
        self.parent: Group | None = None
        self.children: set[Group] = set()
        self.name = name

        for idx, _ in enumerate(parts):
            if idx == 0:
                continue

            Group("/".join(parts[0:idx])).children.add(self)

        is_wildcard = parts[-1] == "..."

        if len(parts) > 1 and not is_wildcard:
            self.parent = Group("/".join(parts[0:-1]) + "/...")

    @classmethod
    def _add_extension(cls, group: "Group", extension: "Extension") -> None:
        if group in cls.extensions:
            raise ValueError("An extension is already registered.")

        cls.extensions[group] = extension

        Supervisor().start_service(
            type(extension).__name__, Loop(Retry(extension.background_worker))
        )

    def add_extension(
        self, ext: "type[Extension]", *args: Any, **kwargs: dict[str, Any]
    ) -> None:
        self._add_extension(self, ext(self, Mailbox()).config(*args, **kwargs))

    def __hash__(self) -> int:
        return self.id


class Mailbox:
    chats: "dict[Group, set[Mailbox]]" = {}

    @classmethod
    def _subscribe(cls, box: "Mailbox", group: Group) -> None:
        cls.chats.setdefault(group, set()).add(box)

    @classmethod
    def _unsubscribe(cls, box: "Mailbox", group: Group) -> None:
        cls.chats[group].discard(box)

    @classmethod
    def send(
        cls, group: Group, content: Any, exclude: list[Group] | None = None
    ) -> None:
        if exclude is None:
            exclude = []

        if group in exclude:
            return

        if not group in cls.chats:
            return

        current_group = group
        response = MessageSentInGroup()
        while current_group is not None:
            if current_group in Group.extensions:
                response = Group.extensions[current_group].message_sent_in_group(
                    content
                )
                break
            current_group = current_group.parent

        if response is None:
            response = MessageSentInGroup()

        content = response.data_transform(content)

        if not response.cancel:
            for reciever in cls.chats[group]:
                reciever.messages.appendleft(Message(group, content))
                reciever._notification.notify_all()

            if group.parent is not None and group.parent not in exclude:
                cls.send(group.parent, content, [group] + exclude)

            for child in group.children:
                if child in exclude:
                    continue

                cls.send(child, content, exclude=[group] + exclude)

        for other in response.forward_to:
            cls.send(other, content, exclude=[group])

    def __init__(self) -> None:
        self.messages: deque[Message] = deque([])
        self._notification = Notification()

    def get(self) -> Message | None:
        if len(self.messages) == 0:
            self._notification.wait()

        return self.messages.pop()

    def subscribe(self, group: Group) -> None:
        self._subscribe(self, group)

    def unsubscribe(self, group: Group) -> None:
        self._unsubscribe(self, group)


class Extension(abc.ABC):
    def __init__(self, group: Group, mailbox: Mailbox) -> None:
        self._group = group
        self._mailbox = mailbox

    @abc.abstractmethod
    def config(self, *args: Any, **kwargs: dict[str, Any]) -> Self: ...

    @abc.abstractmethod
    def message_sent_in_group(self, content: Any) -> MessageSentInGroup | None: ...

    @abc.abstractmethod
    def background_worker(self) -> None: ...
