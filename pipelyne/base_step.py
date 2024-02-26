from abc import ABC, abstractmethod
from typing import Optional
from enum import Flag, auto
from context import BasePipelineContext


class StatusEnum(Flag):
    UNKNOWN = auto()
    RUNNING = auto()
    COMPLETE = auto()
    SKIPPED = auto()
    ERROR = auto()


STATUS_PASSED = StatusEnum.COMPLETE | StatusEnum.SKIPPED


class BaseStep(ABC):
    NAME = "Base Step"
    STYLES: dict[StatusEnum, dict[str, str]] = {
        StatusEnum.UNKNOWN: {"shape": "box", "color": "black", "style": "dashed"},
        StatusEnum.RUNNING: {"shape": "box", "color": "blue"},
        StatusEnum.COMPLETE: {"shape": "invhouse", "color": "green"},
        StatusEnum.SKIPPED: {"shape": "invhouse", "color": "grey"},
        StatusEnum.ERROR: {"shape": "house", "color": "red"},
    }
    DEFAULT_STYLE: dict[str, str] = {}
    COMMENT = ""

    def __init__(self, name: Optional[str] = None, comment: str = "") -> None:
        super().__init__()
        self.name = name or self.NAME
        self.comment = comment or self.COMMENT
        self._status = StatusEnum.UNKNOWN
        self._id = id(self)
        self._str_id = str(self._id)

    @abstractmethod
    def run(self, ctx: BasePipelineContext):
        self.complete()

    @property
    def id(self) -> int:
        return self._id

    @property
    def str_id(self) -> str:
        return self._str_id

    @property
    def status(self) -> StatusEnum:
        return self._status

    def unknown(self):
        self._status = StatusEnum.UNKNOWN

    def running(self):
        self._status = StatusEnum.RUNNING

    def complete(self):
        self._status = StatusEnum.COMPLETE

    def skipped(self):
        self._status = StatusEnum.SKIPPED

    def error(self):
        self._status = StatusEnum.ERROR

    def style(self) -> dict[str, str]:
        return self.STYLES.get(self._status, self.DEFAULT_STYLE)

    def label(self) -> str:
        return f"{self.name}{'\n' if self.comment else ''}{self.comment}"


class RootStep(BaseStep):
    NAME = "Start"
    STYLES = {}
    DEFAULT_STYLE = {"shape": "diamond", "color": "blue"}

    def run(self, ctx: BasePipelineContext):
        self.complete()
