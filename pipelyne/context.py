from dataclasses import dataclass, field, fields
from threading import Lock
from typing import Iterable, Protocol, Self, TypeVar

from pipelyne.exceptions import PipelineError

ContextT = TypeVar("ContextT", bound="BasePipelineContext")


class InvalidContextFields(PipelineError):
    def _init_(
        self, context_cls: type[ContextT], missing_fields: Iterable[str]
    ) -> None:
        self.message = (
            f"Fields '{' '.join(missing_fields)}' do not exist in "
            f"context class {context_cls}."
        )
        super().__init__(self.message)


@dataclass(slots=True)
class BasePipelineContext:
    thread_count = 4
    _thread_lock: Lock = field(init=False, repr=False, default_factory=Lock)
    _fields_: set[str] = field(init=False, repr=False, default_factory=set)

    def __enter__(self) -> Self:
        self._thread_lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._thread_lock.release()

    @classmethod
    def validate_fields(cls, *args: str):
        print(args)
        # if not cls._fields_:
        fields_name = set(f.name for f in fields(cls))
            # print(cls._fields_)
        if err_fields := tuple(filter(lambda f_name: f_name not in fields_name, args)):
            print(err_fields)
            raise InvalidContextFields(cls, err_fields)
