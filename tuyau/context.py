from dataclasses import dataclass, field
from threading import Lock
from typing import (
    Iterable,
    Generic,
    Optional,
    ParamSpec,
    Self,
    TypeVar,
    Union,
    cast,
    get_args,
)


ContextT = TypeVar("ContextT", bound="BasePipelineContext")
T = TypeVar("T")


class CtxVar(Generic[T]):
    def __init__(self, value: T) -> None:
        self.__value = value

    def get(self) -> T:
        return self.__value

    def set(self, value: T):
        self.__value = value

    @classmethod
    def new_field(
        cls, value: T, init: bool = True, repr: bool = True, kw_only: bool = True
    ) -> Self:
        return field(
            init=init, repr=repr, kw_only=kw_only, default_factory=lambda: cls(value)
        )

    def __repr__(self) -> str:
        return str(self.__value)

    def type(self) -> type[T]:
        return type(self.__value)

    @property
    def T(self) -> T:
        return cast(T, self.__value)


OptionalCtxVar = CtxVar[Optional[T]] | CtxVar[T]


@dataclass(slots=True)
class BasePipelineContext:
    thread_count: int = 4
    # _thread_lock: Lock = field(init=False, repr=False, default_factory=Lock)
    _fields_: set[str] = field(init=False, repr=False, default_factory=set)

    def __enter__(self) -> Self:
        # self._thread_lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # self._thread_lock.release()
        return
