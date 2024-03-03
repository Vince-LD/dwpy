from dataclasses import dataclass, field
import logging
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
    final,
    get_args,
)
from tuyau.exceptions import NoDefaultError

ContextT = TypeVar("ContextT", bound="BasePipelineContext")
T = TypeVar("T")


@final
class NoDefault:
    pass


class CtxVar(Generic[T]):
    def __init__(self, value: T | type[NoDefault]) -> None:
        self.__value = value

    def get(self) -> T:
        value = self.__value
        if value is NoDefault:
            raise NoDefaultError(
                "Cannot get a value that was not initialized (=NoDefault). "
                f"If a {self.__class__.__name__} is inititalized with the value "
                f"{NoDefault.__class__.__name__}, you cannot get it until method "
                "CtxVar.set([new_value]) is called."
            )
        return value

    def set(self, value: T):
        self.__value = value

    @classmethod
    def new_field(
        cls,
        value: T | type[NoDefault],
        init: bool = True,
        repr: bool = True,
        kw_only: bool = True,
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
        return cast(T, self)


@dataclass(slots=True)
class BasePipelineContext:
    thread_count: int = 4
    _thread_lock: Lock = field(init=False, repr=False, default_factory=Lock)
    _fields_: set[str] = field(init=False, repr=False, default_factory=set)

    def __enter__(self) -> Self:
        self._thread_lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._thread_lock.release()
        return
