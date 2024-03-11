from dataclasses import dataclass, field, fields
from threading import Lock
from typing import (
    Generic,
    Self,
    TypeVar,
    cast,
    final,
)
from tuyau.exceptions import NoDefaultError

ContextT = TypeVar("ContextT", bound="BasePipelineContext")
T = TypeVar("T")


@final
class NoDefault:
    pass


class PipeVar(Generic[T]):
    def __init__(self, value: T | type[NoDefault]) -> None:
        self.__value = value
        self.__name: str = ""

    def set_name(self, name: str):
        self.__name = name

    def get_name(self) -> str:
        return self.__name

    def get(self) -> T:
        value = self.__value
        if value is NoDefault:
            raise NoDefaultError(
                "Cannot get a value that was not initialized (=NoDefault). "
                f"If a {self.__class__.__name__} is inititalized with the value "
                f"{NoDefault.__class__.__name__}, you cannot get it until method "
                "PipeVar.set([new_value]) is called."
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

    def as_input(self) -> "InVar[T]":
        return InVar(self)

    def as_output(self) -> "OutVar[T]":
        return OutVar(self)

    def as_inout(self) -> "InOutVar[T]":
        return InOutVar(self)


class _IOVar(Generic[T]):
    def __init__(self, var: PipeVar[T]) -> None:
        self._var = var

    def as_pipevar(self) -> PipeVar[T]:
        return self._var

    @property
    def T(self) -> T:
        return cast(T, self._var)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._var})"


class InVar(_IOVar[T]):
    def get(self) -> T:
        return self._var.get()


class OutVar(_IOVar[T]):
    def set(self, value: T):
        self._var.set(value)


class InOutVar(InVar[T], OutVar[T]):
    def get(self) -> T:
        return self._var.get()

    def set(self, value: T):
        self._var.set(value)


@dataclass
class BasePipelineContext:
    thread_count: int = 4
    _thread_lock: Lock = field(init=False, repr=False, default_factory=Lock)
    _fields_: set[str] = field(init=False, repr=False, default_factory=set)

    def __post_init__(self):
        for field_ in fields(self):
            value = getattr(self, field_.name)
            if isinstance(value, PipeVar):
                value.set_name(field_.name)

    def __enter__(self) -> Self:
        self._thread_lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._thread_lock.release()
        return
