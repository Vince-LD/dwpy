from abc import abstractmethod
import logging
import pprint
from typing import Any, Callable, Generic, ParamSpec, Self, TypeVar
from tuyau.context import BasePipelineContext, ContextT
from tuyau.steps.base_step import BaseStep
from tuyau.context import PipeVar, InVar, OutVar


P = ParamSpec("P")
R = TypeVar("R")


class RootStep(BaseStep[ContextT]):
    NAME = "Start"
    STYLES = {}
    DEFAULT_STYLE = {
        "shape": "plaintext",
    }

    def __init__(self, context_class: type[ContextT]) -> None:
        super().__init__()
        self.context_class = context_class
        self.values: dict[str, tuple[Any, type]] = {}
        # self.set_values(context_class())
        self.ctx = context_class()

    def run(self, ctx: ContextT):
        # self.set_values(ctx)
        self.completed()

    def label(self) -> str:
        # flabel = pprint.pformat(self.ctx)
        flabel = str(self.ctx).rstrip(")")
        cls_name, fields_ = flabel.split("(", 1)
        split_fields = fields_.split(",")
        label = "\n".join([cls_name, *split_fields])

        return label


class FinalStep(RootStep):
    NAME = "End"

    def run(self, ctx: BasePipelineContext):
        self.ctx = ctx
        self.completed()


class FuncStep(BaseStep, Generic[P, R]):
    NAME = "Function step"

    def __init__(
        self,
        result_vars: OutVar[R] | tuple[OutVar, ...],
        name: str | None = None,
        comment: str = "",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        super().__init__(name=name, comment=comment)
        self._outputs: tuple[OutVar, ...] = (
            result_vars
            if isinstance(result_vars, tuple)
            and all(isinstance(f, OutVar) for f in result_vars)
            else (result_vars,)
        )  # type: ignore
        self.args = args
        self.kwargs = kwargs
        self._inputs = tuple(var for var in args if isinstance(var, InVar)) + tuple(
            var for var in kwargs.values() if isinstance(var, InVar)
        )
        super().__init__()

    def run(self, ctx: BasePipelineContext):
        args = tuple(
            arg.get() if isinstance(arg, PipeVar) else arg for arg in self.args
        )
        kwargs = {
            key: (arg.get() if isinstance(arg, PipeVar) else arg)
            for key, arg in self.kwargs.items()
        }
        self._cast_results(self.function(*args, **kwargs))  # type: ignore
        self.completed()

    def _cast_results(self, results: R) -> None:
        cast_size = len(self._outputs)
        outputs = results if isinstance(results, tuple) else (results,)
        output_size = len(outputs)
        if cast_size == output_size:
            for r, var in zip(outputs, self._outputs):
                var.set(r)
            return

        if cast_size == 1 and output_size > 1:
            self._outputs[0].set(outputs[0])
            return

        if cast_size > 1 and output_size == 1:
            (res,) = outputs
            for var in self._outputs:
                var.set(res)
            return

        raise ValueError(
            "Number of result PipeVar object given does not match number of "
            f"function outputs and cannot be casted: {cast_size=} != {output_size=}"
        )

    def _is_outvar_tuple(self, value: OutVar[R] | tuple[OutVar, ...]) -> bool:
        return isinstance(value, tuple) and all(isinstance(f, OutVar) for f in value)

    def _is_outvar(self, value: OutVar[R] | tuple[OutVar, ...]) -> bool:
        return isinstance(value, OutVar)

    def label(self) -> str:
        str_args = ", ".join(str(arg) for arg in self.args)
        str_kwargs = ", ".join(
            f"{name}={str(arg)}" for name, arg in self.kwargs.items()
        )
        return (
            f"Function Step: {self.name}\n"
            f"function: {self.function.__name__}\n"
            f"args: {str_args}\n"
            f"kwargs: {str_kwargs}"
        )

    @property
    @abstractmethod
    def function(self) -> Callable[P, R]:
        ...

    @classmethod
    def new(cls, func: Callable[P, R]) -> type[Self]:
        class NewFuncStep(cls):
            @property
            def function(self) -> Callable[P, R]:
                return func

        return NewFuncStep  # type: ignore

    def inputs(self) -> tuple[InVar, ...]:
        return self._inputs

    def outputs(self) -> tuple[OutVar, ...]:
        return self._outputs
