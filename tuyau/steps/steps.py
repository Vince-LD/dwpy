from abc import abstractmethod
import logging
import pprint
from typing import Any, Callable, Generic, ParamSpec, Self, TypeVar
from tuyau.context import BasePipelineContext, ContextT
from tuyau.steps.base_step import BaseStep
from tuyau.context import PipeVar


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
        # return f"{pformat(ctx)}"
        # lines: list[str] = [f"{self.context_class.__name__}:"]
        # for field, (value, type_) in self.values.items():
        #     lines.append(f" - {field}: {type_.__name__} = {value}")
        return pprint.pformat(self.ctx)

    # def set_values(self, ctx: ContextT):
    #     for field in fields(ctx):
    #         if field.init:
    #             v = getattr(ctx, field.name)
    #             if isinstance(v, PipeVar):
    #                 self.values[field.name] = (v.get(), v.type())
    #             else:
    #                 self.values[field.name] = (v, type(v))


class FinalStep(RootStep):
    NAME = "End"

    def run(self, ctx: BasePipelineContext):
        self.ctx = ctx
        self.completed()


class FuncStep(BaseStep, Generic[P, R]):
    NAME = "Function step"

    def __init__(
        self,
        result_vars: PipeVar[R] | tuple[PipeVar, ...],
        name: str | None = None,
        comment: str = "",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        super().__init__(name=name, comment=comment)
        assert self._is_PipeVar_tuple(result_vars) or self._is_PipeVar(result_vars)
        self._outputs: tuple[PipeVar, ...] = (
            result_vars
            if isinstance(result_vars, tuple)
            and all(isinstance(f, PipeVar) for f in result_vars)
            else (result_vars,)
        )  # type: ignore
        self.args = args
        self.kwargs = kwargs
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

    def _is_PipeVar_tuple(self, value: PipeVar[R] | tuple[PipeVar, ...]) -> bool:
        return isinstance(value, tuple) and all(isinstance(f, PipeVar) for f in value)

    def _is_PipeVar(self, value: PipeVar[R] | tuple[PipeVar, ...]) -> bool:
        return isinstance(value, PipeVar)

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


if __name__ == "__main__":

    def test(a: int) -> int:
        return a

    F = FuncStep.new(test)
    f = F(PipeVar[int](0), a=PipeVar(12).T)
    f.run(BasePipelineContext())
