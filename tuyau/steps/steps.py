from typing import Any, Callable, Generic, ParamSpec, TypeVar
from tuyau.context import ContextT
from dataclasses import fields
from tuyau.steps.base_step import BaseStep
from tuyau.context import CtxVar, OptionalCtxVar

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
        self.set_values(context_class())

    def run(self, ctx: ContextT):
        self.set_values(ctx)
        self.completed()

    def label(self) -> str:
        # return f"{pformat(ctx)}"
        lines: list[str] = [f"{self.context_class.__name__}:"]
        for field, (value, type_) in self.values.items():
            lines.append(f" - {field}: {type_.__name__} = {value}")
        return "\n".join(lines)

    def set_values(self, ctx: ContextT):
        for field in fields(ctx):
            if field.init:
                v = getattr(ctx, field.name)
                if isinstance(v, CtxVar):
                    self.values[field.name] = (v.get(), v.type())
                else:
                    self.values[field.name] = (v, type(v))


class FinalStep(RootStep):
    NAME = "End"


class FuncStep(BaseStep, Generic[P]):
    def __init__(
        self,
        func: Callable[P, R],
        result_vars: OptionalCtxVar[R] | tuple[OptionalCtxVar[Any], ...],
        name: str | None = None,
        comment: str = "",
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        super().__init__(name, comment)
        self.func = func
        assert self._is_ctxvar_tuple(result_vars) or self._is_ctxvar(result_vars)
        self._outputs: tuple[OptionalCtxVar, ...] = (
            result_vars
            if isinstance(result_vars, tuple)
            and all(isinstance(f, CtxVar) for f in result_vars)
            else (result_vars,)
        )  # type: ignore
        self.args = args
        self.kwargs = kwargs
        super().__init__()

    def run(self, ctx: CtxVar):
        args = tuple(arg.get() if isinstance(arg, CtxVar) else arg for arg in self.args)
        kwargs = {
            key: arg.get() if isinstance(arg, CtxVar) else arg
            for key, arg in self.kwargs.items()
        }
        self._cast_results(self.func(*args, **kwargs))  # type: ignore
        self.completed()

    def _cast_results(self, results: R) -> None:
        cast_size = len(self._outputs)
        outputs = results if isinstance(results, tuple) else (results,)
        output_size = len(outputs)
        if cast_size > 1 and output_size > 1:
            if cast_size == output_size:
                for r, var in zip(outputs, self._outputs):
                    var.set(r)
                return
            raise ValueError(
                "Number of result CtxVar object given does not match number of "
                f"function outputs and cannot be casted: {cast_size=} != {output_size=}"
            )

        if cast_size == 1 and output_size > 1:
            self._outputs[0].set(outputs[0])
            return

        if cast_size > 1 and output_size == 1:
            (res,) = outputs
            for var in self._outputs:
                var.set(res)

    def _is_ctxvar_tuple(
        self, value: OptionalCtxVar[R] | tuple[OptionalCtxVar, ...]
    ) -> bool:
        return isinstance(value, tuple) and all(isinstance(f, CtxVar) for f in value)

    def _is_ctxvar(self, value: OptionalCtxVar[R] | tuple[OptionalCtxVar, ...]) -> bool:
        return isinstance(value, CtxVar)
