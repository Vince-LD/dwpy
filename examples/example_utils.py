from dataclasses import dataclass
import time
from typing import Any, Optional

from tuyau.steps.base_step import BaseStep
from tuyau.context import BasePipelineContext, CtxVar, CtxVar, NoDefault

import logging

from tuyau.exceptions import BasePipelineError


class ExampleInvalidInputsError(BasePipelineError):
    def __init__(self, message: str) -> None:
        logging.exception(message)
        super().__init__(message)


@dataclass(slots=True)
class ExampleContext(BasePipelineContext):
    input_x: CtxVar[float] = CtxVar.new_field(0.0)
    input_y: CtxVar[float] = CtxVar.new_field(0.0)
    result_step1: CtxVar[float] = CtxVar.new_field(NoDefault)
    result_step3: CtxVar[float] = CtxVar.new_field(NoDefault)
    result_step4: CtxVar[float] = CtxVar.new_field(NoDefault)
    result_step5: CtxVar[float] = CtxVar.new_field(NoDefault)
    result_step6: CtxVar[float] = CtxVar.new_field(NoDefault)
    result_step7: CtxVar[float] = CtxVar.new_field(NoDefault)
    issou: CtxVar[float] = CtxVar.new_field(NoDefault)


class LogStep(BaseStep[ExampleContext]):
    NAME = "Print a field"

    def __init__(
        self,
        field: CtxVar[Any],
        name: Optional[str] = None,
        comment: str = "",
    ) -> None:
        super().__init__(name, comment)
        self.field = field

    def run(self, ctx: ExampleContext):
        with ctx:
            logging.info(f"Printing the field value: {self.field.get()}")
            time.sleep(1)
        self.completed()


class AdditionStep(BaseStep[ExampleContext]):
    NAME = "Add two numbers"

    def __init__(
        self,
        a_field: CtxVar[float],
        b_field: CtxVar[float],
        res_field: CtxVar[float],
        name: str | None = None,
        comment: str = "",
    ) -> None:
        super().__init__(name, comment)
        self.a_field = a_field
        self.b_field = b_field
        self.res_field = res_field
        self.result: Optional[float] = None

    def run(self, ctx: ExampleContext):
        a, b = self.a_field.get(), self.b_field.get()
        if a is None or b is None:
            raise ExampleInvalidInputsError(
                "Invalid input, one of the fields is "
                f"None: {self.a_field.get()=}, {self.b_field.get()}"
            )
        self.result = a + b
        self.res_field.set(self.result)

        logging.info(f"{a} + {b} = {self.result}")
        time.sleep(1)
        self.completed()

    def label(self) -> str:
        return f"{super().label()}\nresult: {self.result}"


class MutliplyStep(BaseStep[ExampleContext]):
    NAME = "MuiltiPly two numbers"

    def __init__(
        self,
        a_field: CtxVar[float],
        b_field: CtxVar[float],
        res_field: CtxVar[float],
        name: str | None = None,
        comment: str = "",
    ) -> None:
        super().__init__(name, comment)
        self.a_field = a_field
        self.b_field = b_field
        self.res_field = res_field
        self.result: Optional[float] = None

    def run(self, ctx: ExampleContext):
        a, b = self.a_field.get(), self.b_field.get()
        if a is None or b is None:
            raise ExampleInvalidInputsError(
                "Invalid input, one of the fields is "
                f"None: {self.a_field.get()=}, {self.b_field.get()}"
            )
        self.result = a * b
        self.res_field.set(self.result)

        logging.info(f"{a} * {b} = {self.result}")
        time.sleep(1)
        self.completed()

    def label(self) -> str:
        return f"{super().label()}\nresult: {self.result}"


class SkipStep(BaseStep[ExampleContext]):
    NAME = "Skipped step"

    def run(self, ctx: ExampleContext):
        logging.info(f"{self.name}")
        self.skipped()
        time.sleep(1)
