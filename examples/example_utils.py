from dataclasses import dataclass
from typing import Any, Optional

from tuyau.base_step import BaseStep
from tuyau.context import BasePipelineContext, ContextVariable

import logging


@dataclass(slots=True)
class ExampleContext(BasePipelineContext):
    input_x: ContextVariable[float] = ContextVariable.new_field(0.0)
    input_y: ContextVariable[float] = ContextVariable.new_field(0.0)
    result_step1: ContextVariable[float] = ContextVariable.new_field(0.0)
    result_step3: ContextVariable[float] = ContextVariable.new_field(0.0)
    result_step4: ContextVariable[float] = ContextVariable.new_field(0.0)
    result_step5: ContextVariable[float] = ContextVariable.new_field(0.0)
    result_step6: ContextVariable[float] = ContextVariable.new_field(0.0)


class LogStep(BaseStep[ExampleContext]):
    NAME = "Print a field"

    def __init__(
        self,
        field: ContextVariable[Any],
        name: Optional[str] = None,
        comment: str = "",
    ) -> None:
        super().__init__(name, comment)
        self.field = field

    def run(self, ctx: ExampleContext):
        with ctx:
            logging.info(f"Printing the field value: {self.field.get()}")
        self.complete()


class AddStep(BaseStep[ExampleContext]):
    NAME = "MuiltiPly two numbers"

    def __init__(
        self,
        x_field: ContextVariable[float],
        y_field: ContextVariable[float],
        res_field: ContextVariable[float],
        name: str | None = None,
        comment: str = "",
    ) -> None:
        super().__init__(name, comment)
        self.x_field = x_field
        self.y_field = y_field
        self.res_field = res_field
        self.result: Optional[float] = None

    def run(self, ctx: ExampleContext):
        with ctx:
            x, y = self.x_field.get(), self.y_field.get()
            self.result = x + y
            self.res_field.set(self.result)

        logging.info(f"{x} + {y} = {self.result}")
        self.complete()

    def label(self) -> str:
        return f"{super().label()}\nresult: {self.result}"


class MutliplyStep(BaseStep[ExampleContext]):
    NAME = "MuiltiPly two numbers"

    def __init__(
        self,
        x_field: ContextVariable[float],
        y_field: ContextVariable[float],
        res_field: ContextVariable[float],
        name: str | None = None,
        comment: str = "",
    ) -> None:
        super().__init__(name, comment)
        self.x_field = x_field
        self.y_field = y_field
        self.res_field = res_field
        self.result: Optional[float] = None

    def run(self, ctx: ExampleContext):
        with ctx:
            x, y = self.x_field.get(), self.y_field.get()
            self.result = x * y
            self.res_field.set(self.result)

        logging.info(f"{x} * {y} = {self.result}")
        self.complete()

    def label(self) -> str:
        return f"{super().label()}\nresult: {self.result}"


class SkipStep(BaseStep[ExampleContext]):
    NAME = "Skipped step"

    def run(self, ctx: ExampleContext):
        logging.info(f"{self.name}")
        self.skipped()
