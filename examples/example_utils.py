from collections.abc import Callable
from dataclasses import dataclass
from typing import Optional

from pipelyne.base_step import BaseStep
from pipelyne.context import BasePipelineContext

import logging


@dataclass(slots=True)
class ExampleContext(BasePipelineContext):
    input_x: float = 0.0
    input_y: float = 0.0
    result_step1: Optional[float] = None
    result_step3: Optional[float] = None
    result_step4: Optional[float] = None
    result_step5: Optional[float] = None
    result_step6: Optional[float] = None


class LogStep(BaseStep[ExampleContext]):
    NAME = "Print a field"

    def __init__(
        self,
        field: str,
        name: Optional[str] = None,
        comment: str = "",
    ) -> None:
        super().__init__(name, comment)
        self.field = field

    def run(self, ctx: ExampleContext):
        with ctx:
            logging.info(f"Printing the field value: {getattr(ctx, self.field)}")
        self.complete()

class AddStep(BaseStep[ExampleContext]):
    NAME = "MuiltiPly two numbers"

    def __init__(
        self,
        x_field: str, y_field: str, res_field: str, name: str | None = None,
        comment: str = "",
    ) -> None:
        super().__init__(name, comment)
        ExampleContext.validate_fields(x_field, y_field, res_field)
        self.x_field = x_field
        self.y_field = y_field
        self.res_field = res_field


    def run(self, ctx: ExampleContext):
        with ctx:
            x, y = getattr(ctx, self.x_field), getattr(ctx, self.y_field)
            setattr(ctx, self.res_field, x + y)
        
        logging.info(f"{x} + {y} = {x + y}")
        self.complete()


class MutliplyStep(BaseStep[ExampleContext]):
    NAME = "MuiltiPly two numbers"

    def __init__(
        self,
        x_field: str, y_field: str, res_field: str, name: str | None = None,
        comment: str = "",
    ) -> None:
        super().__init__(name, comment)
        ExampleContext.validate_fields(x_field, y_field, res_field)
        self.x_field = x_field
        self.y_field = y_field
        self.res_field = res_field

    def run(self, ctx: ExampleContext):
        with ctx:
            x, y = getattr(ctx, self.x_field), getattr(ctx, self.y_field)
            setattr(ctx, self.res_field, x * y)
        
        logging.info(f"{x} + {y} = {x * y}")
        self.complete()

class SkipStep(BaseStep[ExampleContext]):
    NAME = "Skipped step"

    def run(self, ctx: ExampleContext):
        logging.info(f"{self.name}")
        self.skipped()
