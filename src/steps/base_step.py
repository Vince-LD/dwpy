from abc import ABC, abstractmethod
from typing import Optional

import graphviz 

class PipelineContext:
    def __init__(self) -> None:
        self.input_dwi: list[str] = ["path0", "path1"]
        self.thread_count = 4

class BaseStep(ABC):
    NAME = "Base Step"
    def __init__(self, name: Optional[str]=None) -> None:
        super().__init__()
        self.name = name or self.NAME

    @abstractmethod
    def run(self, ctx: PipelineContext) : ... 

    def exec_cmd(self, cmd: list[str]): ...


class RootStep(BaseStep):
    NAME = "Root Step"

    def run(self, ctx: PipelineContext):
        pass