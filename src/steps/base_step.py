from abc import ABC, abstractmethod 

class PipelineContext:
    def __init__(self) -> None:
        self.input_dwi: list[str] = ["path0", "path1"]
        self.thread_count = 4

class BaseStep(ABC):
    NAME = "Abstract Step"
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def run(self, ctx: PipelineContext) : ... 

    def exec_cmd(self, cmd: list[str]): ...