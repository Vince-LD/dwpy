class BasePipelineError(ValueError):
    pass

class NoDefaultError(BasePipelineError):
    pass

class CommandFailed(BasePipelineError):
    def __init__(self, msg: str, return_code: int) -> None:
        super().__init__(msg)
        self.return_code = return_code