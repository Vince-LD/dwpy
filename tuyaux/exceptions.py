class BasePipelineError(ValueError):
    pass


class NoDefaultError(BasePipelineError):
    pass


class ConditionError(BasePipelineError):
    pass


class InputOutputConflictError(BasePipelineError):
    pass

class CycleError(BasePipelineError):
    pass
