class BasePipelineError(ValueError):
    pass

class NoDefaultError(BasePipelineError):
    pass

class ConditionError(BasePipelineError):
    pass