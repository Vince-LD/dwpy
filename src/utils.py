from typing import Callable

from src.steps.base_step import BaseStep

def log_step(step: BaseStep):
    print(step)

def log_cmd(cmd: list[str]): 
    print(" ".join(cmd))
    