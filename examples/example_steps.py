import shlex
import time
from typing import Optional

from pipelyne.exceptions import CommandFailed
from pipelyne.base_step import BaseStep, BasePipelineContext
import subprocess
import logging


class TestStep(BaseStep):
    NAME = "test Step"

    def __init__(self, name: Optional[str]=None, comment: str = "") -> None:
        super().__init__(name, comment)

    def run(self, ctx: BasePipelineContext):
        with ctx:
            logging.info(f"{self.name} do something")
        self.complete()

    def exec_cmd(self, cmd: list[str]):
        # _cmd = shlex.join(cmd)
        proc = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            shell=True,
            universal_newlines=True
        )
        stdout, stderr = proc.communicate()
        return_code: int = proc.returncode
        if return_code != 0:
            raise CommandFailed(f"Command {cmd} failed", return_code)
        logging.info(self.NAME, " => ", return_code, str(stdout), str(stdout))
