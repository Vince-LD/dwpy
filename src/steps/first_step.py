import shlex
import time
from typing import Optional

from exceptions import CommandFailed
from .base_step import BaseStep, PipelineContext
import subprocess
import logging


class TestStep(BaseStep):
    NAME = "test Step"

    def __init__(self, name: Optional[str]=None) -> None:
        super().__init__(name)

    def run(self, ctx: PipelineContext):
        # print(ctx.input_dwi)
        logging.info(f"{self.NAME} running...")
        time.sleep(1)
        logging.info(f"{self.NAME} finished...")

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
