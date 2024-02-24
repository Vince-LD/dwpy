from concurrent.futures import Future, ThreadPoolExecutor
from enum import Enum
from functools import reduce
from typing import Callable, Iterable, Optional, Self
from src.exceptions import PipelineError
from steps import BaseStep, PipelineContext
from steps import TestStep
import logging

logging.basicConfig(level=logging.DEBUG)

class NodeStatus(Enum):
    UNKNOWN = 0
    RUNNING = 1
    FINISHED = 2
    ERROR = 3


class PipeNode:
    def __init__(self, name="") -> None:
        self.name = name
        self.steps: list[BaseStep] = []
        self.depends_on: set[PipeNode] = set()
        self.pipes_into: set[PipeNode] = set()
        self._status = NodeStatus.UNKNOWN
        self._error: Optional[PipelineError] = None

    def run(self, ctx: PipelineContext):
        logging.info(f"{", ".join([n.name+ ":" + str(n.status) for n in self.depends_on])} => {self.name}")
        if not self._all_previous_finished():
            logging.info(
                f"{self.name} cannot run yet, "
                f"{", ".join([n.name+ ":" + str(n.status) for n in self.depends_on])}"
            )
            return
        try:
            for step in self.steps:
                step.run(ctx)
        except PipelineError as e:
            self._error = e
            self._status = NodeStatus.ERROR
        self._status = NodeStatus.FINISHED

    def add_next_step(self, step: BaseStep):
        self.steps.append(step)

    def add_next_node(self, node: Self):
        self.pipes_into.add(node)

    def add_dependency(self, node: Self):
        self.depends_on.add(node)

    def __str__(self) -> str:
        return f"{' -> '.join([step.__class__.__name__ for step in self.steps])}"

    def _all_previous_finished(self) -> bool:
        return reduce(
            lambda bool_, node: bool_ and node.status is NodeStatus.FINISHED,
            self.depends_on,
            True,
        )

    @property
    def status(self) -> NodeStatus:
        return self._status


class Pipeline:
    def __init__(self, start_node: Optional[PipeNode] = None) -> None:
        self.start_node = start_node or PipeNode("Pipeline start")
        self.nodes: list[PipeNode] = []

    def add_nodes_from(self, start_node: PipeNode, next_nodes: Iterable[PipeNode]):
        for node in next_nodes:
            start_node.add_next_node(node)
            node.add_dependency(start_node)
            self.nodes.append(node)

    def merge_nodes_into(self, start_nodes: Iterable[PipeNode], into_node: PipeNode):
        for node in start_nodes:
            node.add_next_node(into_node)
            into_node.add_dependency(node)
            self.nodes.append(into_node)

    def run(self, ctx: PipelineContext):
        self._run(ctx, (self.start_node,))

    def _run(self, ctx: PipelineContext, nodes: Iterable[PipeNode]):
        futures: list[Future[None]] = []
        next_nodes: set[PipeNode] = set()
        with ThreadPoolExecutor(max_workers=ctx.thread_count) as executor:
            for node in nodes:
                if node.status is not NodeStatus.UNKNOWN:
                    continue
                next_nodes.update(node.pipes_into)
                futures.append(executor.submit(node.run, ctx))
        results = [f.result() for f in futures]

        # for node in next_nodes:
        if next_nodes:
            self._run(ctx, next_nodes)

    def build(self):
        pass


class Step0(TestStep):
    NAME = "1st step"


class Step1(TestStep):
    NAME = "2nd step"


#    def run(self, ctx: PipelineContext):
#        self.exec_cmd(["dir"])


class Step2(TestStep):
    NAME = "3rd step"


#    def run(self, ctx: PipelineContext):
#        self.exec_cmd(["dir"])


class Step3(TestStep):
    NAME = "4th step"


#    def run(self, ctx: PipelineContext):
#        self.exec_cmd(["dir"])


class Step4(TestStep):
    NAME = "5th step"


#    def run(self, ctx: PipelineContext):
#        self.exec_cmd(["dir"])


class Step5(TestStep):
    NAME = "6th step"


#    def run(self, ctx: PipelineContext):
#        self.exec_cmd(["dir"])


class Step6(TestStep):
    NAME = "7th step"


#    def run(self, ctx: PipelineContext):
#        self.exec_cmd(["dir"])


class Step7(TestStep):
    NAME = "8th step"


#    def run(self, ctx: PipelineContext):
#        self.exec_cmd(["dir"])


class Step8(TestStep):
    NAME = "9th step"


#    def run(self, ctx: PipelineContext):
#        self.exec_cmd(["dir"])


class Step9(TestStep):
    NAME = "10th step"


class Step10(TestStep):
    NAME = "10th step"


if __name__ == "__main__":
    pipeline = Pipeline()
    
    node0 = pipeline.start_node

    node1 = PipeNode("Node 1")
    node1.add_next_step(Step0())
    # node1.add_next_step(Step1())
    # node1.add_next_step(Step2())

    node2 = PipeNode("Node 2")
    node2.add_next_step(Step3())

    node3 = PipeNode("Node 3")
    node3.add_next_step(Step4())
    
    node4 = PipeNode("Node 4")
    node4.add_next_step(Step5())
    # node4.add_next_step(Step6())
    # node4.add_next_step(Step7())
    # node4.add_next_step(Step8())

    node5 = PipeNode("Node 5")
    node5.add_next_step(Step9())
    
    
    # node3 = PipeNode("Split Branch")
    # node2.add_next_step(Step8())

    # node3 = PipeNode("Split Branch")
    pipeline.add_nodes_from(node0, (node1, node2))
    pipeline.add_nodes_from(node2, (node3, node4))
    pipeline.merge_nodes_into((node1, node3, node4), node5)

    pipeline.run(PipelineContext())
