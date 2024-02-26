from concurrent.futures import ThreadPoolExecutor

from functools import reduce
from itertools import repeat
import time
from typing import Iterable, Optional, Self
from pipelyne.base_step import FinalStep, RootStep, BaseStep, StatusEnum, STATUS_PASSED
from pipelyne.context import BasePipelineContext, ContextT
import logging
from threading import Lock
import graphviz

logging.basicConfig(level=logging.DEBUG)


class PipeNode:
    def __init__(self, name="") -> None:
        self.name = name
        self.steps: list[BaseStep] = []
        self.parent_nodes: set[PipeNode] = set()
        self.child_nodes: set[PipeNode] = set()
        self._status = StatusEnum.UNKNOWN
        self._error: Optional[BaseException] = None
        self._id = id(self)

    def run(self, ctx: BasePipelineContext):
        logging.info(
            f"{", ".join([n.name+ ":" + str(n.status) for n in self.parent_nodes])} => {self.name}"
        )
        if not self._all_previous_complete():
            logging.info(
                f"{self.name} cannot run yet, "
                f"{", ".join([n.name+ ":" + str(n.status) for n in self.parent_nodes])}"
            )
            return

        step = self.first_step
        try:
            for step in self.steps:
                step.run(ctx)
        except BaseException as e:
            self._error = e
            self._status = StatusEnum.ERROR
            step.error()
            return
        self._status = StatusEnum.COMPLETE

    def add_step(self, step: BaseStep):
        self.steps.append(step)

    def add_child_node(self, node: Self):
        self.child_nodes.add(node)

    def add_parent_node(self, node: Self):
        self.parent_nodes.add(node)

    def __str__(self) -> str:
        return f"{' -> '.join([step.__class__.__name__ for step in self.steps])}"

    def _all_previous_complete(self) -> bool:
        return reduce(
            lambda bool_, node: bool_ and bool(node.status & STATUS_PASSED),
            self.parent_nodes,
            True,
        )

    @property
    def first_step(self):
        return self.steps[0]

    @property
    def last_step(self):
        return self.steps[-1]

    @property
    def status(self) -> StatusEnum:
        return self._status

    @property
    def error(self) -> Optional[BaseException]:
        return self._error

    @property
    def id(self) -> int:
        return self._id

    def view(self, graph: graphviz.Digraph) -> graphviz.Digraph:
        sg = graphviz.Digraph(f"cluster_{self._id}")
        sg.attr(label=self.name)

        if self.steps:
            first_step = self.first_step
            sg.node(
                first_step.str_id,
                **first_step.style(),
                comment=first_step.comment,
                label=first_step.label(),
            )
            for prev_id, step in enumerate(self.steps[1:]):
                sg.node(
                    str(step.id),
                    **step.style(),
                    comment=step.comment,
                    label=step.label(),
                )
                sg.edge(self.steps[prev_id].str_id, step.str_id)

        graph.subgraph(sg)
        if self.parent_nodes:
            for p in self.parent_nodes:
                graph.edge(
                    f"{p.last_step.str_id}",
                    f"{self.first_step.str_id}",
                    ltail=f"cluster_{p.id}",
                    lhead=f"cluster_{self._id}",
                )
        return graph


class Pipelyne:
    def __init__(
        self,
        context_class: type[ContextT],
        name: str = "Pipeline",

    ) -> None:
        self.name = name

    
        self.root_node = PipeNode(self.name)
        self.root_node.add_step(RootStep(context_class))

        self.final_node = PipeNode("")
        self.final_node.add_step(FinalStep(context_class))

        self.last_node = PipeNode("Pipeline end")
        self.nodes: list[PipeNode] = []
        self._lock = Lock()
        self._remaining_nodes: int = 0
        self._running_nodes: int = 0
        self.runtime_error: Optional[BaseException] = None

    def add_children_to(self, parent_node: PipeNode, child_nodes: Iterable[PipeNode]):
        for node in child_nodes:
            parent_node.add_child_node(node)
            node.add_parent_node(parent_node)
            self.nodes.append(node)

    def add_parents_to(
        self,
        child_node: PipeNode,
        parent_nodes: Iterable[PipeNode],
    ):
        for node in parent_nodes:
            node.add_child_node(child_node)
            child_node.add_parent_node(node)
            self.nodes.append(child_node)

    def execute(self, ctx: BasePipelineContext):
        self.remaining_nodes = len(self.nodes)
        self.running_nodes = 0
        with ThreadPoolExecutor(max_workers=ctx.thread_count) as executor:
            self._parse_run(ctx, self.root_node, executor)
            while self._keep_running():
                print("keep_running")
                time.sleep(1)
        if self.runtime_error is not None:
            logging.exception(self.runtime_error)
        
    def _parse_run(
        self, ctx: BasePipelineContext, node: PipeNode, executor: ThreadPoolExecutor
    ):
        if node.status is not StatusEnum.UNKNOWN:
            return
        node.run(ctx)
        self.remaining_nodes -= 1

        if node.status is StatusEnum.ERROR:
            self.runtime_error = node.error
            return

        executor.map(
            self._parse_run,
            repeat(ctx),
            node.child_nodes,
            repeat(executor),
        )

    @property
    def remaining_nodes(self) -> int:
        with self._lock:
            return self._remaining_nodes

    @remaining_nodes.setter
    def remaining_nodes(self, value: int):
        with self._lock:
            self._remaining_nodes = value

    # @property
    # def running_nodes(self) -> int:
    #     with self._lock:
    #         return self._running_nodes

    # @running_nodes.setter
    # def running_nodes(self, value: int):
    #     with self._lock:
    #         self._running_nodes = value

    def _keep_running(self):
        print(self.remaining_nodes, self.runtime_error)
        return (
            self.remaining_nodes > 0 and self.runtime_error is None
            # and self.running_nodes > 0
        )

    def build(self):
        pass

    def graph(self, preview=True) -> graphviz.Digraph:
        pipeline_name = f"{self.name}_preview" if preview else self.name
        graph = graphviz.Digraph(pipeline_name, strict=True)
        graph.attr(compound="true", splines="curved")
        self._graph(self.root_node, graph)
        return graph

    def _graph(self, node: PipeNode, graph: graphviz.Digraph):
        node.view(graph)
        for child_node in node.child_nodes:
            self._graph(child_node, graph)

    def connect_final_node(self):
        for node in self.nodes:
            if len(node.child_nodes) == 0:
                node.add_child_node(self.final_node)
                self.final_node.add_parent_node(node)
