from concurrent.futures import ThreadPoolExecutor

from functools import reduce
from itertools import repeat
import time
from typing import Optional, Self
from tuyau.base_step import FinalStep, RootStep, BaseStep, StatusEnum, STATUS_PASSED
from tuyau.context import BasePipelineContext, ContextT
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
        self._lock = Lock()

    def run(self, ctx: BasePipelineContext):
        # logging.info(self)
        # logging.info(
        #     f"{", ".join([n.name+ ":" + str(n.status) for n in self.parent_nodes])} => {self.name}"
        # )
        if not self._all_previous_complete():
            logging.info(
                f"{self.name} cannot run yet, "
                f"{", ".join([n.name+ ":" + str(n.status) for n in self.parent_nodes])}"
            )
            self._status = StatusEnum.UNKNOWN
            return
        
        for step in self.steps:
            try:
                step.run(ctx)
                if not bool(step.status & STATUS_PASSED):
                    self._status = StatusEnum.ERROR
                    self._error = step.error
                    logging.error(step.error)
                    return
            except Exception as e:
                step.errored(e)
                self._error = e
                self._status = StatusEnum.ERROR
                return
        self._status = StatusEnum.COMPLETE

    def add_steps(self, *steps: BaseStep) -> Self:
        self.steps.extend(steps)
        return self

    def add_child_nodes(self, *nodes: Self) -> Self:
        self.child_nodes.update(nodes)
        return self

    def add_parent_nodes(self, *nodes: Self) -> Self:
        self.parent_nodes.update(nodes)
        return self

    def __str__(self) -> str:
        return f"{self.name}: {' -> '.join([step.__class__.__name__ for step in self.steps])}"

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
        with self._lock:
            return self._status

    @property
    def error(self) -> Optional[BaseException]:
        return self._error

    @property
    def id(self) -> int:
        return self._id

    def view(self, graph: graphviz.Digraph) -> graphviz.Digraph:
        sg = graphviz.Digraph(f"cluster_{self._id}")
        sg.attr(label=self.name, color="grey")

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

    def __hash__(self) -> int:
        return self.id

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, self.__class__) and __value.id == self.id


class Pipeline:
    def __init__(
        self,
        context_class: type[ContextT],
        name: str = "Pipeline",
    ) -> None:
        self.name = name

        self.nodes: set[PipeNode] = set()

        self.root_node = PipeNode(self.name)
        self.root_node.add_steps(RootStep(context_class))
        self.final_node = PipeNode("")
        self.final_node.add_steps(FinalStep(context_class))
        self.add_nodes(self.root_node, self.final_node)

        self.last_node = PipeNode("Pipeline end")
        self._lock = Lock()
        self._remaining_nodes: int = 0
        self._running_nodes: int = 0
        self.runtime_error: Optional[BaseException] = None

    def add_node(self, node: PipeNode):
        self.nodes.add(node)

    def add_nodes(self, *nodes: PipeNode) -> Self:
        self.nodes.update(nodes)
        return self

    def add_children_to(
        self,
        parent_node: PipeNode,
        *child_nodes: PipeNode,
    ) -> Self:
        parent_node.add_child_nodes(*child_nodes)

        for node in child_nodes:
            node.add_parent_nodes(parent_node)

        self.nodes.update(child_nodes)
        self.nodes.add(parent_node)
        return self

    def add_child_to(self, parent_node: PipeNode, child_node: PipeNode) -> Self:
        parent_node.add_child_nodes(child_node)
        child_node.add_parent_nodes(parent_node)

        self.nodes.add(child_node)
        self.nodes.add(parent_node)
        return self

    def add_parents_to(
        self,
        child_node: PipeNode,
        *parent_nodes: PipeNode,
    ) -> Self:
        child_node.add_parent_nodes(*parent_nodes)

        for node in parent_nodes:
            node.add_child_nodes(child_node)

        self.nodes.update(parent_nodes)
        self.nodes.add(child_node)
        return self

    def add_parent_to(
        self,
        child_node: PipeNode,
        parent_node: PipeNode,
    ) -> Self:
        parent_node.add_child_nodes(child_node)
        child_node.add_parent_nodes(parent_node)

        self.nodes.add(child_node)
        self.nodes.add(parent_node)
        return self

    def connect_final_node(self) -> Self:
        for node in self.nodes.difference({self.final_node}):
            if len(node.child_nodes) == 0:
                node.add_child_nodes(self.final_node)
                self.final_node.add_parent_nodes(node)

        return self

    def execute(self, ctx: BasePipelineContext):
        self.remaining_nodes = len(self.nodes)
        self.running_nodes = 0
        print(self.remaining_nodes)
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
        
        if not bool(node.status & STATUS_PASSED):
            node.run(ctx)

        if bool(node.status & STATUS_PASSED):
            self.remaining_nodes -= 1
            logging.info(node.name + f"  ---  {self.remaining_nodes=}  ---  {node.status}")
        
        elif node.status is StatusEnum.ERROR:
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

    def _keep_running(self):
        # logging.info(f"Remaining nodes: {self.remaining_nodes}")
        return (
            self.remaining_nodes > 0 and self.runtime_error is None
            # and self.running_nodes > 0
        )

    def build(self):
        pass

    def graph(self, preview=True) -> graphviz.Digraph:
        pipeline_name = f"{self.name}_preview" if preview else self.name
        graph = graphviz.Digraph(pipeline_name, strict=True)
        # graph.attr(compound="true", splines="curved")
        self._graph(self.root_node, graph)
        return graph

    def _graph(self, node: PipeNode, graph: graphviz.Digraph):
        node.view(graph)
        for child_node in node.child_nodes:
            self._graph(child_node, graph)
