from concurrent.futures import ThreadPoolExecutor
import threading
from functools import reduce
from itertools import repeat
import time
from typing import Callable, Iterable, Optional, Self, TypeAlias, TypeVar, Union
from tuyau.exceptions import ConditionError
from tuyau.steps import BaseStep, StatusEnum, FinalStep, RootStep

from tuyau.context import BasePipelineContext, ContextT
import logging

# import asyncio
import graphviz

logging.basicConfig(level=logging.DEBUG)

ConditionExpr = Callable[[], bool]
NodeOrNodeCompT = TypeVar("NodeOrNodeCompT", bound=Union["PipeNode", "NodeComp"])


class PipeNode:
    def __init__(self, name="Node") -> None:
        self.name = name
        self.steps: list[BaseStep] = []
        self.parent_nodes: set[PipeNode] = set()
        self.child_nodes: set[PipeNode] = set()
        self._status = StatusEnum.UNKNOWN
        self._error: Optional[BaseException] = None
        self._id = id(self)
        self.all_parents_executed: threading.Semaphore
        self.update_run_flag()
        self._executed = threading.Event()
        self.conditions: list[ConditionExpr] = []

    def update_run_flag(self):
        required = max(len(self.parent_nodes) - 1, 0)
        self.all_parents_executed = threading.Semaphore(required)

    def run(self, ctx: BasePipelineContext):
        if self.all_parents_executed.acquire(blocking=False):
            self._status = StatusEnum.UNKNOWN
            return

        if self.executed:
            return

        # If just one condition is false, the step should error out, skip step
        if not self.check_conditions:
            self._executed.set()
            return

        for step in self.steps:
            try:
                step.run(ctx)
                if not bool(step.status & StatusEnum.OK):
                    self._status = StatusEnum.ERROR
                    self._error = step.error
                    logging.exception(step.error)
                    return
            except Exception as e:
                step.errored(e)
                self._error = e
                self._status = StatusEnum.ERROR
                return

        self._executed.set()
        self._status = StatusEnum.COMPLETE

    def check_conditions(self) -> bool:
        exec_conditions = [condition() for condition in self.conditions]
        is_conditions_ok = all(exec_conditions)

        if not is_conditions_ok:
            self._status = StatusEnum.CONDITION_FAILED
            self._error = ConditionError(
                f"One or more condition are not met: {exec_conditions}"
            )

        return is_conditions_ok

    def add_steps(self, *steps: BaseStep) -> Self:
        self.steps.extend(steps)
        return self

    def add_child_nodes(self, *nodes: "ChildNode") -> Self:
        self.child_nodes.update(nodes)
        for node in nodes:
            node.update_run_flag()
        return self

    def add_parent_nodes(self, *nodes: "ParentNode") -> Self:
        self.parent_nodes.update(nodes)
        self.update_run_flag()
        return self

    def __str__(self) -> str:
        return f"{self.name}: {' -> '.join([step.name for step in self.steps])}"

    def _all_previous_complete(self) -> bool:
        return reduce(
            lambda bool_, node: bool_ and bool(node.status & StatusEnum.OK),
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

    @property
    def executed(self) -> int:
        return self._executed.is_set()

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

    # TODO refactor theses dunder methods and the corresponding ones in NodeComp
    # to avoid repetitions
    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and other.id == self.id

    def __rshift__(self, other: "NodeOrNodeCompT") -> "NodeOrNodeCompT":
        match other:
            case PipeNode():
                self.add_child_nodes(other)
                other.add_parent_nodes(self)
            case NodeComp():
                self.add_child_nodes(*other.nodes)
                for node in other.nodes:
                    node.add_parent_nodes(self)
        return other

    def __and__(self, other: Self) -> "NodeComp":
        return NodeComp((self, other))

    def __or__(self, other: ConditionExpr | tuple[ConditionExpr]) -> Self:
        match other:
            case [*_]:
                self.conditions.extend(other)
            case _:
                self.conditions.append(other)
        return self


ParentNode: TypeAlias = PipeNode
ChildNode: TypeAlias = PipeNode


class NodeComp:
    def __init__(
        self,
        nodes: Iterable[PipeNode],
    ) -> None:
        self.nodes: list[PipeNode] = list(nodes)

    # TODO refactor theses dunder methods and the corresponding ones in PipeNode
    # to avoid repetitions
    def __and__(self, other: PipeNode):
        match other:
            case PipeNode():
                self.nodes.append(other)
            case NodeComp():
                self.nodes.extend(other.nodes)
        return self

    def __rshift__(self, other: NodeOrNodeCompT) -> NodeOrNodeCompT:
        match other:
            case PipeNode():
                for step in self.nodes:
                    step.add_child_nodes(other)
                    other.add_parent_nodes(*self.nodes)
            case NodeComp():
                for step in self.nodes:
                    step.add_child_nodes(*other.nodes)
                    for node in other.nodes:
                        node.add_parent_nodes(*self.nodes)
        return other

    def __or__(self, other: ConditionExpr | tuple[ConditionExpr, ...]):
        match other:
            case [*_]:
                for step in self.nodes:
                    step.conditions.extend(other)
            case _:
                for step in self.nodes:
                    step.conditions.append(other)
        return self


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
        self.final_node = PipeNode(name="FINAL NODE")
        self.final_node.add_steps(FinalStep(context_class))
        self.add_nodes(self.root_node, self.final_node)

        self.default_thread_count = 4

        self.remaining_nodes = threading.Semaphore(len(self.nodes))
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

    def start_pipeline_with(self, *nodes: PipeNode) -> Self:
        self.add_children_to(self.root_node, *nodes)
        return self

    def terminate_pipeline(self):
        for node in self.nodes.difference({self.final_node}):
            if len(node.child_nodes) == 0:
                self.add_child_to(node, self.final_node)

    def execute(self, ctx: BasePipelineContext):
        self.remaining_nodes = threading.Semaphore(len(self.nodes))
        self.running_nodes = 0
        thread_count = ctx.thread_count or self.default_thread_count
        with ThreadPoolExecutor(thread_count) as executor:
            executor.submit(self._parse_run, ctx, self.root_node, executor)
            while self._keep_running():
                time.sleep(1)

        if self.runtime_error is not None:
            logging.exception(self.runtime_error)

    def _parse_run(
        self, ctx: BasePipelineContext, node: PipeNode, executor: ThreadPoolExecutor
    ):
        if node.status is not StatusEnum.UNKNOWN:
            return

        if not bool(node.status & StatusEnum.OK):
            node.run(ctx)

        if not node.executed:
            return

        if bool(node.status & StatusEnum.OK):
            self.remaining_nodes.acquire(blocking=False)

        elif node.status is StatusEnum.KO:
            self.runtime_error = node.error
            return
        executor.map(self._parse_run, repeat(ctx), node.child_nodes, repeat(executor))

    def _keep_running(self):
        return not self.final_node.executed and self.runtime_error is None

    def build(
        self, start_nodes: PipeNode | Iterable[PipeNode], *args: PipeNode | NodeComp
    ):
        self.start_pipeline_with(*start_nodes)
        self.register_nodes_from(self.root_node)
        self.terminate_pipeline()

    def register_nodes_from(self, start_node: PipeNode):
        self._map(start_node, lambda pl, node: pl.nodes.add(node), set())

    # TODO maybe add map method to the PipeNode class directly
    def map_pipeline(self, func: Callable[[Self, PipeNode], None]):
        self._map(self.root_node, func, set())

    def _map(
        self,
        node: PipeNode,
        func: Callable[[Self, PipeNode], None],
        visited: set[PipeNode],
    ):
        func(self, node)
        for child in node.child_nodes:
            if child in visited:
                continue
            visited.add(child)
            self._map(child, func, visited)

    def reset(self):
        raise NotImplementedError("The nodes must be reset (i.e. their locks)")

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
