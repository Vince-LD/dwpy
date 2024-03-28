from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import pprint
import threading
from functools import reduce
from itertools import repeat
import time
from typing import Callable, Iterable, Optional, Self, TypeAlias, TypeVar, Union
from tuyaux.exceptions import ConditionError, CycleError, InputOutputConflictError
from tuyaux.steps import BaseStep, StatusEnum, FinalStep, RootStep

from tuyaux.context import BasePipelineContext, ContextT, PipeVar
import logging

from uuid import UUID, uuid4

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
        self._id = uuid4()
        self._int_id = self._id.int
        self.all_parents_executed: threading.Semaphore
        self.update_run_flag()
        self._executed = threading.Event()
        self.conditions: list[ConditionExpr] = []
        self.inputs: set[PipeVar] = set()
        self.outputs: set[PipeVar] = set()
        self.branch: set[PipeNode] = set()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} : {self.name}"

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
        if not self.check_conditions():
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
        self.inputs.update(inp.as_pipevar() for step in steps for inp in step.inputs())
        self.outputs.update(
            inp.as_pipevar() for step in steps for inp in step.outputs()
        )
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
    def id(self) -> UUID:
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
        return self._int_id

    # TODO refactor theses dunder methods and the corresponding ones in NodeComp
    # to avoid repetitions
    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and other.id == self.id

    def __rshift__(self, other: Union["PipeNode", "NodeComp"]) -> "NodeComp":
        match other:
            case PipeNode() as pipe_node:
                self.add_child_nodes(other)
                pipe_node.add_parent_nodes(self)
                other = NodeComp((self, other))
            case NodeComp() as node_comp:
                self.add_child_nodes(*node_comp.nodes)
                for node in node_comp.nodes:
                    node.add_parent_nodes(self)
                node_comp.nodes.append(self)

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

    def __rshift__(self, other: Union["NodeComp", PipeNode]) -> "NodeComp":
        match other:
            case PipeNode() as pipe_node:
                for step in self.nodes:
                    step.add_child_nodes(pipe_node)
                    other.add_parent_nodes(*self.nodes)
                self.nodes.append(pipe_node)
            case NodeComp():
                for step in self.nodes:
                    step.add_child_nodes(*other.nodes)
                    for node in other.nodes:
                        node.add_parent_nodes(*self.nodes)
                self.nodes.extend(other.nodes)
        return self

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

        self.pipeline_ends = {self.root_node, self.final_node}

        self.default_thread_count = 4

        self.remaining_nodes = threading.Semaphore(len(self.nodes))
        self._running_nodes: int = 0
        self.runtime_error: Optional[BaseException] = None

        self.parallel_nodes: dict[PipeNode, set[PipeNode]] = defaultdict(set)

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

    def start_nodes(self, *nodes: PipeNode) -> Self:
        self.add_children_to(self.root_node, *nodes)
        return self

    def _terminate_pipeline(self):
        for node in self.nodes.difference(self.pipeline_ends):
            if len(node.child_nodes) == 0:
                self.add_child_to(node, self.final_node)
        self._compute_branches()

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

    def _connect_starting_nodes(self):
        for node in self.nodes.difference(self.pipeline_ends):
            if len(node.parent_nodes) == 0:
                self.add_child_to(self.root_node, node)
                print(f"{self.root_node} >> {node}")

    def build(
        self,
        *node_comps: NodeComp,
        check_io: bool = True,
    ):
        for node_comp in node_comps:
            self.add_nodes(*node_comp.nodes)

        self._connect_starting_nodes()
        self._terminate_pipeline()
        self.register_nodes_from(self.root_node)
        if check_io:
            self.validate_io()

    def register_nodes_from(self, start_node: PipeNode):
        self._map_once(start_node, lambda pl, node: pl.nodes.add(node))

    def _compute_branches(self):
        def add_branch(_: Pipeline, node: PipeNode):
            for parent in node.parent_nodes:
                node.branch.add(parent)
                node.branch.update(parent.branch)
            if node in node.branch:
                raise CycleError(
                    "The following nodes are part of cycles which are "
                    f"node allowed: {node.name}"
                )

        self.map_pipeline(add_branch)

    # TODO maybe add map method to the PipeNode class directly
    def map_pipeline_once(self, func: Callable[[Self, PipeNode], None]):
        self._map_once(self.root_node, func)

    def map_pipeline(self, func: Callable[[Self, PipeNode], None]):
        self._map(self.root_node, func)

    def _map_once(
        self,
        node: PipeNode,
        func: Callable[[Self, PipeNode], None],
    ):
        visited: set[PipeNode] = set()
        queue = deque([node])
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            func(self, node)
            queue.extend(node.child_nodes)

    def _map(self, node: PipeNode, func: Callable[[Self, PipeNode], None]):
        queue = deque([node])
        count: dict[PipeNode, int] = {
            n: max(len(n.parent_nodes), 1) for n in self.nodes
        }
        while queue:
            node = queue.popleft()
            func(self, node)
            if count[node] >= 1:
                count[node] -= 1
                queue.extend(node.child_nodes)

    def _compute_parallel_nodes(
        self,
    ):
        parallel_nodes = self.parallel_nodes
        i = 0
        graph = self.nodes
        remaning_nodes_to_visit = self.nodes.copy()
        for node_i in graph:
            remaning_nodes_to_visit.remove(node_i)
            if (
                len(node_i.parent_nodes) == 1
                and len((parent := next(iter(node_i.parent_nodes))).child_nodes) == 1
            ):
                parallel_nodes[node_i].add(parent)
                continue

            for node_j in remaning_nodes_to_visit:
                if node_i in node_j.branch or node_j in node_i.branch:
                    continue
                i += 1
                parallel_nodes[node_i].add(node_j)
                parallel_nodes[node_j].add(node_i)

    def validate_io(self):
        self._compute_parallel_nodes()
        forbidden_inputs: dict[PipeVar, set[PipeNode]] = defaultdict(set)
        forbidden_outputs: dict[PipeVar, set[PipeNode]] = defaultdict(set)
        for ref_node, parll_nodes in self.parallel_nodes.items():
            for parll_node in parll_nodes:
                if parll_node is ref_node:
                    continue
                s_in = ref_node.inputs.intersection(parll_node.outputs)
                for var in s_in:
                    forbidden_inputs[var].update((parll_node, ref_node))
                s_out = ref_node.inputs.intersection(parll_node.outputs)
                for var in s_out:
                    forbidden_outputs[var].update((parll_node, ref_node))

        message_lines: list[str] = []
        if forbidden_inputs:
            message_lines.extend(
                (
                    "",
                    "/!\\ Forbiden inputs",
                    "Some intputs were used in a node while also used as outputs in "
                    "parallel nodes:",
                    pprint.pformat(
                        {
                            var.get_name(): tuple(node.name for node in nodes)
                            for var, nodes in forbidden_inputs.items()
                        }
                    ),
                )
            )
        if forbidden_outputs:
            message_lines.extend(
                (
                    "",
                    "/!\\ Forbiden outputs",
                    "Some outputs were used in a node while also used as outputs in "
                    "parallel nodes:",
                    pprint.pformat(
                        {
                            var.get_name(): tuple(node.name for node in nodes)
                            for var, nodes in forbidden_outputs.items()
                        }
                    ),
                )
            )
        if message_lines:
            raise InputOutputConflictError("\n".join(message_lines))

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
