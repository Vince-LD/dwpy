from concurrent.futures import ThreadPoolExecutor
import threading
from functools import reduce
from itertools import repeat
import time
from typing import Optional, Self
from tuyau.steps import BaseStep, StatusEnum, STATUS_PASSED, FinalStep, RootStep

from tuyau.context import BasePipelineContext, ContextT
import logging

# import asyncio
import graphviz

logging.basicConfig(level=logging.DEBUG)


class PipeNode:
    def __init__(self, name="Node") -> None:
        self.name = name
        self.steps: list[BaseStep] = []
        self.parent_nodes: set[PipeNode] = set()
        self.child_nodes: set[PipeNode] = set()
        self._status = StatusEnum.UNKNOWN
        self._error: Optional[BaseException] = None
        self._id = id(self)
        self.all_parents_finished = threading.Semaphore(len(self.parent_nodes))
        self._finished = threading.Event()

    def update_run_flag(self):
        self.all_parents_finished = threading.Semaphore(len(self.parent_nodes))

    def run(self, ctx: BasePipelineContext):
        if self.all_parents_finished.acquire(blocking=False):
            logging.info(f"{self.name} cannot run yet")
            # logging.info(
            #     f"{self.name} cannot run yet, "
            #     f"{", ".join([n.name+ ":" + str(n.status) for n in self.parent_nodes])}"
            # )
            self._status = StatusEnum.UNKNOWN
            return

        if self.finished:
            logging.info(f"{self.name} already finished")
            return
        self._finished.set()

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
        for node in self.child_nodes:
            node.all_parents_finished.acquire(blocking=False)

    def add_steps(self, *steps: BaseStep) -> Self:
        self.steps.extend(steps)
        return self

    def add_child_nodes(self, *nodes: Self) -> Self:
        self.child_nodes.update(nodes)
        for node in nodes:
            node.update_run_flag()
        return self

    def add_parent_nodes(self, *nodes: Self) -> Self:
        self.parent_nodes.update(nodes)
        self.update_run_flag()
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
        return self._status

    @property
    def error(self) -> Optional[BaseException]:
        return self._error

    @property
    def id(self) -> int:
        return self._id

    @property
    def finished(self) -> int:
        return self._finished.is_set()

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

        # self._async_loop = asyncio.get_event_loop()
        # self._coro_pool_count = asyncio.Semaphore(4)
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
                # node.add_child_nodes(self.final_node)
                # self.final_node.add_parent_nodes(node)
                self.add_child_to(node, self.final_node)

    def execute(self, ctx: BasePipelineContext):
        self.remaining_nodes = threading.Semaphore(len(self.nodes))
        self.running_nodes = 0
        thread_count = ctx.thread_count or self.default_thread_count
        with ThreadPoolExecutor(thread_count) as executor:
            executor.submit(self._parse_run, ctx, self.root_node, executor)
            while self._keep_running():
                logging.info("Still running")
                time.sleep(1)

            logging.info("PIPELINE FINISHED 1")

        logging.info("PIPELINE FINISHED 2")
        if self.runtime_error is not None:
            logging.exception(self.runtime_error)
        logging.info("PIPELINE FINISHED 3")

    def _parse_run(
        self, ctx: BasePipelineContext, node: PipeNode, executor: ThreadPoolExecutor
    ):
        if node.status is not StatusEnum.UNKNOWN:
            return

        if not bool(node.status & STATUS_PASSED):
            logging.info(f"Trying to run node {node.name}")
            node.run(ctx)
            logging.info(f"Node {node.name} finished")

        if bool(node.status & STATUS_PASSED):
            self.remaining_nodes.acquire(blocking=False)
            logging.info(
                node.name + f"  ---  {self.remaining_nodes=}  ---  {node.status}"
            )

        elif node.status is StatusEnum.ERROR:
            self.runtime_error = node.error
            return
        executor.map(self._parse_run, repeat(ctx), node.child_nodes, repeat(executor))

    def _keep_running(self):
        # print(self.last_node.status, self.runtime_error is None)
        # is_node_remaining = self.remaining_nodes.acquire(blocking=False)
        # self.remaining_nodes.release()
        return not self.final_node.finished and self.runtime_error is None

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
