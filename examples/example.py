import os
from tuyaux.pipeline import Pipeline, PipeNode
from example_utils import (
    ExampleContext,
    AdditionStep,
    MutliplyStep,
    LogStep,
    SkipStep,
)
from tuyaux.steps import FuncStep
from tuyaux.context import PipeVar
from multiprocessing import Pool

from tuyaux.steps.base_step import StatusEnum


def square(a: float) -> float:
    return a**2


def do_something_in_process(a: float, b: float) -> float:
    pool = Pool(2)
    result = pool.map(square, (a, b))
    return sum(result)


def main():
    context = ExampleContext(input_x=PipeVar(1.5), input_y=PipeVar(8), thread_count=4)

    SquareStep = FuncStep.new(do_something_in_process)

    square_step = SquareStep(
        result_vars=context.result_func_step.as_output(),
        a=context.result_step5.as_input().T,
        b=context.result_step6.as_input().T,
    )

    node1 = PipeNode("Process node 1").add_steps(
        AdditionStep(
            a_field=context.input_x.as_input(),
            b_field=context.input_y.as_input(),
            res_field=context.result_step1.as_output(),
            name="Step 1.1",
        ),
        LogStep(context.result_step1, name="Step 1.2"),
        MutliplyStep(
            a_field=context.result_step1.as_input(),
            b_field=context.result_step1.as_input(),
            res_field=context.result_step1.as_output(),
            name="Step 1.3",
            comment="Square previous result",
        ),
        LogStep(context.result_step1, name="Step 1.2"),
    )

    node2 = PipeNode("Process node 2").add_steps(SkipStep("Skip step 2"))

    node3 = PipeNode(name="Process node 3").add_steps(
        AdditionStep(
            a_field=context.input_x.as_input(),
            b_field=context.input_y.as_input(),
            res_field=context.result_step3.as_output(),
            name="Step 3.1",
        )
    )

    node4 = PipeNode("Process node 4").add_steps(
        MutliplyStep(
            a_field=context.input_x.as_input(),
            b_field=context.input_x.as_input(),
            res_field=context.result_step4.as_output(),
            name="Step 4.1",
        )
    )

    node5 = PipeNode("Process node 5").add_steps(
        AdditionStep(
            a_field=context.result_step3.as_input(),
            b_field=context.result_step4.as_input(),
            res_field=context.result_step5.as_output(),
            name="Step 5.1",
        ),
        LogStep(context.result_step5, name="result_step5"),
    )

    node6 = PipeNode("Process node 6").add_steps(
        AdditionStep(
            a_field=context.result_step1.as_input(),
            b_field=context.result_step5.as_input(),
            res_field=context.result_step6.as_output(),
            name="Step 6.1",
        ),
        LogStep(context.result_step6, name="result_step6"),
        square_step,
        LogStep(context.result_func_step, name="result_func_step"),
    )

    directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

    pipeline = Pipeline(ExampleContext, "Example Pipeline")

    # The build method construst some data structures in the pipeline, checks for cycles
    # and can check for input/output usage errors (recommended).
    # It also automatically connects all the orphan nodes to the root node and all
    # the childless nodes to the final node.
    # You MUST use this method to ensure your have a functional pipeline
    pipeline.build(
        (
            # pipeline.root_node >> (node1 & node2),
            node2
            >> (node3 & node4).if_(
                lambda: node2.status is StatusEnum.COMPLETE,
                lambda: node2.status is not StatusEnum.ERROR,
            )
        ),
        (node3 & node4) >> node5.if_(lambda: node3.status is StatusEnum.COMPLETE),
        (node1 & node5) >> node6,
    )

    for node in pipeline.nodes:
        print(node.name, "->", [n.name for n in node.child_nodes])

    graph = pipeline.graph(preview=True)
    graph.render("example_preview", directory=directory, format="svg")
    graph.render("example_preview", directory=directory, format="png")

    pipeline.execute(context)
    graph = pipeline.graph()
    graph.render("example", directory=directory, format="svg")
    graph.render("example", directory=directory, format="png")


if __name__ == "__main__":
    main()
