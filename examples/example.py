import os
from tuyau.pipeline import Pipeline, PipeNode
from example_utils import (
    ExampleContext,
    AdditionStep,
    MutliplyStep,
    LogStep,
    SkipStep,
)
from tuyau.steps import FuncStep
from tuyau.context import PipeVar
from multiprocessing import Pool


def square(a: float) -> float:
    return a**2


def do_something_in_process(a: float, b: float) -> float:
    pool = Pool(2)
    result = pool.map(square, (a, b))
    return sum(result)


def main():
    context = ExampleContext(input_x=PipeVar(1.5), input_y=PipeVar(8), thread_count=2)

    SquareStep = FuncStep.new(do_something_in_process)

    square_step = SquareStep(
        result_vars=context.result_func_step,
        a=context.result_step5.T,
        b=context.result_step6.T,
    )

    node1 = PipeNode("Process node 1").add_steps(
        AdditionStep(
            a_field=context.input_x,
            b_field=context.input_y,
            res_field=context.result_step1,
            name="Step 1.1",
        ),
        LogStep(context.result_step1, name="Step 1.2"),
        MutliplyStep(
            a_field=context.result_step1,
            b_field=context.result_step1,
            res_field=context.result_step1,
            name="Step 1.3",
            comment="Square previous result",
        ),
        LogStep(context.result_step1, name="Step 1.2"),
    )

    node2 = PipeNode("Process node 2").add_steps(SkipStep("Skip step 2"))

    node3 = PipeNode(name="Process node 3").add_steps(
        AdditionStep(
            a_field=context.input_x,
            b_field=context.input_y,
            res_field=context.result_step3,
            name="Step 3.1",
        )
    )

    node4 = PipeNode("Process node 4").add_steps(
        MutliplyStep(
            a_field=context.input_x,
            b_field=context.input_x,
            res_field=context.result_step4,
            name="Step 4.1",
        )
    )

    node5 = PipeNode("Process node 5").add_steps(
        AdditionStep(
            a_field=context.result_step3,
            b_field=context.result_step4,
            res_field=context.result_step5,
            name="Step 5.1",
        ),
        LogStep(context.result_step5, name="result_step5"),
    )

    node6 = PipeNode("Process node 6").add_steps(
        AdditionStep(
            a_field=context.result_step1,
            b_field=context.result_step5,
            res_field=context.result_step6,
            name="Step 6.1",
        ),
        LogStep(context.result_step6, name="result_step6"),
        square_step,
        LogStep(context.result_func_step, name="result_func_step"),
    )

    directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

    # First syntax - Code is commented because right now a pipeline can only be executed
    # once because the steps remain in their state. This could be fixed with a pipeline
    # factory for now. Pipeline.reset() will be implemented soon
    # pipeline1 = Pipeline(ExampleContext, "Example Pipeline 1")
    # (
    #     pipeline1.add_children_to(pipeline1.root_node, node1, node2)
    #     .add_children_to(node2, node3, node4)
    #     .add_parents_to(node5, node3, node4)
    #     .add_parents_to(node6, node5, node1)
    #     .terminate_pipeline()
    # )

    # pipeline1.execute(context)
    # graph = pipeline1.graph()
    # graph.render("example1", directory=directory, format="svg")
    # graph.render("example1", directory=directory, format="png")

    # Second syntax
    pipeline2 = Pipeline(ExampleContext, "Example Pipeline 2 ")
    pipeline2.build(
        (node1, node2),
        node2 >> (node3, node4),
        node5 << (node3, node4),
        node6 << (node1, node5),
    )

    pipeline2.execute(context)
    graph = pipeline2.graph()
    graph.render("example1", directory=directory, format="svg")
    graph.render("example2", directory=directory, format="png")


if __name__ == "__main__":
    main()
