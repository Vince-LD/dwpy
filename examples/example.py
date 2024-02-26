from pipelyne.pipelyne import Pipelyne, PipeNode
from example_utils import ExampleContext, AddStep, MutliplyStep, LogStep, SkipStep


def main():
    pipeline = Pipelyne("Example Pipeline")


    node1 = PipeNode("Process node 1")
    node1.add_step(
        AddStep(
            x_field="input_x",
            y_field="input_y",
            res_field="result_step1",
            name="Step 1.1",
        )
    )
    node1.add_step(
        LogStep(
            "result_step1",
            name="Step 1.2"
        )
    )
    node1.add_step(
        MutliplyStep(
            x_field="result_step1",
            y_field="result_step1",
            res_field="result_step1",
            name="Step 1.3",
        )
    )

    node2 = PipeNode("Process node 2")
    node2.add_step(
        SkipStep("Skip step 2")
    )

    node3 = PipeNode(name="Process node 3")
    node3.add_step(
        AddStep(
            x_field="input_x",
            y_field="input_y",
            res_field="result_step3",
            name="Step 3.1"
        )
    )
    node4 = PipeNode("Process node 4")
    node4.add_step(
        MutliplyStep(
            x_field="input_x",
            y_field="input_y",
            res_field="result_step4",
            name="Step 4.1"
        )
    )

    node5 = PipeNode("Process node ")
    node5.add_step(
        AddStep(
            x_field="result_step3",
            y_field="result_step4",
            res_field="result_step5",
            name="Step 5.1"
        )
    )

    node6 = PipeNode("Process node ")
    node6.add_step(
        MutliplyStep(
            x_field="result_step3",
            y_field="result_step5",
            res_field="result_step6",
            name="Step 6.1"
        )
    )
    node6.add_step(
        LogStep(
            "result_step6",
            name="Final logging!"
        )
    )

    pipeline.add_children_to(pipeline.root_node, (node1, node2))
    pipeline.add_children_to(node2, (node3, node4))
    pipeline.add_parents_to(node5, (node3, node4))
    pipeline.add_parents_to(node6, (node5, node1))

    context = ExampleContext()
    pipeline.execute(context)
    graph = pipeline.graph()
    graph.view()


if __name__ == "__main__":
    main()
