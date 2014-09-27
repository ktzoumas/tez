package org.apache.flink.tez.plan;


import org.apache.flink.compiler.CompilerException;
import org.apache.flink.compiler.plan.DualInputPlanNode;
import org.apache.flink.compiler.plan.NAryUnionPlanNode;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plan.PlanNode;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.compiler.plan.SinkPlanNode;
import org.apache.flink.compiler.plan.SourcePlanNode;
import org.apache.flink.util.Visitor;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.Vertex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TezDAGGenerator implements Visitor<PlanNode> {

    private Map<PlanNode, Vertex> vertices; // a map from optimizer nodes to Tez vertices
    private List<Edge> edges;

    public DAG createDAG (OptimizedPlan program) {
        this.vertices = new HashMap<PlanNode, Vertex>();
        program.accept(this);


        DAG dag = DAG.create (program.getJobName());
        for (Vertex v : vertices.values()) {
            dag.addVertex(v);
        }
        for (Edge e: edges) {
            dag.addEdge(e);
        }

        this.vertices = null;
        this.edges = null;

        return dag;
    }

    @Override
    public boolean preVisit(PlanNode node) {
        if (this.vertices.containsKey(node)) {
            // return false to prevent further descend
            return false;
        }

        final Vertex vertex = null;

        try {
            if (node instanceof SinkPlanNode) {

            }
            else if (node instanceof SourcePlanNode) {

            }
            else if (node instanceof SingleInputPlanNode) {
                vertex = createSingleInputVertex((SingleInputPlanNode)node);
            }
            else if (node instanceof DualInputPlanNode) {

            }
            else if (node instanceof NAryUnionPlanNode) {

            }
            else {
                throw new CompilerException("Unrecognized node type: " + node.getClass().getName());
            }

        }
        catch (Exception e) {
            throw new CompilerException("Error translating node '" + node + "': " + e.getMessage(), e);
        }

        if (vertex != null) {
            this.vertices.put(node, vertex);
        }


        return true;
    }

    @Override
    public void postVisit(PlanNode visitable) {

    }


    private Vertex createSingleInputVertex(SingleInputPlanNode node) throws CompilerException {


        return null;
    }
}
