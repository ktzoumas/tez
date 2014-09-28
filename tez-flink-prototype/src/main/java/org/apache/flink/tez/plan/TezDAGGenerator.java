package org.apache.flink.tez.plan;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.compiler.dag.DataSinkNode;
import org.apache.flink.compiler.dag.DataSourceNode;
import org.apache.flink.compiler.plan.Channel;
import org.apache.flink.compiler.plan.DualInputPlanNode;
import org.apache.flink.compiler.plan.NAryUnionPlanNode;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plan.PlanNode;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.compiler.plan.SinkPlanNode;
import org.apache.flink.compiler.plan.SourcePlanNode;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OutputFormatVertex;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.tez.runtime.ChannelSelector;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.util.Visitor;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TezDAGGenerator implements Visitor<PlanNode> {

    private Map<PlanNode, FlinkVertex> vertices; // a map from optimizer nodes to Tez vertices
    private List<FlinkEdge> edges;
    private final int defaultMaxFan;
    private final TezConfiguration tezConf;

    private final float defaultSortSpillingThreshold;

    public TezDAGGenerator (TezConfiguration tezConf, Configuration config) {
        this.defaultMaxFan = config.getInteger(ConfigConstants.DEFAULT_SPILLING_MAX_FAN_KEY,
                ConfigConstants.DEFAULT_SPILLING_MAX_FAN);
        this.defaultSortSpillingThreshold = config.getFloat(ConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD_KEY,
                ConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD);
        this.tezConf = tezConf;
    }

    public DAG createDAG (OptimizedPlan program) {
        this.vertices = new HashMap<PlanNode, FlinkVertex>();
        this.edges = new ArrayList<FlinkEdge>();
        program.accept(this);

        DAG dag = DAG.create (program.getJobName());
        for (FlinkVertex v : vertices.values()) {
            dag.addVertex(v.createVertex(new TezConfiguration(tezConf)));
        }
        for (FlinkEdge e: edges) {
            dag.addEdge(e.createEdge(new TezConfiguration(tezConf)));
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

        FlinkVertex vertex = null;

        try {
            if (node instanceof SourcePlanNode) {
                vertex = createDataSourceVertex ((SourcePlanNode) node);
            }
            else if (node instanceof SinkPlanNode) {
                vertex = createDataSinkVertex ((SinkPlanNode) node);
            }
            else if ((node instanceof SingleInputPlanNode)) {
                vertex = createSingleInputVertex((SingleInputPlanNode) node);
            }
            else if (node instanceof DualInputPlanNode) {
                vertex = createDualInputVertex((DualInputPlanNode) node);
            }
            else if (node instanceof NAryUnionPlanNode) {
                throw new CompilerException("Union is not supported yet");
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
    public void postVisit(PlanNode node) {
        try {
            if (node instanceof SourcePlanNode || node instanceof NAryUnionPlanNode) {
                return;
            }

            final FlinkVertex targetVertex = vertices.get(node);

            final Iterator<Channel> inConns = node.getInputs().iterator();

            if (!inConns.hasNext()) {
                throw new CompilerException("Bug: Found a non-source task with no input.");
            }

            while (inConns.hasNext()) {
                Channel input = inConns.next();
                FlinkEdge edge;
                FlinkVertex source = vertices.get(input.getSource());
                FlinkVertex target = vertices.get(input.getTarget());
                ShipStrategyType shipStrategy = input.getShipStrategy();
                TypeSerializer<?> serializer = input.getSerializer().getSerializer();
                if ((shipStrategy == ShipStrategyType.FORWARD) || (shipStrategy == ShipStrategyType.NONE)) {
                    edge = new FlinkForwardEdge(source, target, serializer);
                }
                else if (shipStrategy == ShipStrategyType.BROADCAST) {
                    edge = new FlinkBroadcastEdge(source, target, serializer);
                }
                else if (shipStrategy == ShipStrategyType.PARTITION_HASH) {
                    edge = new FlinkPartitionEdge(source, target, serializer);
                }
                else {
                    throw new CompilerException("Ship strategy between nodes " + source.getVertex().getName() + " and " + target.getVertex().getName() + " currently not supported");
                }
                source.getConfig().setNumberSubtasksInOutput(target.getParallelism());

                edges.add(edge);

            }
        }
        catch (Exception e) {
            throw new CompilerException(
                    "An error occurred while translating the optimized plan to a Tez DAG: " + e.getMessage(), e);
        }
    }




    private FlinkVertex createSingleInputVertex(SingleInputPlanNode node) throws CompilerException, IOException {

        final String taskName = node.getNodeName();
        final DriverStrategy ds = node.getDriverStrategy();
        final int dop = node.getDegreeOfParallelism();

        final TezTaskConfig config= new TezTaskConfig(new Configuration());

        config.setDriver(ds.getDriverClass());
        config.setDriverStrategy(ds);
        config.setStubWrapper(node.getPactContract().getUserCodeWrapper());
        config.setStubParameters(node.getPactContract().getParameters());

        for(int i=0;i<ds.getNumRequiredComparators();i++) {
            config.setDriverComparator(node.getComparator(i), i);
        }
        assignDriverResources(node, config);

        return new FlinkVertex(taskName, dop, config);
    }

    private FlinkVertex createDualInputVertex(DualInputPlanNode node) throws CompilerException, IOException {
        final String taskName = node.getNodeName();
        final DriverStrategy ds = node.getDriverStrategy();
        final int dop = node.getDegreeOfParallelism();

        final TezTaskConfig config= new TezTaskConfig(new Configuration());

        config.setDriver(ds.getDriverClass());
        config.setDriverStrategy(ds);
        config.setStubWrapper(node.getPactContract().getUserCodeWrapper());
        config.setStubParameters(node.getPactContract().getParameters());

        if (node.getComparator1() != null) {
            config.setDriverComparator(node.getComparator1(), 0);
        }
        if (node.getComparator2() != null) {
            config.setDriverComparator(node.getComparator2(), 1);
        }
        if (node.getPairComparator() != null) {
            config.setDriverPairComparator(node.getPairComparator());
        }

        assignDriverResources(node, config);

        return new FlinkVertex(taskName, dop, config);
    }

    private FlinkVertex createDataSinkVertex(SinkPlanNode node) throws CompilerException, IOException {
        final String taskName = node.getNodeName();
        final int dop = node.getDegreeOfParallelism();

        final TezTaskConfig config = new TezTaskConfig(new Configuration());

        // set user code
        config.setStubWrapper(node.getPactContract().getUserCodeWrapper());
        config.setStubParameters(node.getPactContract().getParameters());

        return new FlinkVertex(taskName, dop, config);
    }

    private FlinkVertex createDataSourceVertex(SourcePlanNode node) throws CompilerException, IOException {
        final String taskName = node.getNodeName();
        final int dop = node.getDegreeOfParallelism();

        final TezTaskConfig config= new TezTaskConfig(new Configuration());

        config.setStubWrapper(node.getPactContract().getUserCodeWrapper());
        config.setStubParameters(node.getPactContract().getParameters());

        config.setOutputSerializer(node.getSerializer());

        return new FlinkVertex(taskName, dop, config);
    }


    private void assignDriverResources(PlanNode node, TaskConfig config) {
        final double relativeMem = node.getRelativeMemoryPerSubTask();
        if (relativeMem > 0) {
            config.setRelativeMemoryDriver(relativeMem);
            config.setFilehandlesDriver(this.defaultMaxFan);
            config.setSpillingThresholdDriver(this.defaultSortSpillingThreshold);
        }
    }

    private void assignLocalStrategyResources(Channel c, TaskConfig config, int inputNum) {
        if (c.getRelativeMemoryLocalStrategy() > 0) {
            config.setRelativeMemoryInput(inputNum, c.getRelativeMemoryLocalStrategy());
            config.setFilehandlesInput(inputNum, this.defaultMaxFan);
            config.setSpillingThresholdInput(inputNum, this.defaultSortSpillingThreshold);
        }
    }
}
