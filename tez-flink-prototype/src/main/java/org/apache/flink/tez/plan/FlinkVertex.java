package org.apache.flink.tez.plan;


import org.apache.flink.compiler.CompilerException;
import org.apache.flink.tez.runtime.FlinkProcessor;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public abstract class FlinkVertex {

    protected Vertex cached;
    private String taskName;
    private int parallelism;
    protected TezTaskConfig taskConfig;
    protected String uniqueName; //Unique name in DAG
    private Map<FlinkVertex,ArrayList<Integer>> inputPositions;

    public TezTaskConfig getConfig() {
        return taskConfig;
    }

    public FlinkVertex(String taskName, int parallelism, TezTaskConfig taskConfig) {
        this.cached = null;
        this.taskName = taskName;
        this.parallelism = parallelism;
        this.taskConfig = taskConfig;
        this.uniqueName = taskName + UUID.randomUUID().toString();
        this.inputPositions = new HashMap<FlinkVertex, ArrayList<Integer>>();
    }

    public int getParallelism () {
        return parallelism;
    }

    public abstract Vertex createVertex (TezConfiguration conf);

    public Vertex getVertex () {
        return cached;
    }

    protected String getUniqueName () {
        return uniqueName;
    }

    public void addInput (FlinkVertex vertex, int position) {
        if (inputPositions.containsKey(vertex)) {
            inputPositions.get(vertex).add(position);
        }
        else {
            ArrayList<Integer> lst = new ArrayList<Integer>();
            lst.add(position);
            inputPositions.put(vertex,lst);
        }
        //inputPositions.put(vertex, position);
    }

    // Must be called before taskConfig is written to Tez configuration
    protected void writeInputPositionsToConfig () {
        HashMap<String,ArrayList<Integer>> toWrite = new HashMap<String, ArrayList<Integer>>();
        for (FlinkVertex v: inputPositions.keySet()) {
            String name = v.getUniqueName();
            List<Integer> positions = inputPositions.get(v);
            toWrite.put(name, new ArrayList<Integer>(positions));
            //for (Integer pos: positions) {
            //    toWrite.get(name).add(pos);
            //}
        }
        this.taskConfig.setInputPositions(toWrite);
        /*
        for (FlinkVertex v: inputPositions.keySet()) {
            String name = v.getUniqueName();
            int pos = inputPositions.get(v);
            toWrite.put(name, pos);
        }
        this.taskConfig.setInputPositions(toWrite);
        */
    }

}
