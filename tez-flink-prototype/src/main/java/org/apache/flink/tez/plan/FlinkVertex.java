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

public class FlinkVertex {

    private Vertex cached;
    private String taskName;
    private int parallelism;
    private TezTaskConfig taskConfig;

    public TezTaskConfig getConfig() {
        return taskConfig;
    }

    public FlinkVertex(String taskName, int parallelism, TezTaskConfig taskConfig) {
        this.cached = null;
        this.taskName = taskName;
        this.parallelism = parallelism;
        this.taskConfig = taskConfig;
    }

    public int getParallelism () {
        return parallelism;
    }

    public Vertex createVertex (TezConfiguration conf) {
        try {
            conf.set("io.flink.processor.taskconfig", InstantiationUtil.writeObjectToConfig(taskConfig));

            ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                    FlinkProcessor.class.getName());

            descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(conf));

            cached = Vertex.create(taskName, descriptor, parallelism);

            return cached;
        }
        catch (IOException e) {
            throw new CompilerException(
                    "An error occurred while creating a Tez Vertex: " + e.getMessage(), e);
        }
    }

    public Vertex getVertex () {
        return cached;
    }

}
