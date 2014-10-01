package org.apache.flink.tez.plan;


import org.apache.flink.compiler.CompilerException;
import org.apache.flink.tez.runtime.FlinkDataSinkProcessor;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;

import java.io.IOException;

public class FlinkDataSinkVertex extends FlinkVertex {

    public FlinkDataSinkVertex(String taskName, int parallelism, TezTaskConfig taskConfig) {
        super(taskName, parallelism, taskConfig);
    }

    @Override
    public Vertex createVertex(TezConfiguration conf) {
        try {
            this.writeInputPositionsToConfig();

            conf.set("io.flink.processor.taskconfig", InstantiationUtil.writeObjectToConfig(taskConfig));

            ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                    FlinkDataSinkProcessor.class.getName());

            descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(conf));

            cached = Vertex.create(this.getUniqueName(), descriptor, getParallelism());

            return cached;
        }
        catch (IOException e) {
            throw new CompilerException(
                    "An error occurred while creating a Tez Vertex: " + e.getMessage(), e);
        }
    }
}
