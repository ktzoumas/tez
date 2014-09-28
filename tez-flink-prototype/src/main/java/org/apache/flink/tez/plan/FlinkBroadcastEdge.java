package org.apache.flink.tez.plan;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.tez.input.FlinkUnorderedKVEdgeConfig;
import org.apache.flink.tez.input.WritableSerializationDelegate;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.TezConfiguration;

import java.io.IOException;

public class FlinkBroadcastEdge extends FlinkEdge {

    public FlinkBroadcastEdge(FlinkVertex source, FlinkVertex target, TypeSerializer<?> typeSerializer) {
        super(source, target, typeSerializer);
    }

    @Override
    public Edge createEdge(TezConfiguration tezConf) {
        try {

            FlinkUnorderedKVEdgeConfig edgeConfig = (FlinkUnorderedKVEdgeConfig)
                    (FlinkUnorderedKVEdgeConfig
                            .newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName())
                            .setFromConfiguration(tezConf)
                            .configureInput()
                            .setAdditionalConfiguration("io.flink.typeserializer", InstantiationUtil.writeObjectToConfig(
                                    this.typeSerializer
                            )))
                            .done()
                            .build();
            EdgeProperty property = edgeConfig.createDefaultBroadcastEdgeProperty();
            this.cached = Edge.create(source.getVertex(), target.getVertex(), property);
            return cached;

        } catch (IOException e) {
            throw new CompilerException(
                    "An error occurred while creating a Tez Forward Edge: " + e.getMessage(), e);
        }
    }
}
