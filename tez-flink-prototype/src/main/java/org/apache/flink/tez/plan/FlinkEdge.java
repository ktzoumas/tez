package org.apache.flink.tez.plan;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.TezConfiguration;

public abstract class FlinkEdge {

    protected FlinkVertex source;
    protected FlinkVertex target;
    protected TypeSerializer<?> typeSerializer;
    protected Edge cached;

    protected FlinkEdge(FlinkVertex source, FlinkVertex target, TypeSerializer<?> typeSerializer) {
        this.source = source;
        this.target = target;
        this.typeSerializer = typeSerializer;
    }

    public abstract Edge createEdge(TezConfiguration tezConf);

    public Edge getEdge () {
        return cached;
    }

}
