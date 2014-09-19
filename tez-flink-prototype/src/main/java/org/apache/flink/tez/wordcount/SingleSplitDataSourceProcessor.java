package org.apache.flink.tez.wordcount;


import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.io.network.api.RoundRobinChannelSelector;
import org.apache.flink.runtime.operators.shipping.OutputEmitter;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

public abstract class SingleSplitDataSourceProcessor<T, IS extends InputSplit> extends SimpleProcessor {

    InputFormat<T, IS> inputFormat;

    TypeSerializer<T> typeSerializer;

    public SingleSplitDataSourceProcessor(ProcessorContext context) {
        super(context);
        this.inputFormat = createInputFormat();
        this.typeSerializer = createTypeSerializer();
    }

    public abstract IS getSplit ();

    public abstract InputFormat<T, IS> createInputFormat ();

    public abstract TypeSerializer<T> createTypeSerializer ();

    @Override
    public void run() throws Exception {

        KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

        ForwardingSelector<T> channelSelector =
                new ForwardingSelector<T>(this.getContext().getTaskIndex());

        TezOutputCollector<T> collector = new TezOutputCollector<T>(kvWriter, channelSelector, typeSerializer, 1);

        inputFormat.open(getSplit());

        T outValue = null;
        while (!inputFormat.reachedEnd()) {
            if ((outValue = inputFormat.nextRecord(outValue)) != null) {
                collector.collect(outValue);
            }
        }
        collector.close();
    }
}
