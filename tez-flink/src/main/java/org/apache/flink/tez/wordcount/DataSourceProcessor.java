package org.apache.flink.tez.wordcount;


import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

public abstract class DataSourceProcessor<T, IS extends InputSplit> extends SimpleProcessor {

    InputFormat<T, IS> inputFormat;

    TypeSerializer<T> typeSerializer;

    public DataSourceProcessor(ProcessorContext context, InputFormat<T, IS> inputFormat, TypeSerializer<T> typeSerializer) {
        super(context);
        this.inputFormat = inputFormat;
        this.typeSerializer = typeSerializer;
    }

    public abstract IS getSplit ();

    @Override
    public void run() throws Exception {

        KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

        TezRecordWriter<SerializationDelegate<T>> tezRecordWriter =
                new TezRecordWriter<SerializationDelegate<T>>(kvWriter);

        TezOutputCollector<T> collector = new TezOutputCollector<T>(tezRecordWriter, typeSerializer);

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
