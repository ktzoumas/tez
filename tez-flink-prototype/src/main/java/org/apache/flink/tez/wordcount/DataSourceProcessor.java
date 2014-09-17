package org.apache.flink.tez.wordcount;


import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.Collector;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import java.util.Iterator;
/*
public abstract class DataSourceProcessor<T> extends SimpleProcessor{

    private InputFormat<T, InputSplit> format;

    public DataSourceProcessor(ProcessorContext context) {
        super(context);
    }

    public abstract TypeSerializer<T> createTypeSerializer();

    @Override
    public void run() throws Exception {
        KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

        ForwardingSelector<T> channelSelector = new ForwardingSelector<T>(this.getContext().getTaskIndex());

        TezRecordWriter<SerializationDelegate<T>> tezRecordWriter =
                new TezRecordWriter<SerializationDelegate<T>>(kvWriter, channelSelector);

        TezOutputCollector<T> collector = new TezOutputCollector<T>(tezRecordWriter, typeSerializer);

        final Iterator<InputSplit> splitIterator = getInputSplits();

        while (splitIterator.hasNext()) {

            final InputSplit split = splitIterator.next();

            final InputFormat<T, InputSplit> format = this.format;

            // open input format
            format.open(split);

            T record = null;
            // as long as there is data to read
            while (!format.reachedEnd()) {
                // build next pair and ship pair if it is valid
                if ((record = format.nextRecord(record)) != null) {
                    collector.collect(record);
                }
            }
            format.close();
        }
        collector.close();
    }
}
*/