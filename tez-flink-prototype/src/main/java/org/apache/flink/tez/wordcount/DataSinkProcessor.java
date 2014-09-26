package org.apache.flink.tez.wordcount;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.tez.examples.WordCount;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

public abstract class DataSinkProcessor<T> extends SimpleProcessor {

    private OutputFormat<T> format;
    private TypeSerializer<T> typeSerializer;

    public DataSinkProcessor(ProcessorContext context) {
        super(context);
        this.format = createOutputFormat();
        this.typeSerializer = createTypeSerializer();
    }

    public abstract OutputFormat<T> createOutputFormat ();

    public abstract TypeSerializer<T> createTypeSerializer ();

    @Override
    public void run() throws Exception {

        KeyValueReader kvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();

        //WritableSerializationDelegate.registerSerializer(typeSerializer);
        TezReaderIterator<T> input = new TezReaderIterator<T>(kvReader);

        T record = typeSerializer.createInstance();
        while ((record = input.next(record)) != null) {
            format.writeRecord(record);
        }
    }

    @Override
    public void initialize() throws Exception {
        super.initialize();
        format.open(getContext().getTaskIndex(), WordCount.DOP);
    }

    @Override
    public void close() throws Exception {
        super.close();
        format.close();
    }

}
