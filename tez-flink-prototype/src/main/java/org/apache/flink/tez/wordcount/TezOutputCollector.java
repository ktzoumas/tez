package org.apache.flink.tez.wordcount;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.io.IOException;

public class TezOutputCollector<T> implements Collector<T> {

    private KeyValueWriter writer;

    private ChannelSelector<T> outputEmitter;

    int numberOfOutputStreams;

    WritableSerializationDelegate<T> delegate;

    public TezOutputCollector(KeyValueWriter writer, ChannelSelector<T> outputEmitter,
                              TypeSerializer<T> serializer,
                              int numberOfOutputStreams) {
        this.writer = writer;
        this.outputEmitter = outputEmitter;
        this.numberOfOutputStreams = numberOfOutputStreams;
        this.delegate = new WritableSerializationDelegate<T>(serializer);
    }

    @Override
    public void collect(T record) {
        try {
            delegate.setInstance(record);
            for (int channel : outputEmitter.selectChannels(record, numberOfOutputStreams)) {
                IntWritable key = new IntWritable(channel);
                writer.write(key, delegate);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void close() {
    }
}
