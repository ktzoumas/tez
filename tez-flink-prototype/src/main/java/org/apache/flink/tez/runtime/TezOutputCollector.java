package org.apache.flink.tez.runtime;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.tez.input.WritableSerializationDelegate;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TezOutputCollector<T> implements Collector<T> {

    private List<KeyValueWriter> writers;

    private List<ChannelSelector<T>> outputEmitters;

    private List<Integer> numberOfStreamsInOutputs;

    private int numOutputs;

    private TypeSerializer<T> serializer;

    WritableSerializationDelegate<T> delegate;

    public TezOutputCollector(List<KeyValueWriter> writers, List<ChannelSelector<T>> outputEmitters, TypeSerializer<T> serializer, List<Integer> numberOfStreamsInOutputs) {
        this.writers = writers;
        this.outputEmitters = outputEmitters;
        this.numberOfStreamsInOutputs = numberOfStreamsInOutputs;
        this.serializer = serializer;
        this.numOutputs = writers.size();
        this.delegate = new WritableSerializationDelegate<T>(serializer);
    }

    @Override
    public void collect(T record) {
        delegate.setInstance(record);
        for (int i = 0; i < numOutputs; i++) {
            KeyValueWriter writer = writers.get(i);
            ChannelSelector<T> outputEmitter = outputEmitters.get(i);
            int numberOfStreamsInOutput = numberOfStreamsInOutputs.get(i);
            try {
                for (int channel : outputEmitter.selectChannels(record, numberOfStreamsInOutput)) {
                    IntWritable key = new IntWritable(channel);
                    writer.write(key, delegate);
                }
            }
            catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    @Override
    public void close() {

    }
}
