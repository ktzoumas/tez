package org.apache.flink.tez.wordcount;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.io.network.api.RoundRobinChannelSelector;
import org.apache.flink.runtime.io.network.bufferprovider.BufferProvider;
import org.apache.flink.runtime.io.network.bufferprovider.GlobalBufferPool;
import org.apache.flink.runtime.io.network.bufferprovider.LocalBufferPool;
import org.apache.flink.runtime.io.network.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.operators.shipping.OutputEmitter;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.io.IOException;
import java.nio.channels.Channel;

/*
 * Same logic as org.apache.flink.runtime.io.network.api.RecordWriter
 * Writes to Tez KeyValueWriter rather than a Flink OutputGate
 *
 */
public class TezRecordWriter<T extends IOReadableWritable> extends TezBufferWriter {

    private final BufferProvider bufferPool;

    private RecordSerializer<T>[] serializers;

    private int numChannels = WordCount.DOP;

    private ChannelSelector<T> channelSelector;


    public TezRecordWriter(KeyValueWriter writer, ChannelSelector<T> channelSelector) {
        super(writer);
        //this.bufferPool = new LocalBufferPool(new GlobalBufferPool(10, 3), 10);
        this.serializers = new RecordSerializer [this.numChannels];
        for (int i = 0; i < this.numChannels; i++)
            this.serializers[i] = new SpanningRecordSerializer<T>();
        this.channelSelector = channelSelector;
        this.bufferPool = new LocalBufferPool(WordCount.GLOBAL_BUFFER_POOL, WordCount.TASK_NETWORK_PAGES);
    }



    @SuppressWarnings("unchecked")
    public void initializeSerializers() {

    }

    public void emit(final T record) throws IOException, InterruptedException {
        for (int targetChannel : this.channelSelector.selectChannels(record, this.numChannels)) {
            RecordSerializer<T> serializer = this.serializers[targetChannel];

            RecordSerializer.SerializationResult result = serializer.addRecord(record);
            while (result.isFullBuffer()) {
                Buffer buffer = serializer.getCurrentBuffer();

                if ((buffer != null) && (buffer.size() > 0)) {
                    sendBuffer(buffer, targetChannel);
                }


                buffer = this.bufferPool.requestBuffer(this.bufferPool.getBufferSize());

                if (buffer == null)
                    throw new IOException("Could not get buffer from local buffer pool");

                //buffer = new Buffer(new MemorySegment(new byte[1024]), 100, null);

                result = serializer.setNextBuffer(buffer);
            }
        }
    }

    public void flush() throws IOException, InterruptedException {
        for (int targetChannel  = 0; targetChannel < this.numChannels; targetChannel ++) {
            RecordSerializer<T> serializer = this.serializers[targetChannel];

            Buffer buffer = serializer.getCurrentBuffer();

            if ((buffer != null) && (buffer.size() > 0)) {
                sendBuffer(buffer, targetChannel);
            }
            serializer.clear();
        }
    }
}
