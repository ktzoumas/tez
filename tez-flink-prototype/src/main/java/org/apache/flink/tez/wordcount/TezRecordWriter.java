package org.apache.flink.tez.wordcount;


import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.io.network.bufferprovider.BufferProvider;
import org.apache.flink.runtime.io.network.bufferprovider.LocalBufferPool;
import org.apache.flink.runtime.io.network.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.serialization.SpanningRecordSerializer;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.io.IOException;

/*
 * Same logic as org.apache.flink.runtime.io.network.api.RecordWriter
 * Writes to Tez KeyValueWriter rather than a Flink OutputGate
 *
 */
public class TezRecordWriter<T extends IOReadableWritable> extends TezBufferWriter {

    private final BufferProvider bufferPool;

    private RecordSerializer<T>[] serializers;

    private int numberOfOutputStreams;

    private int indexOfSourceStream;

    private ChannelSelector<T> channelSelector;


    public TezRecordWriter(KeyValueWriter writer, ChannelSelector<T> channelSelector, int numberOfOutputStreams, int indexOfSourceStream) {
        super(writer);
        this.numberOfOutputStreams = numberOfOutputStreams;
        this.indexOfSourceStream = indexOfSourceStream;
        this.serializers = new RecordSerializer [this.numberOfOutputStreams];
        for (int i = 0; i < this.numberOfOutputStreams; i++) {
            this.serializers[i] = new SpanningRecordSerializer<T>();
        }
        this.channelSelector = channelSelector;
        this.bufferPool = new LocalBufferPool(WordCount.GLOBAL_BUFFER_POOL, WordCount.TASK_NETWORK_PAGES);
    }



    @SuppressWarnings("unchecked")
    public void initializeSerializers() {

    }

    public void emit(final T record) throws IOException, InterruptedException {
        for (int targetChannel : this.channelSelector.selectChannels(record, this.numberOfOutputStreams)) {
            RecordSerializer<T> serializer = this.serializers[targetChannel];

            RecordSerializer.SerializationResult result = serializer.addRecord(record);
            while (result.isFullBuffer()) {
                Buffer buffer = serializer.getCurrentBuffer();

                if ((buffer != null) && (buffer.size() > 0)) {
                    sendBuffer(buffer, indexOfSourceStream, targetChannel);
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
        for (int targetChannel  = 0; targetChannel < this.numberOfOutputStreams; targetChannel ++) {
            RecordSerializer<T> serializer = this.serializers[targetChannel];

            Buffer buffer = serializer.getCurrentBuffer();

            if ((buffer != null) && (buffer.size() > 0)) {
                sendBuffer(buffer, indexOfSourceStream, targetChannel);
            }
            serializer.clear();
        }
    }
}
