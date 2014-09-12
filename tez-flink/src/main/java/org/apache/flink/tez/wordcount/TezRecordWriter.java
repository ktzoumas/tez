package org.apache.flink.tez.wordcount;


import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.io.network.api.RoundRobinChannelSelector;
import org.apache.flink.runtime.io.network.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.serialization.SpanningRecordSerializer;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.io.IOException;

public class TezRecordWriter<T extends IOReadableWritable> extends TezBufferWriter {

    //private final BufferProvider bufferPool;

    // 512 bytes ought to be enough for everyone
    private MemorySegment memorySegment = new MemorySegment(new byte[512]);

    private RecordSerializer<T> serializer;

    private int numChannels = WordCount.DOP;

    private ChannelSelector<T> channelSelector = new RoundRobinChannelSelector<T>();

    //private final ChannelSelector<T> channelSelector = new OutputEmitter<T>();

    // -----------------------------------------------------------------------------------------------------------------

    public TezRecordWriter(KeyValueWriter writer) {
        super(writer);
        //this.bufferPool = new LocalBufferPool(new GlobalBufferPool(10, 3), 10);
        this.serializer = new SpanningRecordSerializer<T>();
    }


    // -----------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public void initializeSerializers() {
        this.serializer = new SpanningRecordSerializer<T>();
    }

    public void emit(final T record) throws IOException, InterruptedException {
        RecordSerializer.SerializationResult result = this.serializer.addRecord(record);
        while (result.isFullBuffer()) {
            Buffer buffer = serializer.getCurrentBuffer();
            if (buffer != null) {
                int targetChannel = this.channelSelector.selectChannels(record, numChannels)[0];
                sendBuffer(buffer, targetChannel);
            }

            //buffer = this.bufferPool.requestBufferBlocking(this.bufferPool.getBufferSize());

            buffer = new Buffer(memorySegment, 10, null);

            result = serializer.setNextBuffer(buffer);
        }
    }

    public void flush() throws IOException, InterruptedException {
        for (int targetChannel  = 0; targetChannel < numChannels; targetChannel ++) {
            Buffer buffer = serializer.getCurrentBuffer();
            if (buffer != null) {
                sendBuffer(buffer, targetChannel);
            }
            serializer.clear();
        }
    }
}
