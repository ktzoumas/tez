package org.apache.flink.tez.wordcount;


import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.task.AbstractTaskEvent;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.api.AbstractRecordReader;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.serialization.RecordDeserializer;

import java.io.IOException;

public abstract class AbstractTezMutableReader<T extends IOReadableWritable> extends AbstractRecordReader implements MutableReader<T> {

    RecordDeserializer<T> deserializer;

    Buffer currentBuffer;

    protected AbstractTezMutableReader(RecordDeserializer<T> deserializer, Buffer currentBuffer) {
        this.deserializer = deserializer;
        this.currentBuffer = currentBuffer;
    }

    public abstract boolean readNextBuffer () throws IOException;

    @Override
    public boolean next(T target) throws IOException, InterruptedException {
        while (true) {
            if (currentBuffer == null) {
                // Need to read in a new buffer
                if (!(readNextBuffer())) {
                    return false;
                }
            }
            RecordDeserializer.DeserializationResult result = deserializer.getNextRecord(target);
            if (result.isBufferConsumed()) {
                currentBuffer = null;
            }
            if (result.isFullRecord()) {
                return true;
            }
        }
    }

    @Override
    public boolean isInputClosed() {
        return false;
    }

    @Override
    public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {
        throw new IllegalStateException("Not supported");
    }

}
