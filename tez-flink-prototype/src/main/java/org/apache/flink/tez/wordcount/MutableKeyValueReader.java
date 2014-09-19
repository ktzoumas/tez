package org.apache.flink.tez.wordcount;


import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.task.AbstractTaskEvent;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.api.AbstractRecordReader;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.serialization.RecordDeserializer;
import org.apache.tez.runtime.api.Processor;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import java.io.IOException;

public class MutableKeyValueReader<T extends IOReadableWritable> extends AbstractRecordReader implements MutableReader<T> {

    KeyValueReader kvReader;

    RecordDeserializer<T> [] deserializers;

    Buffer currentBuffer;

    int numberOfInputStreams;

    int currentTargetDeserializer;

    private boolean endOfStream;

    SimpleProcessor parent;

    protected MutableKeyValueReader(SimpleProcessor parent, KeyValueReader kvReader, int numberOfInputStreams) {
        this.parent = parent;
        this.numberOfInputStreams = numberOfInputStreams;
        this.deserializers = new RecordDeserializer[numberOfInputStreams];
        for (int i = 0; i < numberOfInputStreams; i++)
            this.deserializers[i] = new AdaptiveSpanningRecordDeserializer<T>();
        this.endOfStream = false;
        this.kvReader = kvReader;
        this.currentBuffer = null;
    }

    private boolean readNextBuffer () throws IOException {
        if (kvReader.next()) {
            Object key = kvReader.getCurrentKey();
            Object value = kvReader.getCurrentValue();
            if (!(value instanceof BufferWritable && key instanceof PairWritable)) {
                throw new IllegalStateException("Wrong key/value type");
            }
            currentBuffer = ((BufferWritable) value).getBuffer();
            PairWritable tw = (PairWritable) key;
            currentTargetDeserializer = (int) tw.first();
            if (tw.second() != parent.getContext().getTaskIndex()) {
                int taskIndex = parent.getContext().getTaskIndex();
                throw new IllegalStateException("Should not have received this buffer");
            }
            deserializers[currentTargetDeserializer].setNextMemorySegment(currentBuffer.getMemorySegment(), currentBuffer.size());
            return true;
        }
        else {
            return false;
        }
    }


    @Override
    public boolean next(T target) throws IOException, InterruptedException {
        if (this.endOfStream) {
            return false;
        }
        while (true) {
            if (currentBuffer == null) {
                // Need to read in a new buffer
                if (!(readNextBuffer())) {
                    endOfStream = true;
                    return false;
                }
            }
            RecordDeserializer.DeserializationResult result = deserializers[currentTargetDeserializer].getNextRecord(target);
            if (result.isBufferConsumed()) {
                // loop again
                currentBuffer = null;
            }
            if (result.isFullRecord()) {
                return true;
            }
        }
    }

    @Override
    public boolean isInputClosed() {
        return this.endOfStream;
    }

    @Override
    public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {
        throw new IllegalStateException("Not supported");
    }

}
