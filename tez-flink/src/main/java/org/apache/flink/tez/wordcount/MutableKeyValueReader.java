package org.apache.flink.tez.wordcount;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.task.AbstractTaskEvent;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.api.AbstractRecordReader;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.serialization.RecordDeserializer;
import org.apache.flink.tez.wordcount.BufferWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.tez.runtime.library.api.KeyValueReader;

import java.io.IOException;


public class MutableKeyValueReader<T extends IOReadableWritable> extends AbstractTezMutableReader<T> {

    KeyValueReader kvReader;

    public MutableKeyValueReader(KeyValueReader kvReader) {
        super (new AdaptiveSpanningRecordDeserializer<T>(), null);
        this.kvReader = kvReader;
    }

    @Override
    public boolean readNextBuffer () throws IOException {
        if (kvReader.next()) {
            Object key = kvReader.getCurrentKey();
            Object value = kvReader.getCurrentValue();
            if (!(value instanceof BufferWritable && key instanceof LongWritable)) {
                throw new IllegalStateException("Wrong key/value type");
            }
            currentBuffer = ((BufferWritable) value).getBuffer();
            deserializer.setNextMemorySegment(currentBuffer.getMemorySegment(), currentBuffer.size());
            return true;
        }
        else {
            return false;
        }
    }
}
