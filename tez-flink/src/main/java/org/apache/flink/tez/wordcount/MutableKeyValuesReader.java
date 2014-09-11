package org.apache.flink.tez.wordcount;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.event.task.AbstractTaskEvent;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.api.AbstractRecordReader;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.io.network.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.serialization.RecordDeserializer;
import org.apache.hadoop.io.LongWritable;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValuesReader;

import java.io.IOException;


public class MutableKeyValuesReader<T extends IOReadableWritable> extends AbstractTezMutableReader<T> {

    KeyValuesReader kvReader;

    public MutableKeyValuesReader(KeyValuesReader kvReader) {
        super (new AdaptiveSpanningRecordDeserializer<T>(), null);
        this.kvReader = kvReader;
    }

    @Override
    public boolean readNextBuffer () throws IOException {
        if (kvReader.next()) {
            Object keyObj = kvReader.getCurrentKey();
            Iterable<Object> values = kvReader.getCurrentValues();
            if (!(keyObj instanceof LongWritable)) {
                throw new IllegalStateException("Wrong key/value type");
            }
            LongWritable key = (LongWritable) keyObj;
            for (Object value : values) {
                if (!(value instanceof BufferWritable)) {
                    throw new IllegalStateException("Wrong key/value type");
                }
                currentBuffer = ((BufferWritable) value).getBuffer();
                deserializer.setNextMemorySegment(currentBuffer.getMemorySegment(), currentBuffer.size());
                return true;
            }
            throw new IllegalStateException("Should not be here");
        }
        else {
            return false;
        }
    }
}
