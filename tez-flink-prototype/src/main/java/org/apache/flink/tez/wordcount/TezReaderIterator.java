package org.apache.flink.tez.wordcount;


import org.apache.flink.util.MutableObjectIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.runtime.library.api.KeyValueReader;

import java.io.IOException;

public class TezReaderIterator<T> implements MutableObjectIterator<T>{

    private KeyValueReader kvReader;

    public TezReaderIterator(KeyValueReader kvReader) {
        this.kvReader = kvReader;
    }

    @Override
    public T next(T reuse) throws IOException {
        if (kvReader.next()) {
            Object key = kvReader.getCurrentKey();
            Object value = kvReader.getCurrentValue();
            if (!(key instanceof IntWritable && value instanceof WritableSerializationDelegate)) {
                throw new IllegalStateException("Wrong key/value type");
            }
            WritableSerializationDelegate<T> delegate = (WritableSerializationDelegate<T>) value;
            reuse = delegate.getInstance();
            return reuse;
        }
        else {
            return null;
        }
    }
}
