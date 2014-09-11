package org.apache.flink.tez.wordcount;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class TezOutputCollector<T> implements Collector<T> {

    protected TezRecordWriter<SerializationDelegate<T>> writer;
    private final SerializationDelegate<T> delegate;

    public TezOutputCollector(TezRecordWriter<SerializationDelegate<T>> writer, TypeSerializer<T> typeSerializer) {
        this.delegate = new SerializationDelegate<T>(typeSerializer);
        this.writer = writer;
    }

    @Override
    public void collect(T record) {
        this.delegate.setInstance(record);
        try {
            writer.emit(this.delegate);
        }
        catch (IOException e) {
            throw new RuntimeException("Emitting the record caused an I/O exception: " + e.getMessage(), e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Emitting the record was interrupted: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
