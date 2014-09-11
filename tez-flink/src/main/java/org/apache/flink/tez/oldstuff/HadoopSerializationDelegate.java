package org.apache.flink.tez.oldstuff;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.serialization.DataInputDeserializer;
import org.apache.flink.runtime.io.network.serialization.DataOutputSerializer;
import org.apache.flink.types.StringValue;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class HadoopSerializationDelegate<T> implements Writable {

    private T instance;

    private final TypeSerializer<T> typeSerializer;

    public HadoopSerializationDelegate(TypeSerializer<T> serializer) {
        this.typeSerializer = serializer;

    }

    public HadoopSerializationDelegate() {
        typeSerializer = null;
    }

    public void setInstance(T instance) {
        this.instance = instance;
    }

    public T getInstance() {
        return this.instance;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(128);
        //this.typeSerializer.serialize(this.instance, out);
        //byte [] buffer = new byte [dataOutputSerializer.length()];
        //dataOutputSerializer.write(buffer);
        //out.write(dataOutputSerializer.length());
        //out.write(buffer);
        //dataOutputSerializer.clear();

        if (!(this.instance instanceof String)) {
            throw new IllegalStateException("Should be string");
        }
        String value = (String) this.instance;
        StringValue.writeString(value, out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //int size = in.readInt();
        //byte [] buffer = new byte [size];
        //in.readFully(buffer);
        //DataInputDeserializer dataInputDeserializer = new DataInputDeserializer(buffer, 0, size);
        //this.instance = this.typeSerializer.deserialize(this.instance, dataInputDeserializer);
        String value = StringValue.readString(in);
        this.instance = (T) value;
    }
}
