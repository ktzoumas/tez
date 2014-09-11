package org.apache.flink.tez.oldstuff;


import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.types.StringValue;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TezStringSerializer implements Writable {

    private String instance;

    private final StringSerializer stringSerializer;

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getInstance() {
        return this.instance;
    }

    public TezStringSerializer() {
        this.stringSerializer = new StringSerializer();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        StringValue.writeString(this.instance, out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.instance = StringValue.readString(in);
    }
}
