package org.apache.flink.tez.wordcount;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements Writable {

    private long first;
    private long second;

    public PairWritable(long first, long second) {
        this.first = first;
        this.second = second;
    }

    public PairWritable() {
    }

    public long first() {
        return first;
    }

    public long second() {
        return second;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        out.writeLong(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readLong();
        second = in.readLong();
    }


}
