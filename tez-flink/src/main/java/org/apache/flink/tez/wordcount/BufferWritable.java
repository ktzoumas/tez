package org.apache.flink.tez.wordcount;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Arrays;


public class BufferWritable implements Writable {

    private Buffer buffer;

    public BufferWritable(Buffer buffer) {
        this.buffer = buffer;
    }

    public BufferWritable() {
        buffer = null;
    }

    public Buffer getBuffer() {
        return this.buffer;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(buffer.size());
        MemorySegment memorySegment = buffer.getMemorySegment();
        out.writeInt(memorySegment.size());
        byte [] memory = new byte [memorySegment.size()];
        memorySegment.get(0, memory);
        out.write(memory);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int bufSize = in.readInt();
        int memSize = in.readInt();
        byte [] memory = new byte [memSize];
        in.readFully(memory);
        MemorySegment memorySegment = new MemorySegment(memory);
        buffer = new Buffer(memorySegment, bufSize, null);
    }

    @Override
    public String toString () {
        String out = "";
        for (int i = 0; i < buffer.size(); i++)
            out += buffer.getMemorySegment().getChar(i);
        return out;
    }


    public static void main (String [] args) {
        String s = "Hello, world";
        byte [] b = s.getBytes();
        MemorySegment mem = new MemorySegment(b);
        Buffer buf = new Buffer (mem, mem.size(), null);
        BufferWritable bw = new BufferWritable(buf);
        try {
            DataOutputStream dos = new DataOutputStream(new FileOutputStream("/tmp/serde_test"));
            bw.write(dos);
            DataInputStream dis = new DataInputStream(new FileInputStream("/tmp/serde_test"));
            BufferWritable newBw = new BufferWritable();
            newBw.readFields(dis);
            System.out.println(newBw.getBuffer().toString());
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }


}
