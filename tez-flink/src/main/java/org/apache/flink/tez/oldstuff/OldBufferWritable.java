package org.apache.flink.tez.oldstuff;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.BufferRecycler;
import org.apache.flink.runtime.io.network.bufferprovider.DiscardBufferPool;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


public class OldBufferWritable implements Writable {

    private  MemorySegment memorySegment;

    private BufferRecycler recycler;

    // -----------------------------------------------------------------------------------------------------------------

    private AtomicInteger referenceCounter;

    private int size;

    // -----------------------------------------------------------------------------------------------------------------

    public OldBufferWritable(MemorySegment memorySegment, int size, BufferRecycler recycler) {
        this.memorySegment = memorySegment;
        this.size = size;
        this.recycler = recycler;

        // we are the first, so we start with reference count of one
        this.referenceCounter = new AtomicInteger(1);
    }

    public OldBufferWritable(Buffer buffer) {
        this.memorySegment = buffer.getMemorySegment();
        this.size = buffer.size();
        this.recycler = new DiscardBufferPool();
        this.referenceCounter = new AtomicInteger(1);
    }

    /**
     * @param toDuplicate Buffer instance to duplicate
     */
    private OldBufferWritable(OldBufferWritable toDuplicate) {
        if (toDuplicate.referenceCounter.getAndIncrement() == 0) {
            throw new IllegalStateException("Buffer was released before duplication.");
        }

        this.memorySegment = toDuplicate.memorySegment;
        this.size = toDuplicate.size;
        this.recycler = toDuplicate.recycler;
        this.referenceCounter = toDuplicate.referenceCounter;
    }

    // -----------------------------------------------------------------------------------------------------------------

    public MemorySegment getMemorySegment() {
        return this.memorySegment;
    }

    public int size() {
        return this.size;
    }

    public void limitSize(int size) {
        if (size >= 0 && size <= this.memorySegment.size()) {
            this.size = size;
        } else {
            throw new IllegalArgumentException();
        }
    }

    public void recycleBuffer() {
        int refCount = this.referenceCounter.decrementAndGet();
        if (refCount == 0) {
            this.recycler.recycle(this.memorySegment);
        }
    }

    public OldBufferWritable duplicate() {
        return new OldBufferWritable(this);
    }

    public void copyToBuffer(OldBufferWritable destinationBuffer) {
        if (size() > destinationBuffer.size()) {
            throw new IllegalArgumentException("Destination buffer is too small to store content of source buffer.");
        }

        this.memorySegment.copyTo(0, destinationBuffer.memorySegment, 0, size);
    }

    public Buffer getBuffer () {
        return new Buffer (this.memorySegment, this.size, this.recycler);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        int size = this.size();
        byte [] buf = new byte[size];
        this.getMemorySegment().get(0, buf);
        out.write(size);
        out.write(buf);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        byte [] buf = new byte [size];
        this.memorySegment = new MemorySegment(buf);
        this.recycler = new DiscardBufferPool();
        this.referenceCounter = new AtomicInteger(1);
    }
}
