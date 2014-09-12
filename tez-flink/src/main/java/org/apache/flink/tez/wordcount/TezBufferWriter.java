package org.apache.flink.tez.wordcount;


import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.tez.wordcount.BufferWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.io.IOException;

public class TezBufferWriter {

    private KeyValueWriter kvWriter;

    public TezBufferWriter(KeyValueWriter kvWriter) {
        this.kvWriter = kvWriter;
    }

    public void sendBuffer(Buffer buffer, int targetChannel) throws IOException, InterruptedException {
        BufferWritable bufferWritable = new BufferWritable(buffer);
        LongWritable size = new LongWritable(targetChannel);
        kvWriter.write(size, bufferWritable);
    }

    /*
    public void sendEvent(AbstractEvent event, int targetChannel) throws IOException, InterruptedException {
        this.outputGate.sendEvent(event, targetChannel);
    }

    public void sendBufferAndEvent(Buffer buffer, AbstractEvent event, int targetChannel) throws IOException, InterruptedException {
        this.outputGate.sendBufferAndEvent(buffer, event, targetChannel);
    }

    public void broadcastBuffer(Buffer buffer) throws IOException, InterruptedException {
        this.outputGate.broadcastBuffer(buffer);
    }

    public void broadcastEvent(AbstractEvent event) throws IOException, InterruptedException {
        this.outputGate.broadcastEvent(event);
    }

    // -----------------------------------------------------------------------------------------------------------------

    public void subscribeToEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {
        this.outputGate.subscribeToEvent(eventListener, eventType);
    }

    public void unsubscribeFromEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {
        this.outputGate.unsubscribeFromEvent(eventListener, eventType);
    }

    public void sendEndOfSuperstep() throws IOException, InterruptedException {
        this.outputGate.broadcastEvent(EndOfSuperstepEvent.INSTANCE);
    }
    */
}
