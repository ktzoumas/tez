package org.apache.flink.tez.wordcount;

import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;

// Forwards to the sub-task with the same index

public class ForwardingSelector<T> implements ChannelSelector<SerializationDelegate<T>> {

    int [] nextChannelToSendTo = new int [1];

    public ForwardingSelector(int subTaskIndex) {
        this.nextChannelToSendTo[0] = subTaskIndex;
    }

    @Override
    public int[] selectChannels(SerializationDelegate<T> record, int numberOfOutputChannels) {
        return nextChannelToSendTo;
    }
}
