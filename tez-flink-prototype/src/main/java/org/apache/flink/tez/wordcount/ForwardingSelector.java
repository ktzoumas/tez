package org.apache.flink.tez.wordcount;

import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;


public class ForwardingSelector<T> implements ChannelSelector<SerializationDelegate<T>> {

    int subTaskIndex;

    public ForwardingSelector(int subTaskIndex) {
        this.subTaskIndex = subTaskIndex;
    }

    @Override
    public int[] selectChannels(SerializationDelegate<T> record, int numberOfOutputChannels) {
        int [] channels = new int[1];
        channels[0] = subTaskIndex;
        return channels;
    }
}
