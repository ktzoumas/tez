package org.apache.flink.tez.wordcount_old;


// Forwards to the sub-task with the same index

import org.apache.flink.tez.runtime.ChannelSelector;

public class ForwardingSelector<T> implements ChannelSelector<T> {

    int [] nextChannelToSendTo = new int [1];

    public ForwardingSelector(int subTaskIndex) {
        this.nextChannelToSendTo[0] = subTaskIndex;
    }

    @Override
    public int[] selectChannels(T record, int numberOfOutputChannels) {
        return nextChannelToSendTo;
    }
}
