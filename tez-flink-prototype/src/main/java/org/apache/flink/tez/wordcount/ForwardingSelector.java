package org.apache.flink.tez.wordcount;


// Forwards to the sub-task with the same index

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
