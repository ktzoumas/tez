package org.apache.flink.tez.wordcount_old;


import org.apache.flink.tez.runtime.ChannelSelector;

public class RoundRobinSelector<T> implements ChannelSelector<T> {


    private final int[] nextChannelToSendTo = new int[1];

    /**
     * Constructs a new default channel selector.
     */
    public RoundRobinSelector() {
        this.nextChannelToSendTo[0] = 0;
    }


    @Override
    public int[] selectChannels(final T record, final int numberOfOutputChannels) {

        this.nextChannelToSendTo[0] = (this.nextChannelToSendTo[0] + 1) % numberOfOutputChannels;

        return this.nextChannelToSendTo;
    }

}
