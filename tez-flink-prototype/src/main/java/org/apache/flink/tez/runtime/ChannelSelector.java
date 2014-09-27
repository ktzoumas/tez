package org.apache.flink.tez.runtime;

import java.io.Serializable;

public interface ChannelSelector<T> extends Serializable {

    /**
     * Called to determine to which attached {@link org.apache.flink.runtime.io.network.channels.OutputChannel} objects the given record shall be forwarded.
     *
     * @param record
     *        the record to the determine the output channels for
     * @param numberOfOutputChannels
     *        the total number of output channels which are attached to respective output gate
     * @return a (possibly empty) array of integer numbers which indicate the indices of the output channels through
     *         which the record shall be forwarded
     */
    int[] selectChannels(T record, int numberOfOutputChannels);
}