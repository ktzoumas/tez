package org.apache.flink.tez.wordcount;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.runtime.plugable.SerializationDelegate;


public class PartitioningSelector<T> implements ChannelSelector<T> {

    private final TypeComparator<T> comparator;

    public PartitioningSelector(TypeComparator<T> comparator) {
        this.comparator = comparator;
    }

    private int [] channels = new int[1];


    @Override
    public int[] selectChannels(T record, int numberOfChannels) {
        int hash = this.comparator.hash(record);

        hash = murmurHash(hash);

        if (hash >= 0) {
            this.channels[0] = hash % numberOfChannels;
        }
        else if (hash != Integer.MIN_VALUE) {
            this.channels[0] = -hash % numberOfChannels;
        }
        else {
            this.channels[0] = 0;
        }

        return this.channels;
    }

    private final int murmurHash(int k) {
        k *= 0xcc9e2d51;
        k = Integer.rotateLeft(k, 15);
        k *= 0x1b873593;

        k = Integer.rotateLeft(k, 13);
        k *= 0xe6546b64;

        k ^= 4;
        k ^= k >>> 16;
        k *= 0x85ebca6b;
        k ^= k >>> 13;
        k *= 0xc2b2ae35;
        k ^= k >>> 16;

        return k;
    }
}
