package org.apache.flink.tez.wordcount_old;


import org.apache.hadoop.io.IntWritable;
import org.apache.tez.runtime.library.api.Partitioner;

public class SimplePartitioner implements Partitioner {

    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
        if (!(key instanceof IntWritable)) {
            throw new IllegalStateException("Partitioning key should be int");
        }
        IntWritable channel = (IntWritable) key;
        return channel.get();
    }
}
