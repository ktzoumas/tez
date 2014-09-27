package org.apache.flink.tez.input;

import org.apache.tez.runtime.library.conf.*;

import java.util.Map;

/**
 * Created by kostas on 19/09/14.
 */
public class FlinkUnorderedPartitionedKVEdgeConfig extends UnorderedPartitionedKVEdgeConfig {


    protected  FlinkUnorderedPartitionedKVEdgeConfig(
            UnorderedPartitionedKVOutputConfig outputConfiguration,
            UnorderedKVInputConfig inputConfiguration) {
        super (outputConfiguration, inputConfiguration);
    }

    /**
     * Create a builder to configure the relevant Input and Output
     * @param keyClassName the key class name
     * @param valueClassName the value class name
     * @return a builder to configure the edge
     */

    public static Builder newBuilder(String keyClassName, String valueClassName,
                                     String partitionerClassName) {
        return new Builder(keyClassName, valueClassName, partitionerClassName, null);
    }

    @Override
    public String getInputClassName() {
        return FlinkUnorderedKVInput.class.getName();
    }

    public static class Builder extends UnorderedPartitionedKVEdgeConfig.Builder {

        protected Builder(String keyClassName, String valueClassName, String partitionerClassName,
                          Map<String, String> partitionerConf) {
            super (keyClassName, valueClassName, partitionerClassName, partitionerConf);

        }

        public FlinkUnorderedPartitionedKVEdgeConfig build() {
            return new FlinkUnorderedPartitionedKVEdgeConfig(outputBuilder.build(), inputBuilder.build());
        }

    }
}
