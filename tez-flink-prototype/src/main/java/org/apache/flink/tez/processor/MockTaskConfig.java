package org.apache.flink.tez.processor;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.util.TaskConfig;

public class MockTaskConfig extends TaskConfig {

    public MockTaskConfig() {
        super(null);
    }

    public MockTaskConfig(Configuration config) {
        super(config);
    }
}
