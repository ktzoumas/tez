package org.apache.flink.tez.processor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.util.*;

import java.io.Serializable;

public class TezTaskConfig extends MockTaskConfig implements Serializable{

    public TezTaskConfig() {
        super();
    }

    public TezTaskConfig(Configuration config) {
        super(config);
    }
}
