package org.apache.flink.tez.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;

public class TezTaskConfig extends TaskConfig {

    private static final String NUMBER_SUBTASKS_IN_OUTPUT = "tez.num_subtasks_in_output";

    private static final String CHANNEL_SELECTOR = "tez.channel_selector";

    public TezTaskConfig(Configuration config) {
        super(config);
    }

    public void setNumberSubtasksInOutput(int numberSubtasksInOutput) {
        this.config.setInteger(NUMBER_SUBTASKS_IN_OUTPUT, numberSubtasksInOutput);
    }

    public String getTaskName() {
        return this.config.getInteger(NUMBER_SUBTASKS_IN_OUTPUT, -1)
    }

    public void setChannelSelector (ChannelSelector<?> channelSelector) {
        try {
            InstantiationUtil.writeObjectToConfig(channelSelector, this.config, CHANNEL_SELECTOR);
        } catch (IOException e) {
            throw new RuntimeException("Error while writing the channel selector object to the task configuration.");
        }
    }

    public ChannelSelector<?> getChannelSelector () {
        ChannelSelector<?> channelSelector = null;
        try {
            channelSelector = (ChannelSelector<?>) InstantiationUtil.readObjectFromConfig(this.config, CHANNEL_SELECTOR, getConfiguration().getClassLoader())
        } catch (IOException e) {
            throw new RuntimeException("Error while reading the channel selector object from the task configuration.");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Error while reading the channel selector object from the task configuration. " +
                    "ChannelSelector class not found.");
        }
        if (channelSelector == null) {
            throw new NullPointerException();
        }
        return channelSelector;
    }

}
