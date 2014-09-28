package org.apache.flink.tez.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;

public class TezTaskConfig extends TaskConfig {

    private static final String NUMBER_SUBTASKS_IN_OUTPUT = "tez.num_subtasks_in_output";

    private static final String INPUT_SPLIT_PROVIDER = "tez.input_split_provider";

    public TezTaskConfig(Configuration config) {
        super(config);
    }

    public void setNumberSubtasksInOutput(int numberSubtasksInOutput) {
        this.config.setInteger(NUMBER_SUBTASKS_IN_OUTPUT, numberSubtasksInOutput);
    }

    public int getNumberSubtasksInOutput() {
        return this.config.getInteger(NUMBER_SUBTASKS_IN_OUTPUT, -1);
    }


    public void setInputSplitProvider (InputSplitProvider inputSplitProvider) {
        try {
            InstantiationUtil.writeObjectToConfig(inputSplitProvider, this.config, INPUT_SPLIT_PROVIDER);
        } catch (IOException e) {
            throw new RuntimeException("Error while writing the input split provider object to the task configuration.");
        }
    }

    public InputSplitProvider getInputSplitProvider () {
        InputSplitProvider inputSplitProvider = null;
        try {
            inputSplitProvider = (InputSplitProvider) InstantiationUtil.readObjectFromConfig(this.config, INPUT_SPLIT_PROVIDER, getConfiguration().getClassLoader());
        }
        catch (IOException e) {
            throw new RuntimeException("Error while reading the input split provider object from the task configuration.");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Error while reading the input split provider object from the task configuration. " +
                    "ChannelSelector class not found.");
        }
        if (inputSplitProvider == null) {
            throw new NullPointerException();
        }
        return inputSplitProvider;
    }

}
