package org.apache.flink.tez.runtime;

import com.google.common.base.Preconditions;
import org.apache.flink.tez.input.FlinkUnorderedKVReader;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class FlinkNoOpProcessor extends AbstractLogicalIOProcessor {

    private TezTaskConfig config;
    protected Map<String, LogicalInput> inputs;
    protected Map<String, LogicalOutput> outputs;
    private List<FlinkUnorderedKVReader> readers;
    private List<KeyValueWriter> writers;
    private int numInputs;
    private int numOutputs;

    public FlinkNoOpProcessor(ProcessorContext context) {
        super(context);
    }

    @Override
    public void initialize() throws Exception {
        UserPayload payload = getContext().getUserPayload();
        Configuration conf = TezUtils.createConfFromUserPayload(payload);

        this.config = (TezTaskConfig) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.taskconfig"), getClass().getClassLoader());
        config.setTaskName(getContext().getTaskVertexName());
    }

    @Override
    public void handleEvents(List<Event> processorEvents) {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
        this.inputs = inputs;
        this.outputs = outputs;
        this.numInputs = inputs.size();
        this.numOutputs = outputs.size();

        Preconditions.checkArgument(inputs.size() == 1);
        Preconditions.checkArgument(outputs.size() == 1);

        FlinkUnorderedKVReader reader = (FlinkUnorderedKVReader) inputs.values().iterator().next().getReader();
        KeyValueWriter writer = (KeyValueWriter) outputs.values().iterator().next().getWriter();

        while (reader.next()) {
            Object key = reader.getCurrentKey();
            Object value = reader.getCurrentRawValue();
            writer.write(key, value);
        }
    }
}
