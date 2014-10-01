package org.apache.flink.tez.runtime;


import com.google.common.base.Preconditions;
import org.apache.flink.tez.input.FlinkUnorderedKVReader;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
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

public class FlinkUnionProcessor extends AbstractLogicalIOProcessor {

    private TezTaskConfig config;
    protected Map<String, LogicalInput> inputs;
    protected Map<String, LogicalOutput> outputs;
    private List<FlinkUnorderedKVReader> readers;
    private List<KeyValueWriter> writers;
    private int numInputs;
    private int numOutputs;

    public FlinkUnionProcessor(ProcessorContext context) {
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

        this.readers = new ArrayList<FlinkUnorderedKVReader>(numInputs);
        if (this.inputs != null) {
            for (LogicalInput input: this.inputs.values()) {
                input.start();
                readers.add((FlinkUnorderedKVReader) input.getReader());
            }
        }

        this.writers = new ArrayList<KeyValueWriter>(numOutputs);
        if (this.outputs != null) {
            for (LogicalOutput output : this.outputs.values()) {
                output.start();
                writers.add((KeyValueWriter) output.getWriter());
            }
        }

        Preconditions.checkArgument(writers.size() == 1);
        KeyValueWriter writer = writers.get(0);

        for (FlinkUnorderedKVReader reader: this.readers) {
            while (reader.next()) {
                Object key = reader.getCurrentKey();
                Object value = reader.getCurrentRawValue();
                writer.write(key, value);
            }
        }
    }
}
