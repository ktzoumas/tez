package org.apache.flink.tez.processor;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.operators.udf.RuntimeUDFContext;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.tez.wordcount.ChannelSelector;
import org.apache.flink.tez.wordcount.ForwardingSelector;
import org.apache.flink.tez.wordcount.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.*;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class FlinkProcessor<S extends Function, OT> extends AbstractLogicalIOProcessor {

    private TaskContext<S,OT> task;
    protected Map<String, LogicalInput> inputs;
    protected Map<String, LogicalOutput> outputs;
    private List<KeyValueReader> readers;
    private List<KeyValueWriter> writers;
    private int numInputs;
    private int numOutputs;


    public FlinkProcessor(ProcessorContext context) {
        super(context);
    }

    @Override
    public void initialize() throws Exception {
        UserPayload payload = getContext().getUserPayload();
        Configuration conf = TezUtils.createConfFromUserPayload(payload);

        TaskConfig taskConfig = (TaskConfig) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.taskconfig"), getClass().getClassLoader());
        taskConfig.setTaskName(getContext().getTaskVertexName());
        Integer numberOfOutputSubTasks = (Integer) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.numberofoutputsubtasks"), getClass().getClassLoader());
        ChannelSelector<OT> channelSelector = (ChannelSelector<OT>) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.channelselector"), getClass().getClassLoader());

        RuntimeUDFContext runtimeUdfContext = new RuntimeUDFContext(getContext().getTaskVertexName(), getContext().getVertexParallelism(), getContext().getTaskIndex());

        this.task = new TaskContext<S, OT>(taskConfig, runtimeUdfContext, numberOfOutputSubTasks, channelSelector);
    }

    @Override
    public void handleEvents(List<Event> processorEvents) {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {

        // Initialize inputs, get readers and writers
        this.inputs = inputs;
        this.outputs = outputs;
        this.numInputs = inputs.size();
        this.numOutputs = outputs.size();
        this.readers = new ArrayList<KeyValueReader>(numInputs);
        this.writers = new ArrayList<KeyValueWriter>(numOutputs);
        if (this.inputs != null) {
            for (LogicalInput input : this.inputs.values()) {
                input.start();
                readers.add((KeyValueReader) input.getReader());
            }
        }
        if (this.outputs != null) {
            for (LogicalOutput output : this.outputs.values()) {
                output.start();
                writers.add((KeyValueWriter) output.getWriter());
            }
        }

        // Do the work
        task.invoke (readers, writers);
    }
}
