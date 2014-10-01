package org.apache.flink.tez.runtime;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.operators.PactDriver;
import org.apache.flink.runtime.operators.udf.RuntimeUDFContext;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.flink.tez.util.ListUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.*;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
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

        TezTaskConfig taskConfig = (TezTaskConfig) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.taskconfig"), getClass().getClassLoader());
        taskConfig.setTaskName(getContext().getTaskVertexName());
        //Integer numberOfOutputSubTasks = (Integer) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.numberofoutputsubtasks"), getClass().getClassLoader());
        //ChannelSelector<OT> channelSelector = (ChannelSelector<OT>) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.channelselector"), getClass().getClassLoader());

        RuntimeUDFContext runtimeUdfContext = new RuntimeUDFContext(getContext().getTaskVertexName(), getContext().getVertexParallelism(), getContext().getTaskIndex());

        //this.task = new TaskContext<S, OT>(taskConfig, runtimeUdfContext, numberOfOutputSubTasks, channelSelector);
        this.task = new TaskContext<S, OT>(taskConfig, runtimeUdfContext);
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
        final Class<? extends PactDriver<S, OT>> driverClass = this.task.getTaskConfig().getDriver();
        PactDriver<S,OT> driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);
        this.numInputs = driver.getNumberOfInputs();
        this.numOutputs = outputs.size();


        this.readers = new ArrayList<KeyValueReader>(numInputs);
        ListUtils.ensureSize(readers, numInputs);
        HashMap<String, ArrayList<Integer>> inputPositions = ((TezTaskConfig) this.task.getTaskConfig()).getInputPositions();
        if (this.inputs != null) {
            for (String name : this.inputs.keySet()) {
                LogicalInput input = this.inputs.get(name);
                ArrayList<Integer> positions = inputPositions.get(name);
                for (Integer pos : positions) {
                    //int pos = inputPositions.get(name);
                    readers.set(pos, (KeyValueReader) input.getReader());
                }
            }
        }

        this.writers = new ArrayList<KeyValueWriter>(numOutputs);
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
