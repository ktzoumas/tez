package org.apache.flink.tez.runtime;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;


public class FlinkDataSourceProcessor<OT> extends AbstractLogicalIOProcessor {

    private TezTaskConfig config;
    protected Map<String, LogicalOutput> outputs;
    private List<KeyValueWriter> writers;
    private int numOutputs;
    private Collector<OT> collector;

    private InputFormat<OT, InputSplit> format;
    private TypeSerializerFactory<OT> serializerFactory;
    private InputSplitProvider inputSplitProvider;
    //private boolean taskCanceled = false;
    private ClassLoader userCodeClassLoader = getClass().getClassLoader();


    public FlinkDataSourceProcessor(ProcessorContext context) {
        super(context);
    }

    @Override
    public void initialize() throws Exception {
        UserPayload payload = getContext().getUserPayload();
        Configuration conf = TezUtils.createConfFromUserPayload(payload);

        this.config = (TezTaskConfig) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.taskconfig"), getClass().getClassLoader());
        config.setTaskName(getContext().getTaskVertexName());

        this.inputSplitProvider = config.getInputSplitProvider();
        this.serializerFactory = config.getOutputSerializer(this.userCodeClassLoader);

        initInputFormat();
    }

    @Override
    public void handleEvents(List<Event> processorEvents) {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {

        Preconditions.checkArgument((inputs == null) || (inputs.size() == 0));
        Preconditions.checkArgument(outputs.size() == 1);

        // Initialize inputs, get readers and writers
        this.outputs = outputs;
        this.numOutputs = outputs.size();
        this.writers = new ArrayList<KeyValueWriter>(numOutputs);
        if (this.outputs != null) {
            for (LogicalOutput output : this.outputs.values()) {
                output.start();
                writers.add((KeyValueWriter) output.getWriter());
            }
        }
        this.invoke();
    }


    private void invoke () {
        final TypeSerializer<OT> serializer = this.serializerFactory.getSerializer();
        try {
            final Iterator<InputSplit> splitIterator = getInputSplits();
            while (splitIterator.hasNext()) {
                final InputSplit split = splitIterator.next();
                OT record = serializer.createInstance();
                final InputFormat<OT, InputSplit> format = this.format;
                format.open(split);

                ChannelSelector<OT> channelSelector = null;
                final ShipStrategyType strategy = config.getOutputShipStrategy(0);
                final TypeComparatorFactory<OT> compFactory = config.getOutputComparator(0, this.userCodeClassLoader);
                final DataDistribution dataDist = config.getOutputDataDistribution(0, this.userCodeClassLoader);
                if (compFactory == null) {
                    channelSelector = new OutputEmitter<OT>(strategy);
                } else if (dataDist == null){
                    final TypeComparator<OT> comparator = compFactory.createComparator();
                    channelSelector = new OutputEmitter<OT>(strategy, comparator);
                } else {
                    final TypeComparator<OT> comparator = compFactory.createComparator();
                    channelSelector = new OutputEmitter<OT>(strategy, comparator, dataDist);
                }

                int numberOfOutputStreams = config.getNumberSubtasksInOutput();
                collector = new TezOutputCollector<OT>(writers.get(0), channelSelector, serializerFactory.getSerializer(), numberOfOutputStreams);
                while (!format.reachedEnd()) {
                    // build next pair and ship pair if it is valid
                    if ((record = format.nextRecord(record)) != null) {
                        collector.collect(record);
                    }
                }
                format.close();
            }
            collector.close();

        }
        catch (Exception ex) {
            // close the input, but do not report any exceptions, since we already have another root cause
            try {
                this.format.close();
            } catch (Throwable t) {}
        }
    }


    private void initInputFormat() {
        try {
            this.format = config.<InputFormat<OT, InputSplit>>getStubWrapper(this.userCodeClassLoader)
                    .getUserCodeObject(InputFormat.class, this.userCodeClassLoader);

            // check if the class is a subclass, if the check is required
            if (!InputFormat.class.isAssignableFrom(this.format.getClass())) {
                throw new RuntimeException("The class '" + this.format.getClass().getName() + "' is not a subclass of '" +
                        InputFormat.class.getName() + "' as is required.");
            }
        }
        catch (ClassCastException ccex) {
            throw new RuntimeException("The stub class is not a proper subclass of " + InputFormat.class.getName(),
                    ccex);
        }
        // configure the stub. catch exceptions here extra, to report them as originating from the user code
        try {
            this.format.configure(this.config.getStubParameters());
        }
        catch (Throwable t) {
            throw new RuntimeException("The user defined 'configure()' method caused an error: " + t.getMessage(), t);
        }
    }

    private Iterator<InputSplit> getInputSplits() {

        final InputSplitProvider provider = this.inputSplitProvider;

        return new Iterator<InputSplit>() {

            private InputSplit nextSplit;

            private boolean exhausted;

            @Override
            public boolean hasNext() {
                if (exhausted) {
                    return false;
                }

                if (nextSplit != null) {
                    return true;
                }

                InputSplit split = provider.getNextInputSplit();

                if (split != null) {
                    this.nextSplit = split;
                    return true;
                }
                else {
                    exhausted = true;
                    return false;
                }
            }

            @Override
            public InputSplit next() {
                if (this.nextSplit == null && !hasNext()) {
                    throw new NoSuchElementException();
                }

                final InputSplit tmp = this.nextSplit;
                this.nextSplit = null;
                return tmp;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}
