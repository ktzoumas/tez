package org.apache.flink.tez.runtime;


import com.google.common.base.Preconditions;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.runtime.operators.util.CloseableInputProvider;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FlinkDataSinkProcessor<IT> extends AbstractLogicalIOProcessor{

    // Tez stuff
    private TezTaskConfig config;
    protected Map<String, LogicalInput> inputs;
    private List<KeyValueReader> readers;
    private int numInputs;
    private TezRuntimeEnvironment runtimeEnvironment;
    AbstractInvokable invokable = new DummyInvokable();

    // Flink stuff
    private OutputFormat<IT> format;
    private ClassLoader userCodeClassLoader = this.getClass().getClassLoader();
    private CloseableInputProvider<IT> localStrategy;
    // input reader
    private MutableObjectIterator<IT> reader;
    // input iterator
    private MutableObjectIterator<IT> input;
    private TypeSerializerFactory<IT> inputTypeSerializerFactory;




    public FlinkDataSinkProcessor(ProcessorContext context) {
        super(context);
    }

    @Override
    public void initialize() throws Exception {
        UserPayload payload = getContext().getUserPayload();
        Configuration conf = TezUtils.createConfFromUserPayload(payload);

        this.config = (TezTaskConfig) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.taskconfig"), getClass().getClassLoader());
        config.setTaskName(getContext().getTaskVertexName());

        this.runtimeEnvironment = new TezRuntimeEnvironment();

        this.inputTypeSerializerFactory = this.config.getInputSerializer(0, this.userCodeClassLoader);

        initOutputFormat();
    }

    @Override
    public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {

        Preconditions.checkArgument((outputs == null) || (outputs.size() == 0));
        Preconditions.checkArgument(inputs.size() == 1);

        this.inputs = inputs;
        this.numInputs = inputs.size();
        this.readers = new ArrayList<KeyValueReader>(numInputs);
        if (this.inputs != null) {
            for (LogicalInput input: this.inputs.values()) {
                input.start();
                readers.add((KeyValueReader) input.getReader());
            }
        }

        this.reader = new TezReaderIterator<IT>(readers.get(0));

        this.invoke();
    }

    @Override
    public void handleEvents(List<Event> processorEvents) {

    }

    @Override
    public void close() throws Exception {

    }

    private void invoke () {
        try {
            // initialize local strategies
            switch (this.config.getInputLocalStrategy(0)) {
                case NONE:
                    // nothing to do
                    localStrategy = null;
                    input = reader;
                    break;
                case SORT:
                    // initialize sort local strategy
                    try {
                        // get type comparator
                        TypeComparatorFactory<IT> compFact = this.config.getInputComparator(0, this.userCodeClassLoader);
                        if (compFact == null) {
                            throw new Exception("Missing comparator factory for local strategy on input " + 0);
                        }

                        // initialize sorter
                        UnilateralSortMerger<IT> sorter = new UnilateralSortMerger<IT>(
                                this.runtimeEnvironment.getMemoryManager(),
                                this.runtimeEnvironment.getIOManager(),
                                this.reader, this.invokable, this.inputTypeSerializerFactory, compFact.createComparator(),
                                this.config.getRelativeMemoryInput(0), this.config.getFilehandlesInput(0),
                                this.config.getSpillingThresholdInput(0));

                        this.localStrategy = sorter;
                        this.input = sorter.getIterator();
                    } catch (Exception e) {
                        throw new RuntimeException("Initializing the input processing failed" +
                                e.getMessage() == null ? "." : ": " + e.getMessage(), e);
                    }
                    break;
                default:
                    throw new RuntimeException("Invalid local strategy for DataSinkTask");
            }

            final TypeSerializer<IT> serializer = this.inputTypeSerializerFactory.getSerializer();
            final MutableObjectIterator<IT> input = this.input;
            final OutputFormat<IT> format = this.format;


            IT record = serializer.createInstance();
            format.open (this.getContext().getTaskIndex(), this.getContext().getVertexParallelism());

            // work!
            while (((record = input.next(record)) != null)) {
                format.writeRecord(record);
            }

            this.format.close();
            this.format = null;
        }
        catch (IOException e) {
            throw new RuntimeException();
        }
        finally {
            if (this.format != null) {
                // close format, if it has not been closed, yet.
                // This should only be the case if we had a previous error, or were canceled.
                try {
                    this.format.close();
                }
                catch (Throwable t) {
                    //TODO log warning message
                }
            }
            // close local strategy if necessary
            if (localStrategy != null) {
                try {
                    this.localStrategy.close();
                } catch (Throwable t) {
                    //TODO log warning message
                }
            }
        }
    }

    private void initOutputFormat () {
        try {
            this.format = config.<OutputFormat<IT>>getStubWrapper(this.userCodeClassLoader).getUserCodeObject(OutputFormat.class, this.userCodeClassLoader);

            // check if the class is a subclass, if the check is required
            if (!OutputFormat.class.isAssignableFrom(this.format.getClass())) {
                throw new RuntimeException("The class '" + this.format.getClass().getName() + "' is not a subclass of '" +
                        OutputFormat.class.getName() + "' as is required.");
            }
        }
        catch (ClassCastException ccex) {
            throw new RuntimeException("The stub class is not a proper subclass of " + OutputFormat.class.getName(), ccex);
        }

        // configure the stub. catch exceptions here extra, to report them as originating from the user code
        try {
            this.format.configure(this.config.getStubParameters());
        }
        catch (Throwable t) {
            throw new RuntimeException("The user defined 'configure()' method in the Output Format caused an error: "
                    + t.getMessage(), t);
        }
    }

}
