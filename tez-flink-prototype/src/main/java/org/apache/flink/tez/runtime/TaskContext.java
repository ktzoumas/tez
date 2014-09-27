package org.apache.flink.tez.runtime;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FlatCombineFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.PactDriver;
import org.apache.flink.runtime.operators.PactTaskContext;
import org.apache.flink.runtime.operators.sort.CombiningUnilateralSortMerger;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.runtime.operators.udf.RuntimeUDFContext;
import org.apache.flink.runtime.operators.util.CloseableInputProvider;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;

import java.io.IOException;
import java.util.List;


public class TaskContext<S extends Function,OT>  implements PactTaskContext<S, OT> {

    protected static final Log LOG = LogFactory.getLog(TaskContext.class);

    AbstractInvokable invokable = new DummyInvokable();

    /**
     * The driver that invokes the user code (the stub implementation). The central driver in this task
     * (further drivers may be chained behind this driver).
     */
    protected volatile PactDriver<S, OT> driver;

    /**
     * The instantiated user code of this task's main operator (driver). May be null if the operator has no udf.
     */
    protected S stub;

    /**
     * The udf's runtime context.
     */
    protected RuntimeUDFContext runtimeUdfContext;

    /**
     * The collector that forwards the user code's results. May forward to a channel or to chained drivers within
     * this task.
     */
    protected Collector<OT> output;

    /**
     * The inputs reader, wrapped in an iterator. Prior to the local strategies, etc...
     */
    protected MutableObjectIterator<?>[] inputIterators;

    /**
     * The local strategies that are applied on the inputs.
     */
    protected volatile CloseableInputProvider<?>[] localStrategies;

    /**
     * The inputs to the operator. Return the readers' data after the application of the local strategy
     * and the temp-table barrier.
     */
    protected MutableObjectIterator<?>[] inputs;

    /**
     * The serializers for the input data type.
     */
    protected TypeSerializerFactory<?>[] inputSerializers;

    /**
     * The comparators for the central driver.
     */
    protected TypeComparator<?>[] inputComparators;

    /**
     * The task configuration with the setup parameters.
     */
    protected TaskConfig config;

    /**
     * The class loader used to instantiate user code and user data types.
     */
    protected ClassLoader userCodeClassLoader = getClass().getClassLoader();

    /**
     * The flag that tags the task as still running. Checked periodically to abort processing.
     */
    protected volatile boolean running = true;

    /*
     * Tez-specific variables given by the Processor
     */
    protected TypeSerializer<OT> outSerializer;

    protected int numberOfOutputTasks;

    protected ChannelSelector<OT> channelSelector;

    protected String taskName;

    protected int numberOfSubtasks;

    protected int indexInSubtaskGroup;

    TezRuntimeEnvironment runtimeEnvironment;


    public TaskContext(TaskConfig config, RuntimeUDFContext runtimeUdfContext, int numberOfOutputTasks, ChannelSelector<OT> channelSelector) {
        this.config = config;
        final Class<? extends PactDriver<S, OT>> driverClass = this.config.getDriver();
        this.driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);
        this.stub = this.config.<S>getStubWrapper(this.userCodeClassLoader).getUserCodeObject(Function.class, this.userCodeClassLoader); //TODO get superclass properly
        this.runtimeUdfContext = runtimeUdfContext;
        this.outSerializer = (TypeSerializer<OT>) this.config.getOutputSerializer(getClass().getClassLoader()).getSerializer();
        this.numberOfOutputTasks = numberOfOutputTasks;
        this.channelSelector = channelSelector;
        this.taskName = this.config.getTaskName();
        this.numberOfSubtasks = this.runtimeUdfContext.getNumberOfParallelSubtasks();
        this.indexInSubtaskGroup = this.runtimeUdfContext.getIndexOfThisSubtask();
        this.runtimeEnvironment = new TezRuntimeEnvironment(512, 32768);
    }

    //-------------------------------------------------------------
    // Setters needed to initialize this object properly
    //-------------------------------------------------------------

    /*
    public void setOutSerializer(TypeSerializer<OT> outSerializer) {
        this.outSerializer = outSerializer;
    }

    public void setNumberOfOutputTasks(int numberOfOutputTasks) {
        this.numberOfOutputTasks = numberOfOutputTasks;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public void setNumberOfSubtasks(int numberOfSubtasks) {
        this.numberOfSubtasks = numberOfSubtasks;
    }

    public void setIndexInSubtaskGroup(int indexInSubtaskGroup) {
        this.indexInSubtaskGroup = indexInSubtaskGroup;
    }

    public void setChannelSelector(ChannelSelector<OT> channelSelector) {
        this.channelSelector = channelSelector;
    }

    public void setTaskConfig (TaskConfig config) {
        this.config = config;
    }

    public void setUserCodeClassLoader (ClassLoader userCodeClassLoader) {
        this.userCodeClassLoader = userCodeClassLoader;
    }

    public void setStub(S stub) {
        this.stub = stub;
    }

    public void setInputSerializers(TypeSerializerFactory<?>[] inputSerializers) {
        this.inputSerializers = inputSerializers;
    }

    public void setInputComparators(TypeComparator<?>[] inputComparators) {
        this.inputComparators = inputComparators;
    }

    public void setConfig(TaskConfig config) {
        this.config = config;
    }
    */

    //-------------------------------------------------------------
    // Interface to Processor
    //-------------------------------------------------------------

    public void invoke(List<KeyValueReader> readers, List<KeyValueWriter> writers) throws Exception {

        // whatever happens in this scope, make sure that the local strategies are cleaned up!
        // note that the initialization of the local strategies is in the try-finally block as well,
        // so that the thread that creates them catches its own errors that may happen in that process.
        // this is especially important, since there may be asynchronous closes (such as through canceling).
        try {
            // initialize the inputs and outputs
            initInputsOutputs(readers, writers);

            // pre main-function initialization
            initialize();

            // the work goes here
            run();
        }
        finally {
            // clean up in any case!
            closeLocalStrategies();
        }

        if (this.running) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(formatLogString("Finished task code."));
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(formatLogString("Task code cancelled."));
            }
        }
    }


    /*
     * Initialize inputs, input serializers, input comparators, and collector
     * Assumes that the config and userCodeClassLoader has been set
     */
    private void initInputsOutputs (List<KeyValueReader> readers, List<KeyValueWriter> writers) throws Exception {

        int numInputs = readers.size();
        Preconditions.checkArgument(numInputs == driver.getNumberOfInputs());

        // Prior to local strategies
        this.inputIterators = new MutableObjectIterator[numInputs];
        //local strategies
        this.localStrategies = new CloseableInputProvider[numInputs];
        // After local strategies
        this.inputs = new MutableObjectIterator[numInputs];

        initInputsSerializersAndComparators(numInputs);

        int index = 0;
        for (KeyValueReader reader : readers) {
            this.inputIterators[index] = new TezReaderIterator<Object>(reader);
            initInputLocalStrategy(index);

        }

        // For now, only one writer allowed
        Preconditions.checkArgument(writers.size() == 1);
        KeyValueWriter writer = writers.get(0);
        this.output = new TezOutputCollector<OT>(writer, channelSelector, outSerializer, numberOfOutputTasks);
    }



    // --------------------------------------------------------------------
    // PactTaskContext interface
    // --------------------------------------------------------------------

    @Override
    public TaskConfig getTaskConfig() {
        return (TaskConfig) this.config;
    }

    @Override
    public ClassLoader getUserCodeClassLoader() {
        return this.userCodeClassLoader;
    }

    @Override
    public MemoryManager getMemoryManager() {
        return runtimeEnvironment.getMemoryManager();
    }

    @Override
    public IOManager getIOManager() {
        return runtimeEnvironment.getIOManager();
    }

    @Override
    public <X> MutableObjectIterator<X> getInput(int index) {
        if (index < 0 || index > this.driver.getNumberOfInputs()) {
            throw new IndexOutOfBoundsException();
        }
        // check for lazy assignment from input strategies
        if (this.inputs[index] != null) {
            @SuppressWarnings("unchecked")
            MutableObjectIterator<X> in = (MutableObjectIterator<X>) this.inputs[index];
            return in;
        } else {
            final MutableObjectIterator<X> in;
            try {
                if (this.localStrategies[index] != null) {
                    @SuppressWarnings("unchecked")
                    MutableObjectIterator<X> iter = (MutableObjectIterator<X>) this.localStrategies[index].getIterator();
                    in = iter;
                } else {
                    throw new RuntimeException("Bug: null input iterator, null temp barrier, and null local strategy.");
                }
                this.inputs[index] = in;
                return in;
            } catch (InterruptedException iex) {
                throw new RuntimeException("Interrupted while waiting for input " + index + " to become available.");
            } catch (IOException ioex) {
                throw new RuntimeException("An I/O Exception occurred whily obaining input " + index + ".");
            }
        }
    }

    @Override
    public <X> TypeSerializerFactory<X> getInputSerializer(int index) {
        if (index < 0 || index >= this.driver.getNumberOfInputs()) {
            throw new IndexOutOfBoundsException();
        }

        @SuppressWarnings("unchecked")
        final TypeSerializerFactory<X> serializerFactory = (TypeSerializerFactory<X>) this.inputSerializers[index];
        return serializerFactory;
    }

    @Override
    public <X> TypeComparator<X> getInputComparator(int index) {
        if (this.inputComparators == null) {
            throw new IllegalStateException("Comparators have not been created!");
        }
        else if (index < 0 || index >= this.driver.getNumberOfInputs()) {
            throw new IndexOutOfBoundsException();
        }

        @SuppressWarnings("unchecked")
        final TypeComparator<X> comparator = (TypeComparator<X>) this.inputComparators[index];
        return comparator;
    }

    @Override
    public S getStub() {
        return this.stub;
    }

    @Override
    public Collector<OT> getOutputCollector() {
        return this.output;
    }

    @Override
    public AbstractInvokable getOwningNepheleTask() {
        return this.invokable;
    }

    @Override
    public String formatLogString(String message) {
        return null;
    }


    // --------------------------------------------------------------------
    // Adapted from RegularPactTask
    // --------------------------------------------------------------------

    private void initInputLocalStrategy(int inputNum) throws Exception {
        // check if there is already a strategy
        if (this.localStrategies[inputNum] != null) {
            throw new IllegalStateException();
        }

        // now set up the local strategy
        final LocalStrategy localStrategy = this.config.getInputLocalStrategy(inputNum);
        if (localStrategy != null) {
            switch (localStrategy) {
                case NONE:
                    // the input is as it is
                    this.inputs[inputNum] = this.inputIterators[inputNum];
                    break;
                case SORT:
                    @SuppressWarnings({ "rawtypes", "unchecked" })
                    UnilateralSortMerger<?> sorter = new UnilateralSortMerger(getMemoryManager(), getIOManager(),
                            this.inputIterators[inputNum], this.invokable, this.inputSerializers[inputNum], getLocalStrategyComparator(inputNum),
                            this.config.getRelativeMemoryInput(inputNum), this.config.getFilehandlesInput(inputNum),
                            this.config.getSpillingThresholdInput(inputNum));
                    // set the input to null such that it will be lazily fetched from the input strategy
                    this.inputs[inputNum] = null;
                    this.localStrategies[inputNum] = sorter;
                    break;
                case COMBININGSORT:
                    // sanity check this special case!
                    // this still breaks a bit of the abstraction!
                    // we should have nested configurations for the local strategies to solve that
                    if (inputNum != 0) {
                        throw new IllegalStateException("Performing combining sort outside a (group)reduce task!");
                    }

                    // instantiate ourselves a combiner. we should not use the stub, because the sort and the
                    // subsequent (group)reduce would otherwise share it multi-threaded
                    final Class<S> userCodeFunctionType = this.driver.getStubType();
                    if (userCodeFunctionType == null) {
                        throw new IllegalStateException("Performing combining sort outside a reduce task!");
                    }
                    final S localStub;
                    try {
                        localStub = initStub(userCodeFunctionType);
                    } catch (Exception e) {
                        throw new RuntimeException("Initializing the user code and the configuration failed" +
                                e.getMessage() == null ? "." : ": " + e.getMessage(), e);
                    }

                    if (!(localStub instanceof FlatCombineFunction)) {
                        throw new IllegalStateException("Performing combining sort outside a reduce task!");
                    }

                    @SuppressWarnings({ "rawtypes", "unchecked" })
                    CombiningUnilateralSortMerger<?> cSorter = new CombiningUnilateralSortMerger(
                            (FlatCombineFunction) localStub, getMemoryManager(), getIOManager(), this.inputIterators[inputNum],
                            this.invokable, this.inputSerializers[inputNum], getLocalStrategyComparator(inputNum),
                            this.config.getRelativeMemoryInput(inputNum), this.config.getFilehandlesInput(inputNum),
                            this.config.getSpillingThresholdInput(inputNum));
                    cSorter.setUdfConfiguration(this.config.getStubParameters());

                    // set the input to null such that it will be lazily fetched from the input strategy
                    this.inputs[inputNum] = null;
                    this.localStrategies[inputNum] = cSorter;
                    break;
                default:
                    throw new Exception("Unrecognized local strategy provided: " + localStrategy.name());
            }
        } else {
            // no local strategy in the config
            this.inputs[inputNum] = this.inputIterators[inputNum];
        }
    }

    private <T> TypeComparator<T> getLocalStrategyComparator(int inputNum) throws Exception {
        TypeComparatorFactory<T> compFact = this.config.getInputComparator(inputNum, this.userCodeClassLoader);
        if (compFact == null) {
            throw new Exception("Missing comparator factory for local strategy on input " + inputNum);
        }
        return compFact.createComparator();
    }

    protected S initStub(Class<? super S> stubSuperClass) throws Exception {
        try {
            S stub = config.<S>getStubWrapper(this.userCodeClassLoader).getUserCodeObject(stubSuperClass, this.userCodeClassLoader);
            // check if the class is a subclass, if the check is required
            if (stubSuperClass != null && !stubSuperClass.isAssignableFrom(stub.getClass())) {
                throw new RuntimeException("The class '" + stub.getClass().getName() + "' is not a subclass of '" +
                        stubSuperClass.getName() + "' as is required.");
            }
            FunctionUtils.setFunctionRuntimeContext(stub, this.runtimeUdfContext);
            return stub;
        }
        catch (ClassCastException ccex) {
            throw new Exception("The stub class is not a proper subclass of " + stubSuperClass.getName(), ccex);
        }
    }

    /**
     * Creates all the serializers and comparators.
     */
    protected void initInputsSerializersAndComparators(int numInputs) throws Exception {
        this.inputSerializers = new TypeSerializerFactory<?>[numInputs];
        this.inputComparators = this.driver.requiresComparatorOnInput() ? new TypeComparator[numInputs] : null;
        this.inputIterators = new MutableObjectIterator[numInputs];

        for (int i = 0; i < numInputs; i++) {
            //  ---------------- create the serializer first ---------------------
            final TypeSerializerFactory<?> serializerFactory = this.config.getInputSerializer(i, this.userCodeClassLoader);
            this.inputSerializers[i] = serializerFactory;

            //  ---------------- create the driver's comparator ---------------------
            if (this.inputComparators != null) {
                final TypeComparatorFactory<?> comparatorFactory = this.config.getDriverComparator(i, this.userCodeClassLoader);
                this.inputComparators[i] = comparatorFactory.createComparator();
            }
        }
    }

    protected void initialize() throws Exception {
        // create the operator
        try {
            this.driver.setup(this);
        }
        catch (Throwable t) {
            throw new Exception("The driver setup for '" + //TODO put taks name here
                    "' , caused an error: " + t.getMessage(), t);
        }

        this.runtimeUdfContext = createRuntimeContext();

        // instantiate the UDF
        try {
            final Class<? super S> userCodeFunctionType = this.driver.getStubType();
            // if the class is null, the driver has no user code
            if (userCodeFunctionType != null) {
                this.stub = initStub(userCodeFunctionType);
            }
        } catch (Exception e) {
            throw new RuntimeException("Initializing the UDF" +
                    e.getMessage() == null ? "." : ": " + e.getMessage(), e);
        }
    }

    public RuntimeUDFContext createRuntimeContext() {
        return new RuntimeUDFContext(this.taskName, this.numberOfSubtasks, this.indexInSubtaskGroup, null);
    }

    protected void closeLocalStrategies() {
        if (this.localStrategies != null) {
            for (int i = 0; i < this.localStrategies.length; i++) {
                if (this.localStrategies[i] != null) {
                    try {
                        this.localStrategies[i].close();
                    } catch (Throwable t) {
                        LOG.error("Error closing local strategy for input " + i, t);
                    }
                }
            }
        }
    }

    protected void run() throws Exception {
        // ---------------------------- Now, the actual processing starts ------------------------
        // check for asynchronous canceling
        if (!this.running) {
            return;
        }

        boolean stubOpen = false;

        try {
            // run the data preparation
            try {
                this.driver.prepare();
            }
            catch (Throwable t) {
                // if the preparation caused an error, clean up
                // errors during clean-up are swallowed, because we have already a root exception
                throw new Exception("The data preparation for task '" + this.taskName +
                        "' , caused an error: " + t.getMessage(), t);
            }

            // check for canceling
            if (!this.running) {
                return;
            }

            // open stub implementation
            if (this.stub != null) {
                try {
                    Configuration stubConfig = this.config.getStubParameters();
                    FunctionUtils.openFunction(this.stub, stubConfig);
                    stubOpen = true;
                }
                catch (Throwable t) {
                    throw new Exception("The user defined 'open()' method caused an exception: " + t.getMessage(), t);
                }
            }

            // run the user code
            this.driver.run();

            // close. We close here such that a regular close throwing an exception marks a task as failed.
            if (this.running && this.stub != null) {
                FunctionUtils.closeFunction(this.stub);
                stubOpen = false;
            }

            this.output.close();

        }
        catch (Exception ex) {
            // close the input, but do not report any exceptions, since we already have another root cause
            if (stubOpen) {
                try {
                    FunctionUtils.closeFunction(this.stub);
                }
                catch (Throwable t) {}
            }
        }
        finally {
            this.driver.cleanup();
        }
    }

}
