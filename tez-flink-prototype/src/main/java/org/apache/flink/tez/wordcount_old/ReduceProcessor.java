package org.apache.flink.tez.wordcount_old;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.ReduceDriver;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.tez.examples.WordCount;
import org.apache.flink.tez.runtime.DummyInvokable;
import org.apache.flink.tez.runtime.TezOutputCollector;
import org.apache.flink.tez.runtime.TezReaderIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

public abstract class ReduceProcessor<T> extends SimpleProcessor {

    ReduceFunction<T> function;
    TezTaskContext<ReduceFunction<T>, T> taskContext;
    TypeSerializer<T> typeSerializer;
    ReduceDriver<T> driver;
    KeyValueReader kvReader;
    KeyValueWriter kvWriter;
    Collector<T> collector;
    TypeComparator<T> comparator;

    MemoryManager memoryManager;
    IOManager ioManager;
    TypeSerializerFactory<T> typeSerializerFactory;
    UnilateralSortMerger<T> sorter;


    public ReduceProcessor(ProcessorContext context) {
        super(context);
        this.function = createUdf();
        this.typeSerializerFactory = createTypeSerializerFactory();
        this.typeSerializer = typeSerializerFactory.getSerializer();
        this.comparator = createComparator();


        this.taskContext = new TezTaskContext<ReduceFunction<T>, T>();
        this.memoryManager = new DefaultMemoryManager(WordCount.SORTING_PAGES * WordCount.PAGE_SIZE, 1);
        this.ioManager = new IOManager();

    }

    public abstract ReduceFunction<T> createUdf ();

    public abstract TypeSerializerFactory<T> createTypeSerializerFactory ();

    public abstract TypeComparator<T> createComparator();


    @Override
    public void run() throws Exception {

        taskContext.setUdf(function);

        taskContext.setComparator1(this.comparator);

        kvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();
        kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

        //WritableSerializationDelegate.registerSerializer(typeSerializer);
        TezReaderIterator<T> readerIterator = new TezReaderIterator<T>(kvReader);

        sorter = new UnilateralSortMerger<T>(
                memoryManager,
                ioManager,
                readerIterator,
                new DummyInvokable(),
                typeSerializerFactory,
                comparator,
                0.3,
                100,
                0.8f
        );

        MutableObjectIterator<T> sortedIterator = sorter.getIterator();

        taskContext.setInput1(sortedIterator, typeSerializer);
        taskContext.getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_REDUCE);

        driver = new ReduceDriver<T>();
        driver.setup(taskContext);

        ForwardingSelector<T> channelSelector =
                new ForwardingSelector<T>(this.getContext().getTaskIndex());

        collector = new TezOutputCollector<T>(kvWriter, channelSelector, typeSerializer, 1);

        taskContext.setCollector(collector);

        try {
            // run the data preparation
            try {
                this.driver.prepare();
            }
            catch (Throwable t) {
                // if the preparation caused an error, clean up
                // errors during clean-up are swallowed, because we have already a root exception
                throw new Exception("The data preparation for task '" +
                        "' , caused an error: " + t.getMessage(), t);
            }

            // run the user code
            this.driver.run();
        }
        catch (Exception ex) {
            // close the input, but do not report any exceptions, since we already have another root cause
            ex.printStackTrace();
            System.exit(1);
        }
        finally {
            this.sorter.close();
            this.driver.cleanup();
            this.collector.close();
        }
    }
}
