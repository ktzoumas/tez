package org.apache.flink.tez.wordcount;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.ReduceDriver;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.Collector;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

public class ReduceProcessor<T> extends SimpleProcessor {

    ReduceFunction<T> function;
    TezTaskContext<ReduceFunction<T>, T> taskContext;
    TypeSerializer<T> typeSerializer;
    ReduceDriver<T> driver;
    KeyValueReader kvReader;
    KeyValueWriter kvWriter;
    Collector<T> collector;
    TypeComparator<T> comparator;

    public ReduceProcessor(ProcessorContext context, ReduceFunction<T> function, TypeSerializer<T> typeSerializer, TypeComparator<T> comparator) {
        super(context);
        this.function = function;
        this.typeSerializer = typeSerializer;
        this.comparator = comparator;

        this.taskContext = new TezTaskContext<ReduceFunction<T>, T>();
    }


    @Override
    public void run() throws Exception {

        taskContext.setUdf(function);

        taskContext.setComparator1(this.comparator);

        kvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();
        kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

        ReaderIterator<T> readerIterator = new ReaderIterator<T>(
                new MutableKeyValueReader<DeserializationDelegate<T>>(kvReader),
                typeSerializer
        );
        taskContext.setInput1(readerIterator, typeSerializer);
        taskContext.getTaskConfig().setDriverStrategy(DriverStrategy.SORTED_REDUCE);


        driver = new ReduceDriver<T>();
        driver.setup(taskContext);

        collector = new TezOutputCollector<T> (
                new TezRecordWriter<SerializationDelegate<T>>(kvWriter), typeSerializer);
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
            this.driver.cleanup();
        }
    }
}
