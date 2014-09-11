package org.apache.flink.tez.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.FlatMapDriver;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.Collector;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;



public abstract class FlatMapProcessor<IN, OUT> extends SimpleProcessor {

    FlatMapFunction<IN, OUT> function;
    TezTaskContext<FlatMapFunction<IN,OUT>, OUT> taskContext;
    TypeSerializer<IN> inTypeSerializer;
    TypeSerializer<OUT> outTypeSerializer;
    FlatMapDriver<IN, OUT> driver;
    KeyValueReader kvReader;
    KeyValueWriter kvWriter;
    Collector<OUT> collector;

    public FlatMapProcessor(ProcessorContext context, FlatMapFunction<IN, OUT> function, TypeSerializer<IN> inTypeSerializer, TypeSerializer<OUT> outTypeSerializer) {
        super(context);
        this.function = function;
        this.taskContext = new TezTaskContext<FlatMapFunction<IN, OUT>, OUT>();
        this.inTypeSerializer = inTypeSerializer;
        this.outTypeSerializer = outTypeSerializer;
    }

    @Override
    public void run() throws Exception {
        taskContext.setUdf(function);
        kvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();
        kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

        ReaderIterator<IN> readerIterator = new ReaderIterator<IN>(
                new MutableKeyValueReader<DeserializationDelegate<IN>>(kvReader),
                inTypeSerializer
        );
        taskContext.setInput1(readerIterator, inTypeSerializer);

        driver = new FlatMapDriver<IN, OUT>();
        driver.setup(taskContext);

        collector = new TezOutputCollector<OUT>(new TezRecordWriter<SerializationDelegate<OUT>>(kvWriter), outTypeSerializer);
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
