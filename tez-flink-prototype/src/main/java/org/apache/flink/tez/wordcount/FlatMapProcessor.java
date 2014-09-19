package org.apache.flink.tez.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.ChannelSelector;
import org.apache.flink.runtime.operators.FlatMapDriver;
import org.apache.flink.runtime.operators.shipping.OutputEmitter;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
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
    TypeComparator<OUT> outTypeComparator;

    public FlatMapProcessor(ProcessorContext context, FlatMapFunction<IN, OUT> function, TypeSerializer<IN> inTypeSerializer, TypeSerializer<OUT> outTypeSerializer, TypeComparator<OUT> outTypeComparator) {
        super(context);
        this.function = function;
        this.taskContext = new TezTaskContext<FlatMapFunction<IN, OUT>, OUT>();
        this.inTypeSerializer = inTypeSerializer;
        this.outTypeSerializer = outTypeSerializer;
        this.outTypeComparator = outTypeComparator;
    }

    @Override
    public void run() throws Exception {

        taskContext.setUdf(function);
        kvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();
        kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

        //WritableSerializationDelegate.registerSerializer(inTypeSerializer);
        TezReaderIterator<IN> readerIterator = new TezReaderIterator<IN>(kvReader);
        taskContext.setInput1(readerIterator, inTypeSerializer);


        driver = new FlatMapDriver<IN, OUT>();
        driver.setup(taskContext);


        PartitioningSelector<OUT> channelSelector =
                 new PartitioningSelector<OUT>(this.outTypeComparator);
        int numOutputStreams = getContext().getVertexParallelism();
        collector = new TezOutputCollector<OUT>(kvWriter, channelSelector, outTypeSerializer, numOutputStreams);


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

            this.collector.close();
        }
    }

}
