
package org.apache.flink.tez.wordcount_old;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.FlatMapDriver;
import org.apache.flink.tez.runtime.TezOutputCollector;
import org.apache.flink.tez.runtime.TezReaderIterator;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;


public class FlatMapProcessorImpl<IN, OUT> extends SimpleProcessor {

    FlatMapFunction<IN, OUT> function;
    TezTaskContext<FlatMapFunction<IN,OUT>, OUT> taskContext;
    TypeSerializer<IN> inTypeSerializer;
    TypeSerializer<OUT> outTypeSerializer;
    FlatMapDriver<IN, OUT> driver;
    KeyValueReader kvReader;
    KeyValueWriter kvWriter;
    Collector<OUT> collector;
    TypeComparator<OUT> outTypeComparator;

    public FlatMapProcessorImpl(ProcessorContext context) {
        super(context);
        this.taskContext = new TezTaskContext<FlatMapFunction<IN, OUT>, OUT>();
    }

    @Override
    public void initialize() throws Exception {
        super.initialize();
        UserPayload payload = getContext().getUserPayload();
        Configuration conf = TezUtils.createConfFromUserPayload(payload);
        this.function = (FlatMapFunction) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.udf"), getClass().getClassLoader());
        this.inTypeSerializer = (TypeSerializer<IN>) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.inSerializer"), getClass().getClassLoader());
        this.outTypeSerializer = (TypeSerializer<OUT>) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.outSerializer"), getClass().getClassLoader());
        this.outTypeComparator = (TypeComparator<OUT>) InstantiationUtil.readObjectFromConfig(conf.get("io.flink.processor.outTypeComparator"), getClass().getClassLoader());
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


        //PartitioningSelector<OUT> channelSelector =
        //        new PartitioningSelector<OUT>(this.outTypeComparator);

        ForwardingSelector<OUT> channelSelector =
                new ForwardingSelector<OUT>(getContext().getTaskIndex());

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

