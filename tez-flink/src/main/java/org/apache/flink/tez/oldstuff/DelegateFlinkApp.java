package org.apache.flink.tez.oldstuff;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.operators.MapDriver;
import org.apache.flink.tez.wordcount.TezTaskContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.*;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfigurer;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import java.io.IOException;
import java.util.Arrays;

public class DelegateFlinkApp {


    private static class TezCollector<T> implements Collector<T> {

        KeyValueWriter kvWriter;

        TypeSerializer<T> typeSerializer;

        private TezCollector(KeyValueWriter kvWriter, TypeSerializer<T> typeSerializer) {
            this.kvWriter = kvWriter;
            this.typeSerializer = typeSerializer;
        }

        @Override
        public void collect(T record) {
            HadoopSerializationDelegate<T> delegate = new HadoopSerializationDelegate<T>(typeSerializer);
            LongWritable length = new LongWritable(1);
            delegate.setInstance(record);
            try {
                kvWriter.write(length, delegate);
            }
            catch (Exception e) {
                System.err.println("Cannot collect " + record);
                e.printStackTrace();
                System.exit(1);
            }

        }

        @Override
        public void close() {

        }
    }


    private static class TezIterator<T> implements MutableObjectIterator<T> {

        KeyValueReader kvReader;
        TypeSerializer<T> typeSerializer;

        private TezIterator(KeyValueReader kvReader, TypeSerializer<T> typeSerializer) {
            this.kvReader = kvReader;
            this.typeSerializer = typeSerializer;
        }

        @Override
        public T next(T reuse) throws IOException {
            if (kvReader.next()) {
                Object value = kvReader.getCurrentValue();
                if (!(value instanceof HadoopSerializationDelegate)) {
                    throw new IllegalStateException("Value should be HadoopSerializationDelegate");
                }
                HadoopSerializationDelegate<T> delegate = (HadoopSerializationDelegate<T>) value;
               return delegate.getInstance();
            }
            else {
                return null;
            }
        }
    }

    public static final class DataSourceVertex extends SimpleProcessor {

        CollectionInputFormat<String> inputFormat;
        TezCollector<String> collector;

        public DataSourceVertex(ProcessorContext context) {
            super(context);
        }

        @Override
        public void run() throws Exception {
            inputFormat = new CollectionInputFormat(
                    Arrays.asList("one,", "two", "three", "four", "five"),
                    new StringSerializer());

            KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

            collector = new TezCollector<String>(kvWriter, new StringSerializer());

            inputFormat.open(new GenericInputSplit());

            String outValue = null;
            while (!inputFormat.reachedEnd()) {
                if ((outValue = inputFormat.nextRecord(outValue)) != null) {
                    collector.collect(outValue);
                    System.out.println (outValue);
                }
            }
        }
    }


    public static final class MapVertex extends SimpleProcessor {

        TezTaskContext<MapFunction<String,String>, String> mapContext;

        MapFunction<String,String> mapFunction;

        public MapVertex(ProcessorContext context) {
            super(context);
        }

        @Override
        public void run() throws Exception {

            mapFunction = new MapFunction<String, String>() {
                @Override
                public String map(String value) throws Exception {
                    return value.toUpperCase();
                }
            };
            mapContext = new TezTaskContext<MapFunction<String, String>, String>();
            mapContext.setUdf(mapFunction);
            mapContext.setCollector(new Collector<String>() {
                @Override
                public void collect(String record) {
                    System.out.println(record);
                }

                @Override
                public void close() {
                }
            });

            KeyValueReader kvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();

            StringSerializer stringSerializer = new StringSerializer();
            mapContext.setInput1(new TezIterator<String>(kvReader, stringSerializer), stringSerializer);

            MapDriver<String,String> mapDriver = new MapDriver<String, String>();
            mapDriver.setup(mapContext);

            mapDriver.run();

            Collector<String> strings = mapContext.getOutputCollector();
        }
    }


    public static DAG createDAG (TezConfiguration tezConf) throws Exception {

        DataSinkDescriptor dataSink = MROutput.createConfigurer(new Configuration(tezConf),
                TextOutputFormat.class, "/tmp/helloworldoutput12/").create();

        Vertex dataSourceVertex = new Vertex ("DataSource",
                new ProcessorDescriptor(DataSourceVertex.class.getName()),
                1);


        Vertex mapVertex = new Vertex ("Mapper",
                new ProcessorDescriptor(MapVertex.class.getName()),
                1);

        UnorderedKVEdgeConfigurer edgeConf = UnorderedKVEdgeConfigurer
                .newBuilder(LongWritable.class.getName(), HadoopSerializationDelegate.class.getName())
                .setFromConfiguration(tezConf)
                .build();

        EdgeProperty edgeProperty = edgeConf.createDefaultOneToOneEdgeProperty();


        Edge edge = new Edge (dataSourceVertex, mapVertex, edgeProperty);

        DAG dag = new DAG ("Source-Map");

        dag.addVertex(dataSourceVertex).addVertex(mapVertex).addEdge(edge);


        return dag;
    }

    public static void main (String [] args) {
        try {
            final TezConfiguration tezConf = new TezConfiguration();

            tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
            tezConf.set("fs.defaultFS", "file:///");
            tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

            TezClient tezClient = new TezClient("HelloWorldApp", tezConf);
            tezClient.start();


            try {
                DAG dag = createDAG(tezConf);

                tezClient.waitTillReady();
                System.out.println("Submitting DAG to Tez Client");
                DAGClient dagClient = tezClient.submitDAG(dag);
                System.out.println("Submitted DAG to Tez Client");

                // monitoring
                DAGStatus dagStatus = dagClient.waitForCompletion();

                if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
                    System.out.println("BufferFlinkApp failed with diagnostics: " + dagStatus.getDiagnostics());
                    System.exit(1);
                }
                System.out.println("BufferFlinkApp finished successfully");
                System.exit(0);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                tezClient.stop();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
