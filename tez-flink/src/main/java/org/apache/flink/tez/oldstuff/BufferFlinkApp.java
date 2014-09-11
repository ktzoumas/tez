package org.apache.flink.tez.oldstuff;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.operators.MapDriver;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
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

public class BufferFlinkApp {


    private static class TezCollector<T> implements Collector<T> {

        RecordSerializer<SerializationDelegate<T>> serializer;

        KeyValueWriter kvWriter;

        SerializationDelegate<T> delegate;

        private TezCollector (KeyValueWriter kvWriter, TypeSerializer<T> typeSerializer) {
            this.kvWriter = kvWriter;
            serializer = new SpanningRecordSerializer<SerializationDelegate<T>>();
            delegate = new SerializationDelegate<T>(typeSerializer);
            serializer.clear();
        }

        @Override
        public void collect (T record) {
            try {
                delegate.setInstance(record);
                RecordSerializer.SerializationResult result = serializer.addRecord(delegate);
                while (result.isFullBuffer()) {
                    Buffer buffer = serializer.getCurrentBuffer();
                    if (buffer != null) {
                        LongWritable length = new LongWritable(buffer.size());
                        kvWriter.write(length, new OldBufferWritable(buffer));
                    }
                    serializer.clear();
                    result = serializer.setNextBuffer(buffer);
                }


            }
            catch (Exception e) {
                System.err.println("Cannot collect " + record);
                e.printStackTrace();
                System.exit(1);
            }
        }

        @Override
        public void close() { }
    }


    private static class TezIterator<T> implements MutableObjectIterator<T> {

        KeyValueReader kvReader;
        DeserializationDelegate<T> delegate;
        RecordDeserializer<DeserializationDelegate<T>> deserializer;

        private TezIterator(KeyValueReader kvReader, TypeSerializer<T> typeSerializer) {
            this.kvReader = kvReader;
            deserializer = new AdaptiveSpanningRecordDeserializer<DeserializationDelegate<T>>();
            delegate = new DeserializationDelegate<T>(typeSerializer);
        }

        @Override
        public T next(T reuse) throws IOException {
            if (kvReader.next()) {
                Object key = kvReader.getCurrentKey();
                Object value = kvReader.getCurrentValue();
                if (!(value instanceof OldBufferWritable && key instanceof LongWritable))
                    throw new IllegalStateException("Value should be OldBufferWritable, key should be LongWritable");
                Buffer buffer = ((OldBufferWritable) value).getBuffer();
                LongWritable length = (LongWritable) key;
                if (buffer.size() != length.get())
                    throw new IllegalStateException("Length is not correct");
                deserializer.getNextRecord(delegate);
                T record = delegate.getInstance();
                return record;
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
                }
            }
            collector.close();
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
                .newBuilder(LongWritable.class.getName(), OldBufferWritable.class.getName())
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
