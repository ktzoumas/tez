package org.apache.flink.tez.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfigurer;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import java.io.IOException;
import java.util.Arrays;


public class WordCount {

    public static int DOP = 1;

    public static final class CollectionDataSource extends DataSourceProcessor<String,GenericInputSplit> {


        public CollectionDataSource(ProcessorContext context) {
            super(context,
                    new CollectionInputFormat<String>(Arrays.asList("on on on hello hello at at at at roger roger roger"), new StringSerializer()),
                    new StringSerializer());
        }

        @Override
        public GenericInputSplit getSplit() {
            return new GenericInputSplit();
        }
    }

    public static final class FileDataSource extends DataSourceProcessor<String, FileInputSplit> {

        public FileDataSource(ProcessorContext context) {
            super(context,
                    new TextInputFormat(new Path("/tmp/hamlet.txt")),
                    new StringSerializer());
        }

        @Override
        public FileInputSplit getSplit() {
            return new FileInputSplit(0, new Path("/tmp/hamlet.txt"), 0, -1, null);
        }
    }

    public static final class FileDataSink extends DataSinkProcessor<Tuple2<String,Integer>> {

        public FileDataSink(ProcessorContext context) {
            super(context);
        }

        @Override
        public OutputFormat<Tuple2<String, Integer>> createOutputFormat() {
            TextOutputFormat<Tuple2<String,Integer>> format =
                    new TextOutputFormat<Tuple2<String, Integer>>(new Path("/tmp/wcout5"));
            format.setWriteMode(FileSystem.WriteMode.NO_OVERWRITE);
            return format;
        }

        @Override
        public TypeSerializer<Tuple2<String, Integer>> createTypeSerializer() {
            return new TupleSerializer<Tuple2<String, Integer>>(
                    (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class,
                    new TypeSerializer [] {
                            new StringSerializer(),
                            new IntSerializer()
                    });
        }
    }

    public static final class PrintingDataSink extends DataSinkProcessor<Tuple2<String,Integer>> {

        public PrintingDataSink(ProcessorContext context) {
            super(context);
        }

        @Override
        public OutputFormat<Tuple2<String, Integer>> createOutputFormat() {
            return new PrintingOutputFormat<Tuple2<String, Integer>>();
        }

        @Override
        public TypeSerializer<Tuple2<String, Integer>> createTypeSerializer() {
            return new TupleSerializer<Tuple2<String, Integer>>(
                    (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class,
                    new TypeSerializer [] {
                            new StringSerializer(),
                            new IntSerializer()
                    });
        }
    }


    public static class Tokenizer extends FlatMapProcessor<String, Tuple2<String, Integer>> {

        public Tokenizer(ProcessorContext context) {
            super (context,
                    new FlatMapFunction<String, Tuple2<String, Integer>>() {
                        @Override
                        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                            String[] tokens = value.toLowerCase().split("\\W+");

                            // emit the pairs
                            for (String token : tokens) {
                                if (token.length() > 0) {
                                    out.collect(new Tuple2<String, Integer>(token, 1));
                                }
                            }
                        }
                    },
                    new StringSerializer(),
                    new TupleSerializer<Tuple2<String, Integer>>(
                            (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class,
                            new TypeSerializer [] {
                                    new StringSerializer(),
                                    new IntSerializer()
                            }
                    )
            );
        }
    }

    public static class Summer extends ReduceProcessor<Tuple2<String, Integer>> {

        public Summer(ProcessorContext context) {
            super(context,
                    new ReduceFunction<Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                            return new Tuple2<String,Integer> (value1.f0, value1.f1 + value2.f1);
                        }
                    },
                    new TupleSerializer<Tuple2<String, Integer>>(
                            (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class,
                            new TypeSerializer [] {
                                    new StringSerializer(),
                                    new IntSerializer()
                            }
                    ),
                    new TupleComparator<Tuple2<String, Integer>>(
                            new int[] {0},
                            new TypeComparator [] {
                                new StringComparator(true),
                            },
                            new TypeSerializer[] {
                                new StringSerializer()
                            }
                    )
            );
        }

    }


    public static DAG createDAG (TezConfiguration tezConf) throws Exception {


        //DataSinkDescriptor dataSink = MROutput.createConfigurer(new Configuration(tezConf),
          //      TextOutputFormat.class, "/tmp/words.txt/").create();

        Vertex dataSource = new Vertex ("DataSource",
                new ProcessorDescriptor(CollectionDataSource.class.getName()), DOP);

        Vertex tokenizer = new Vertex ("TokenizerVertex",
                new ProcessorDescriptor(Tokenizer.class.getName()),DOP);

        Vertex summer = new Vertex("SumVertex",
                new ProcessorDescriptor(Summer.class.getName()), DOP);
                //.addDataSink("DataSink", dataSink);

        Vertex dataSink = new Vertex ("DataSink",
                new ProcessorDescriptor(FileDataSink.class.getName()), DOP);

        UnorderedKVEdgeConfigurer edgeConf = UnorderedKVEdgeConfigurer
                .newBuilder(LongWritable.class.getName(), BufferWritable.class.getName())
                .setFromConfiguration(tezConf)
                .build();

        UnorderedPartitionedKVEdgeConfigurer edgeConf2 = UnorderedPartitionedKVEdgeConfigurer
                .newBuilder(LongWritable.class.getName(), BufferWritable.class.getName(), HashPartitioner.class.getName())
                .setFromConfiguration(tezConf)
                .build();

        EdgeProperty edgeProperty1 = edgeConf.createDefaultOneToOneEdgeProperty();

        EdgeProperty edgeProperty2 = edgeConf2.createDefaultEdgeProperty();

        Edge edge1 = new Edge (dataSource, tokenizer, edgeProperty1);

        Edge edge2 = new Edge (tokenizer, summer, edgeProperty1);

        Edge edge3 = new Edge (summer, dataSink, edgeProperty1);

        DAG dag = new DAG ("WordCount");

        dag.addVertex(dataSource).addVertex(tokenizer).addVertex(summer).addVertex(dataSink)
                .addEdge(edge1).addEdge(edge2).addEdge(edge3);

        return dag;
    }

    public static void main (String [] args) {
        try {
            final TezConfiguration tezConf = new TezConfiguration();

            tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
            tezConf.set("fs.defaultFS", "file:///");
            tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

            TezClient tezClient = new TezClient("FlinkWordCount", tezConf);
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
                    System.out.println("FlinkWordCount failed with diagnostics: " + dagStatus.getDiagnostics());
                    System.exit(1);
                }
                System.out.println("FlinkWordCount finished successfully");
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
