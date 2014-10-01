package org.apache.flink.tez.examples_old;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.io.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.io.network.bufferprovider.GlobalBufferPool;
import org.apache.flink.tez.input.FlinkUnorderedKVEdgeConfig;
import org.apache.flink.tez.input.FlinkUnorderedPartitionedKVEdgeConfig;
import org.apache.flink.tez.input.WritableSerializationDelegate;
import org.apache.flink.tez.runtime.DataSinkProcessor;
import org.apache.flink.tez.runtime.SimplePartitioner;
import org.apache.flink.tez.runtime.SingleSplitDataSourceProcessor;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.flink.tez.wordcount_old.*;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.dag.api.*;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;

import java.util.Arrays;


public class WordCount extends ProgramLauncher {

    public static int DOP = 4;

    public static int BUF_COUNTER = 0;

    public static int PAGE_SIZE = 32768;

    public static int SORTING_PAGES = 1024;

    public static int TOTAL_NETWORK_PAGES = 1024;

    public static int TASK_NETWORK_PAGES = 1024;

    public static GlobalBufferPool GLOBAL_BUFFER_POOL;

    //public static String INPUT_FILE="hdfs://localhost:9000/tmp/hamlet.txt";

    //public static String OUTPUT_FILE="hdfs://localhost:9000/tmp/job_output4";

    public static String INPUT_FILE="hdfs://localhost:9000/tmp/hamlet.txt";

    public static String OUTPUT_FILE="hdfs://localhost:9000/tmp/job_output8";

    static {
        GLOBAL_BUFFER_POOL = new GlobalBufferPool(TOTAL_NETWORK_PAGES, PAGE_SIZE);
    }

    public static final class CollectionDataSource extends SingleSplitDataSourceProcessor<String,GenericInputSplit> {

        public CollectionDataSource(ProcessorContext context) {
            super(context);
        }

        @Override
        public GenericInputSplit getSplit() {
            return new GenericInputSplit();
        }

        @Override
        public InputFormat<String, GenericInputSplit> createInputFormat() {
            return new CollectionInputFormat<String>(Arrays.asList("on on on on hello hello at at at at roger roger roger roger roger really really really"), new StringSerializer());
        }

        @Override
        public TypeSerializer<String> createTypeSerializer() {
            return new StringSerializer();
        }
    }

    public static final class FileSingleSplitDataSource extends SingleSplitDataSourceProcessor<String, FileInputSplit> {

        public FileSingleSplitDataSource(ProcessorContext context) {
            super(context);
        }

        @Override
        public FileInputSplit getSplit() {
            return new FileInputSplit(0, new Path(INPUT_FILE), 0, -1, null);
        }

        @Override
        public InputFormat<String, FileInputSplit> createInputFormat() {
            return new TextInputFormat(new Path(INPUT_FILE));
        }

        @Override
        public TypeSerializer<String> createTypeSerializer() {
            return new StringSerializer();
        }
    }

    public static final class FileDataSink extends DataSinkProcessor<Tuple2<String,Integer>> {

        public FileDataSink(ProcessorContext context) {
            super(context);
        }

        @Override
        public OutputFormat<Tuple2<String, Integer>> createOutputFormat() {
            TextOutputFormat<Tuple2<String, Integer>> format =
                    new TextOutputFormat<Tuple2<String, Integer>>(new Path(OUTPUT_FILE));
            format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
            format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
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

    public static final class TextDataSink extends DataSinkProcessor<String> {

        public TextDataSink(ProcessorContext context) {
            super(context);
        }

        @Override
        public OutputFormat<String> createOutputFormat() {
            TextOutputFormat<String> format =
                    new TextOutputFormat<String>(new Path(OUTPUT_FILE));
            format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
            format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);
            return format;
        }

        @Override
        public TypeSerializer<String> createTypeSerializer() {
            return new StringSerializer();
        }
    }

    /*
    public static final class CsvDataSink extends DataSinkProcessor<Tuple2<String,Integer>> {

        public CsvDataSink(ProcessorContext context) {
            super(context);
        }

        @Override
        public OutputFormat<Tuple2<String, Integer>> createOutputFormat() {
            CsvOutputFormat<Tuple2<String,Integer>> format =
                    new CsvOutputFormat<Tuple2<String, Integer>>()
            return null;
        }

        @Override
        public TypeSerializer<Tuple2<String, Integer>> createTypeSerializer() {
            return null;
        }
    }
    */

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


    public static final class Tokenizer extends FlatMapProcessor<String, Tuple2<String, Integer>> {

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
                    ),
                    new TupleComparator<Tuple2<String, Integer>>(
                            new int [] {0},
                            new TypeComparator[] {new StringComparator(true)},
                            new TypeSerializer[] {new StringSerializer()}
                    )
            );
        }
    }

    public static final class Summer extends ReduceProcessor<Tuple2<String, Integer>> {

        public Summer(ProcessorContext context) {
            super(context);
        }

        @Override
        public ReduceFunction<Tuple2<String, Integer>> createUdf() {
            return new ReduceFunction<Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                    return new Tuple2<String,Integer> (value1.f0, value1.f1 + value2.f1);
                }
            };
        }

        @Override
        public TypeSerializerFactory<Tuple2<String, Integer>> createTypeSerializerFactory() {
            return new TypeSerializerFactory<Tuple2<String, Integer>>() {
                @Override
                public void writeParametersToConfig(Configuration config) { }

                @Override
                public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {}

                @Override
                public TypeSerializer<Tuple2<String, Integer>> getSerializer() {
                    return new TupleSerializer<Tuple2<String, Integer>>(
                            (Class<Tuple2<String, Integer>>) (Class<?>) Tuple2.class,
                            new TypeSerializer[]{
                                    new StringSerializer(),
                                    new IntSerializer()
                            }
                    );
                }

                @Override
                public Class<Tuple2<String, Integer>> getDataType() {
                    return (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class;
                }
            };
        }

        @Override
        public TypeComparator<Tuple2<String, Integer>> createComparator() {
            return new TupleComparator<Tuple2<String, Integer>>(
                    new int[] {0},
                    new TypeComparator [] {
                            new StringComparator(true),
                    },
                    new TypeSerializer[] {
                            new StringSerializer(),
                            new IntSerializer()
                    }
            );
        }
    }


    @Override
    public DAG createDAG (TezConfiguration tezConf) throws Exception {


        Vertex dataSource = Vertex.create("DataSource",
                ProcessorDescriptor.create(FileSingleSplitDataSource.class.getName()), DOP);

        Vertex tokenizer = Vertex.create("TokenizerVertex",
                ProcessorDescriptor.create(Tokenizer.class.getName()), DOP);

        Vertex summer = Vertex.create ("SumVertex",
                ProcessorDescriptor.create(Summer.class.getName()), DOP);

        Vertex dataSink = Vertex.create ("DataSink",
                ProcessorDescriptor.create (FileDataSink.class.getName()), DOP);

        FlinkUnorderedKVEdgeConfig srcMapEdgeConf = (FlinkUnorderedKVEdgeConfig) (FlinkUnorderedKVEdgeConfig
                .newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName())
                .setFromConfiguration(tezConf)
                .configureInput()
                .setAdditionalConfiguration("io.flink.typeserializer", InstantiationUtil.writeObjectToConfig(
                        new StringSerializer()
                )))
                .done()
                .build();

        FlinkUnorderedPartitionedKVEdgeConfig mapReduceEdgeConf = (FlinkUnorderedPartitionedKVEdgeConfig) (FlinkUnorderedPartitionedKVEdgeConfig
                .newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName(),
                        SimplePartitioner.class.getName())
                .setFromConfiguration(tezConf)
                .setAdditionalConfiguration(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, "true")
                .configureInput()
                .setAdditionalConfiguration("io.flink.typeserializer", InstantiationUtil.writeObjectToConfig(
                        new TupleSerializer<Tuple2<String, Integer>>(
                                (Class<Tuple2<String, Integer>>) (Class<?>) Tuple2.class,
                                new TypeSerializer[]{
                                        new StringSerializer(),
                                        new IntSerializer()
                                }
                        ))))
                .done()
                .build();




        FlinkUnorderedKVEdgeConfig reduceSinkConf = (FlinkUnorderedKVEdgeConfig) (FlinkUnorderedKVEdgeConfig
                .newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName())
                .setFromConfiguration(tezConf)
                .configureInput()
                .setAdditionalConfiguration("io.flink.typeserializer", InstantiationUtil.writeObjectToConfig(
                new TupleSerializer<Tuple2<String, Integer>>(
                        (Class<Tuple2<String, Integer>>) (Class<?>) Tuple2.class,
                        new TypeSerializer[]{
                                new StringSerializer(),
                                new IntSerializer()
                        }
                ))))
                .done()
                .build();



        EdgeProperty srcMapEdgeProperty = srcMapEdgeConf.createDefaultOneToOneEdgeProperty();

        EdgeProperty mapReduceEdgeProperty = mapReduceEdgeConf.createDefaultEdgeProperty();

        EdgeProperty reduceSinkEdgeProperty = reduceSinkConf.createDefaultOneToOneEdgeProperty();

        Edge edge1 = Edge.create (dataSource, tokenizer, srcMapEdgeProperty);

        Edge edge2 = Edge.create(tokenizer, summer, mapReduceEdgeProperty);

        Edge edge3 = Edge.create (summer, dataSink, reduceSinkEdgeProperty);

        DAG dag = DAG.create ("WordCount");

        dag.addVertex(dataSource)
                .addVertex(tokenizer)
                .addVertex(summer)
                .addVertex(dataSink)
                .addEdge(edge1)
                .addEdge(edge2)
                .addEdge(edge3);

        return dag;
    }

    public static DAG createMapDAG (TezConfiguration tezConf) throws Exception {

        Vertex dataSource = Vertex.create("DataSource",
                ProcessorDescriptor.create(FileSingleSplitDataSource.class.getName()), DOP);

        Vertex tokenizer = Vertex.create("TokenizerVertex",
                ProcessorDescriptor.create(Tokenizer.class.getName()), DOP);

        Vertex dataSink = Vertex.create("DataSink",
                ProcessorDescriptor.create(FileDataSink.class.getName()), DOP);

        UnorderedKVEdgeConfig edgeConf = UnorderedKVEdgeConfig
                .newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName())
                .setFromConfiguration(tezConf)
                .setAdditionalConfiguration("io.flink.typeserializer", InstantiationUtil.writeObjectToConfig(
                        new StringSerializer()
                ))
                .build();

        UnorderedPartitionedKVEdgeConfig edgeConf2 = UnorderedPartitionedKVEdgeConfig
                .newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName(),
                        SimplePartitioner.class.getName())
                .setFromConfiguration(tezConf)
                .setAdditionalConfiguration(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, "true")
                .build();

        EdgeProperty edgeProperty1 = edgeConf.createDefaultOneToOneEdgeProperty();

        EdgeProperty edgeProperty2 = edgeConf2.createDefaultEdgeProperty();

        Edge edge1 = Edge.create (dataSource, tokenizer, edgeProperty1);

        Edge edge3 = Edge.create (tokenizer, dataSink, edgeProperty2);

        DAG dag = DAG.create ("Tokenizer");

        dag.addVertex(dataSource)
                .addVertex(tokenizer)
                .addVertex(dataSink)
                .addEdge(edge1)
                .addEdge(edge3);

        return dag;
    }

    public static DAG createNoOpDAG (TezConfiguration tezConf) throws Exception {

        Vertex dataSource = Vertex.create("DataSource",
                ProcessorDescriptor.create(FileSingleSplitDataSource.class.getName()), DOP);

        Vertex dataSink = Vertex.create("DataSink",
                ProcessorDescriptor.create(TextDataSink.class.getName()), DOP);

        UnorderedKVEdgeConfig edgeConf = UnorderedKVEdgeConfig
                .newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName())
                .setFromConfiguration(tezConf)
                .build();

        EdgeProperty edgeProperty1 = edgeConf.createDefaultOneToOneEdgeProperty();

        Edge edge1 = Edge.create (dataSource, dataSink, edgeProperty1);

        DAG dag = DAG.create ("Source-Sink");

        dag.addVertex(dataSource)
                .addVertex(dataSink)
                .addEdge(edge1);

        return dag;
    }

    public WordCount() {
        super("FlinkWordCount");
    }

    public static void main (String [] args) {
        new WordCount().runYarn();
    }

}
