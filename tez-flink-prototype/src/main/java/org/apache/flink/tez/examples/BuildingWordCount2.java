package org.apache.flink.tez.examples;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
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
import org.apache.flink.api.java.typeutils.runtime.RuntimeComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimeStatefulSerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.FlatMapDriver;
import org.apache.flink.runtime.operators.ReduceDriver;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.tez.processor.FlinkProcessor;
import org.apache.flink.tez.wordcount.*;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.*;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

import java.util.Arrays;

public class BuildingWordCount2 extends ProgramLauncher {

    private static int DOP = 4;

    public static String INPUT_FILE="/tmp/sherlock.txt";

    public static String OUTPUT_FILE="/tmp/job_output6";

    public static StringSerializer stringSerializer = new StringSerializer();

    public static StringComparator stringComparator = new StringComparator(true);

    public static TypeSerializer<Tuple2<String,Integer>> tupleSerializer = new TupleSerializer<Tuple2<String, Integer>>(
            (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class,
            new TypeSerializer[] {
                    new StringSerializer(),
                    new IntSerializer()
            }
    );

    public static TupleComparator<Tuple2<String,Integer>> tupleComparator = new TupleComparator<Tuple2<String, Integer>>(
            new int [] {0},
            new TypeComparator[] {new StringComparator(true)},
            new TypeSerializer[] {new StringSerializer()}
    );


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

    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>> {

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

    }

    public static class Summer implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            return new Tuple2<String,Integer> (value1.f0, value1.f1 + value2.f1);
        }
    }

    public BuildingWordCount2() {
        super("BuildingWordCount2");
    }

    public static void main (String [] args) {
        new BuildingWordCount2().runYarn();
    }

    private Vertex createVertex (String name, org.apache.hadoop.conf.Configuration conf, TaskConfig taskConfig, int numberOfSubTaskInOutput, ChannelSelector channelSelector) throws Exception {

        conf.set("io.flink.processor.taskconfig", InstantiationUtil.writeObjectToConfig(taskConfig));
        conf.set("io.flink.processor.numberofoutputsubtasks", InstantiationUtil.writeObjectToConfig(numberOfSubTaskInOutput));
        conf.set("io.flink.processor.channelselector", InstantiationUtil.writeObjectToConfig(channelSelector));

        ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                FlinkProcessor.class.getName());

        descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(conf));

        Vertex vertex = Vertex.create(name, descriptor, DOP);

        return vertex;
    }

    private Vertex createTokenizer (TezConfiguration tezConf) throws Exception {

        TaskConfig tokenizerConfig = new TaskConfig (new Configuration());
        tokenizerConfig.setDriver(FlatMapDriver.class);
        tokenizerConfig.setDriverStrategy(DriverStrategy.FLAT_MAP);
        tokenizerConfig.setStubWrapper(new UserCodeClassWrapper<Tokenizer>(Tokenizer.class));
        tokenizerConfig.setStubParameters(new Configuration());
        tokenizerConfig.setInputSerializer(new RuntimeStatefulSerializerFactory<String>(stringSerializer, String.class), 0);
        tokenizerConfig.setInputComparator(new RuntimeComparatorFactory<String>(stringComparator), 0);
        tokenizerConfig.setOutputSerializer(new RuntimeStatefulSerializerFactory<Tuple2<String,Integer>>(tupleSerializer, (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class));
        tokenizerConfig.setOutputComparator(new RuntimeComparatorFactory<Tuple2<String,Integer>>(tupleComparator), 0);
        tokenizerConfig.setInputLocalStrategy(0, LocalStrategy.NONE);

        int numberOfSubTasksInOutput = DOP;

        //RoundRobinSelector<Tuple2<String,Integer>> channelSelector = new RoundRobinSelector<Tuple2<String, Integer>>();

        PartitioningSelector<Tuple2<String,Integer>> channelSelector = new PartitioningSelector<Tuple2<String, Integer>>(tupleComparator);


        return createVertex("Tokenizer", new TezConfiguration(tezConf), tokenizerConfig, numberOfSubTasksInOutput, channelSelector);
    }

    private Vertex createSummer (TezConfiguration tezConf) throws Exception {
        TaskConfig summerConfig = new TaskConfig(new Configuration());
        summerConfig.setDriver(ReduceDriver.class);
        summerConfig.setDriverStrategy(DriverStrategy.SORTED_REDUCE);
        summerConfig.setStubWrapper(new UserCodeClassWrapper<Summer>(Summer.class));
        summerConfig.setStubParameters(new Configuration());
        summerConfig.setInputSerializer(new RuntimeStatefulSerializerFactory<Tuple2<String,Integer>>(tupleSerializer, (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class), 0);
        summerConfig.setInputComparator(new RuntimeComparatorFactory<Tuple2<String,Integer>>(tupleComparator), 0);
        summerConfig.setOutputSerializer(new RuntimeStatefulSerializerFactory<Tuple2<String,Integer>>(tupleSerializer, (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class));
        summerConfig.setOutputComparator(new RuntimeComparatorFactory<Tuple2<String,Integer>>(tupleComparator), 0);
        summerConfig.setInputLocalStrategy(0, LocalStrategy.SORT);
        summerConfig.setDriverComparator(new RuntimeComparatorFactory<Tuple2<String,Integer>>(tupleComparator), 0);
        summerConfig.setFilehandlesDriver(10);
        summerConfig.setFilehandlesInput(0, 10);
        summerConfig.setSpillingThresholdInput(0, 0.7f);
        summerConfig.setSpillingThresholdDriver(0.7f);
        summerConfig.setRelativeMemoryInput(0, 0.7f);

        int numberOfSubTasksInOutput = DOP;

        RoundRobinSelector<Tuple2<String,Integer>> channelSelector = new RoundRobinSelector<Tuple2<String, Integer>>();

        return createVertex ("Summer", new TezConfiguration(tezConf), summerConfig, numberOfSubTasksInOutput, channelSelector);
    }

    @Override
    public DAG createDAG(TezConfiguration tezConf) throws Exception {

        Vertex mapper = createTokenizer(tezConf);

        Vertex reducer = createSummer(tezConf);

        Vertex dataSource = Vertex.create("DataSource",
                ProcessorDescriptor.create(FileSingleSplitDataSource.class.getName()), DOP);

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

        Edge edge1 = Edge.create(dataSource, mapper, srcMapEdgeProperty);

        Edge edge2 = Edge.create(mapper, reducer, mapReduceEdgeProperty);

        Edge edge3 = Edge.create (reducer, dataSink, reduceSinkEdgeProperty);

        DAG dag = DAG.create("WordCountBuilt");

        dag.addVertex(dataSource)
                .addVertex(mapper)
                .addVertex(reducer)
                .addVertex(dataSink)
                .addEdge(edge1)
                .addEdge(edge2)
                .addEdge(edge3);

        return dag;




    }
}
