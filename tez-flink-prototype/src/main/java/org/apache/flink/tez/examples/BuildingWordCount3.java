package org.apache.flink.tez.examples;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
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
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.FlatMapDriver;
import org.apache.flink.runtime.operators.ReduceDriver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.tez.input.FlinkUnorderedKVEdgeConfig;
import org.apache.flink.tez.input.FlinkUnorderedPartitionedKVEdgeConfig;
import org.apache.flink.tez.input.WritableSerializationDelegate;
import org.apache.flink.tez.runtime.DataSinkProcessor;
import org.apache.flink.tez.runtime.FlinkDataSinkProcessor;
import org.apache.flink.tez.runtime.FlinkDataSourceProcessor;
import org.apache.flink.tez.runtime.FlinkProcessor;
import org.apache.flink.tez.runtime.MockInputSplitProvider;
import org.apache.flink.tez.runtime.SimplePartitioner;
import org.apache.flink.tez.runtime.SingleSplitDataSourceProcessor;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.tez.util.InstantiationUtil;
import org.apache.flink.tez.wordcount_old.ProgramLauncher;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

public class BuildingWordCount3 extends ProgramLauncher {

    private static int DOP = 8;

    public static String INPUT_FILE="/tmp/sherlock.txt";

    public static String OUTPUT_FILE="/tmp/job_output17";

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

    public BuildingWordCount3() {
        super("BuildingWordCount3");
    }

    public static void main (String [] args) {
        new BuildingWordCount3().runLocal();
    }

    private Vertex createVertex (String name, org.apache.hadoop.conf.Configuration conf, TezTaskConfig taskConfig) throws Exception {

        conf.set("io.flink.processor.taskconfig", InstantiationUtil.writeObjectToConfig(taskConfig));
        //conf.set("io.flink.processor.numberofoutputsubtasks", InstantiationUtil.writeObjectToConfig(numberOfSubTaskInOutput));
        //conf.set("io.flink.processor.channelselector", InstantiationUtil.writeObjectToConfig(channelSelector));

        ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                FlinkProcessor.class.getName());

        descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(conf));

        Vertex vertex = Vertex.create(name, descriptor, DOP);

        return vertex;
    }

    private Vertex createDataSink (TezConfiguration conf) throws Exception {
        TezConfiguration tezConf = new TezConfiguration(conf);
        TezTaskConfig sinkConfig = new TezTaskConfig(new Configuration());

        TextOutputFormat<Tuple2<String, Integer>> format =
                new TextOutputFormat<Tuple2<String, Integer>>(new Path(OUTPUT_FILE));
        format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY);

        sinkConfig.setStubWrapper(new UserCodeObjectWrapper<TextOutputFormat>(format));
        sinkConfig.setInputSerializer(new RuntimeStatefulSerializerFactory<Tuple2<String,Integer>>(tupleSerializer, (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class), 0);

        tezConf.set("io.flink.processor.taskconfig", InstantiationUtil.writeObjectToConfig(sinkConfig));
        ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                FlinkDataSinkProcessor.class.getName());
        descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(tezConf));
        Vertex vertex = Vertex.create("Data Sink", descriptor, DOP);
        return vertex;
    }

    private Vertex createDataSource (TezConfiguration conf) throws Exception {

        TezConfiguration tezConf = new TezConfiguration(conf);
        TezTaskConfig sourceConfig = new TezTaskConfig (new Configuration());
        TextInputFormat format = new TextInputFormat(new Path(INPUT_FILE));

        MockInputSplitProvider inputSplitProvider = new MockInputSplitProvider();
        inputSplitProvider.addInputSplits(INPUT_FILE, 1);

        sourceConfig.setStubWrapper(new UserCodeObjectWrapper<TextInputFormat>(format));
        sourceConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
        sourceConfig.setOutputSerializer(new RuntimeStatefulSerializerFactory<String>(stringSerializer, String.class));
        sourceConfig.setNumberSubtasksInOutput(DOP);
        sourceConfig.setInputSplitProvider(inputSplitProvider);

        tezConf.set("io.flink.processor.taskconfig", InstantiationUtil.writeObjectToConfig(sourceConfig));
        ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                FlinkDataSourceProcessor.class.getName());
        descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(tezConf));
        Vertex vertex = Vertex.create("Data Source", descriptor, DOP);

        return vertex;
    }




    private Vertex createTokenizer (TezConfiguration tezConf) throws Exception {

        TezTaskConfig tokenizerConfig = new TezTaskConfig (new Configuration());
        tokenizerConfig.setDriver(FlatMapDriver.class);
        tokenizerConfig.setDriverStrategy(DriverStrategy.FLAT_MAP);
        tokenizerConfig.setStubWrapper(new UserCodeClassWrapper<Tokenizer>(Tokenizer.class));
        tokenizerConfig.setStubParameters(new Configuration());
        tokenizerConfig.setInputSerializer(new RuntimeStatefulSerializerFactory<String>(stringSerializer, String.class), 0);
        tokenizerConfig.setInputComparator(new RuntimeComparatorFactory<String>(stringComparator), 0);
        tokenizerConfig.setOutputSerializer(new RuntimeStatefulSerializerFactory<Tuple2<String,Integer>>(tupleSerializer, (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class));
        tokenizerConfig.setOutputComparator(new RuntimeComparatorFactory<Tuple2<String,Integer>>(tupleComparator), 0);
        tokenizerConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
        tokenizerConfig.setNumberSubtasksInOutput(DOP);
        //tokenizerConfig.setChannelSelector(new PartitioningSelector<Tuple2<String, Integer>>(tupleComparator));
        tokenizerConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);

        //int numberOfSubTasksInOutput = DOP;

        //RoundRobinSelector<Tuple2<String,Integer>> channelSelector = new RoundRobinSelector<Tuple2<String, Integer>>();

        //PartitioningSelector<Tuple2<String,Integer>> channelSelector = new PartitioningSelector<Tuple2<String, Integer>>(tupleComparator);


        return createVertex("Tokenizer", new TezConfiguration(tezConf), tokenizerConfig);
    }

    private Vertex createSummer (TezConfiguration tezConf) throws Exception {
        TezTaskConfig summerConfig = new TezTaskConfig(new Configuration());
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
        summerConfig.setNumberSubtasksInOutput(DOP);
        //summerConfig.setChannelSelector(new RoundRobinSelector<Tuple2<String, Integer>>());
        summerConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);


        //int numberOfSubTasksInOutput = DOP;

        //RoundRobinSelector<Tuple2<String,Integer>> channelSelector = new RoundRobinSelector<Tuple2<String, Integer>>();

        return createVertex ("Summer", new TezConfiguration(tezConf), summerConfig);
    }

    @Override
    public DAG createDAG(TezConfiguration tezConf) throws Exception {

        Vertex mapper = createTokenizer(tezConf);

        Vertex reducer = createSummer(tezConf);

        Vertex dataSource = createDataSource(tezConf);

        Vertex dataSink = createDataSink(tezConf);

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
