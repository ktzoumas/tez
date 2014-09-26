package org.apache.flink.tez.examples;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.tez.wordcount.*;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.*;
import org.apache.tez.runtime.api.ProcessorContext;

import java.util.Arrays;

public class BuildingWordCount extends ProgramLauncher {

    private static FlatMapFunction<String, Tuple2<String, Integer>> udf =
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
            };


    private static int DOP = 1;

    public BuildingWordCount() {
        super("FlinkWordCount");
    }

    public static void main (String [] args) {
        new BuildingWordCount().runLocal();
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

    private Vertex createFlatMapVertex (Configuration conf, FlatMapFunction udf, TypeSerializer<?> inSerializer, TypeSerializer<?> outSerializer, TypeComparator<?> outComparator) throws Exception {

        conf.set("io.flink.processor.udf", InstantiationUtil.writeObjectToConfig(udf));
        conf.set("io.flink.processor.inSerializer", InstantiationUtil.writeObjectToConfig(inSerializer));
        conf.set("io.flink.processor.outSerializer", InstantiationUtil.writeObjectToConfig(outSerializer));
        conf.set("io.flink.processor.outComparator", InstantiationUtil.writeObjectToConfig(outComparator));

        ProcessorDescriptor descriptor = ProcessorDescriptor.create(
                FlatMapProcessorImpl.class.getName());

        descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(conf));

        Vertex vertex = Vertex.create("Tokenizer", descriptor, BuildingWordCount.DOP);

        return vertex;
    }

    public DAG createDAG(TezConfiguration tezConf) throws Exception {

        Vertex dataSource = Vertex.create("DataSource",
                ProcessorDescriptor.create(CollectionDataSource.class.getName()), DOP);

        Vertex dataSink = Vertex.create ("DataSink",
                ProcessorDescriptor.create (PrintingDataSink.class.getName()), DOP);

        TypeSerializer<String> inSerializer = new StringSerializer();

        TypeSerializer<Tuple2<String,Integer>> outSerializer = new TupleSerializer<Tuple2<String, Integer>>(
                (Class<Tuple2<String,Integer>>) (Class<?>) Tuple2.class,
                new TypeSerializer[] {
                        new StringSerializer(),
                        new IntSerializer()
                }
        );

        TupleComparator<Tuple2<String,Integer>> outComparator = new TupleComparator<Tuple2<String, Integer>>(
                new int [] {0},
                new TypeComparator[] {new StringComparator(true)},
                new TypeSerializer[] {new StringSerializer()}
        );

        Vertex mapper = createFlatMapVertex(new TezConfiguration(tezConf), udf, inSerializer, outSerializer, outComparator);

        FlinkUnorderedKVEdgeConfig srcMapEdgeConf = (FlinkUnorderedKVEdgeConfig) (FlinkUnorderedKVEdgeConfig
                .newBuilder(IntWritable.class.getName(), WritableSerializationDelegate.class.getName())
                .setFromConfiguration(tezConf)
                .configureInput()
                .setAdditionalConfiguration("io.flink.typeserializer", InstantiationUtil.writeObjectToConfig(
                        new StringSerializer()
                )))
                .done()
                .build();

        FlinkUnorderedKVEdgeConfig mapSinkConf = (FlinkUnorderedKVEdgeConfig) (FlinkUnorderedKVEdgeConfig
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

        EdgeProperty mapSinkEdgeProperty = mapSinkConf.createDefaultOneToOneEdgeProperty();

        Edge srcMapEdge = Edge.create(dataSource, mapper, srcMapEdgeProperty);

        Edge mapSinkEdge = Edge.create(mapper, dataSink, mapSinkEdgeProperty);

        DAG dag = DAG.create("WordCOuntBuilt");

        dag.addVertex(dataSource).addVertex(mapper).addVertex(dataSink).addEdge(srcMapEdge).addEdge(mapSinkEdge);

        return dag;
    }
}
