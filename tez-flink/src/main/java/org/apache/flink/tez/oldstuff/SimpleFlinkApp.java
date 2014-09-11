package org.apache.flink.tez.oldstuff;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.typeutils.runtime.WritableSerializer;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.runtime.operators.MapDriver;
import org.apache.flink.tez.wordcount.TezTaskContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfigurer;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SimpleFlinkApp {


	public static final class DataSourceVertex extends SimpleProcessor {

		CollectionInputFormat<String> inputFormat;

		private static class KeyValueCollector implements Collector<Text> {

			KeyValueWriter kvWriter;

			private KeyValueCollector(KeyValueWriter kvWriter) {
				this.kvWriter = kvWriter;
			}

			@Override
			public void collect(Text record) {
				try {
					kvWriter.write(new LongWritable(record.toString().length()), record);
				}
				catch (Exception e) {
					System.err.println("Cannot write " + record + " to intermediate file");
					e.printStackTrace();
					System.exit(1);
				}
			}

			@Override
			public void close() { }
		}

		public DataSourceVertex(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {

			inputFormat = new CollectionInputFormat(
					Arrays.asList("one,", "two", "three", "four", "five"),
					new StringSerializer());

			Writer rawWriter = getOutputs().values().iterator().next().getWriter();

			if (!(rawWriter instanceof KeyValueWriter)) {
				throw new IllegalStateException("Writer must be of KeyValueWriter type");
			}

			KeyValueWriter kvWriter = (KeyValueWriter) rawWriter;

			KeyValueCollector kvCollector = new KeyValueCollector(kvWriter);

			inputFormat.open(new GenericInputSplit());

			String outValue = null;
			while (!inputFormat.reachedEnd()) {
				if ((outValue = inputFormat.nextRecord(outValue)) != null) {
					kvCollector.collect(new Text(outValue));
				}
			}
		}

		@Override
		public void close() throws Exception {
			super.close();
			inputFormat.close();
		}
	}


	public static final class MapVertex extends SimpleProcessor {

		TezTaskContext<MapFunction<Text,Text>, Text> mapContext;

		public MapVertex(ProcessorContext context) {
			super(context);
		}

		private class KeyValueIterator implements MutableObjectIterator<Text> {

			KeyValueReader kvReader;

			private KeyValueIterator(KeyValueReader kvReader) {
				this.kvReader = kvReader;
			}

			@Override
			public Text next(Text reuse) throws IOException {
				if (kvReader.next()) {
					Object key = kvReader.getCurrentKey();
					Object value = kvReader.getCurrentValue();
					if (!(value instanceof Text && key instanceof LongWritable))
						throw new IllegalStateException("Value should be Text, key should be LongWritable");
                    Text string = (Text) value;
					LongWritable length = (LongWritable) key;
					if (string.toString().length() != length.get())
						throw new IllegalStateException("Length is not correct");
					return string;
				}
				else {
					return null;
				}
			}
		}

		@Override
		public void run() throws Exception {
			MapFunction<Text,Text> mapFunction = new MapFunction<Text, Text>() {
				@Override
				public Text map(Text value) throws Exception {
					return new Text(value.toString().toUpperCase());
				}
			};

			mapContext = new TezTaskContext<MapFunction<Text, Text>, Text>();
			mapContext.setUdf(mapFunction);
			mapContext.setCollector(new Collector<Text>() {
				@Override
				public void collect(Text record) {
					System.out.println(record.toString());
				}

				@Override
				public void close() {
				}
			});

			Reader rawReader = getInputs().values().iterator().next().getReader();

			if (!(rawReader instanceof KeyValueReader)) {
				throw new IllegalStateException("Reader must be of KeyValue reader type");
			}

			KeyValueReader kvReader = (KeyValueReader) rawReader;

			mapContext.setInput1(new KeyValueIterator(kvReader), new WritableSerializer<Text>(Text.class));
			MapDriver<Text,Text> mapDriver = new MapDriver<Text, Text>();
			mapDriver.setup(mapContext);

			mapDriver.run();

			Collector<Text> strings = mapContext.getOutputCollector();

		}
	}

	public static final class Stage1Vertex extends SimpleProcessor {

		private class StringIterator implements MutableObjectIterator<String> {

			String [] strings = {"one", "two", "three", "four", "five"};

			int i = 0;

			@Override
			public String next(String reuse) throws IOException {
				if (i >= 4)
					return null;
				i++;
				return strings[i-1];
			}
		}

		private static final class ListOutputCollector implements Collector<String> {

			private final List<String> output;

			public ListOutputCollector(List<String> outputList) {
				this.output = outputList;
			}


			@Override
			public void collect(String string) {
				this.output.add(string);
			}

			@Override
			public void close() {}
		}

		public Stage1Vertex(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {

			MapFunction<String,String> mapFunction = new MapFunction<String, String>() {
				@Override
				public String map(String value) throws Exception {
					return value.toUpperCase();
				}
			};
			TezTaskContext<MapFunction<String, String>, String> context = new TezTaskContext<MapFunction<String, String>, String>();
			context.setUdf(mapFunction);
			context.setCollector(new Collector<String>() {
				@Override
				public void collect(String record) {
					System.out.println(record);
				}

				@Override
				public void close() {
				}
			});
			context.setInput1(new StringIterator(), new StringSerializer());

			MapDriver<String,String> mapDriver = new MapDriver<String, String>();
			mapDriver.setup(context);

			mapDriver.run();

			Collector<String> strings = context.getOutputCollector();
		}
	}

	public static DAG createDAG (TezConfiguration tezConf) throws Exception {

		DataSinkDescriptor dataSink = MROutput.createConfigurer(new Configuration(tezConf),
				TextOutputFormat.class, "/tmp/helloworldoutput12/").create();

		//Vertex stage1Vertex = new Vertex("Stage1Vertex",
		//		new ProcessorDescriptor(Stage1Vertex.class.getName()),
		//		1);

		//DAG dag = new DAG ("HelloWorldDAG");

		//dag.addVertex(stage1Vertex);

		Vertex dataSourceVertex = new Vertex ("DataSource",
				new ProcessorDescriptor(DataSourceVertex.class.getName()),
				1);

		Vertex mapVertex = new Vertex ("Mapper",
				new ProcessorDescriptor(MapVertex.class.getName()),
				1);

		UnorderedKVEdgeConfigurer edgeConf = UnorderedKVEdgeConfigurer
				.newBuilder(LongWritable.class.getName(), Text.class.getName())
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
					System.out.println("HelloWorldApp failed with diagnostics: " + dagStatus.getDiagnostics());
					System.exit(1);
				}
				System.out.println("HelloWorldApp finished successfully");
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
