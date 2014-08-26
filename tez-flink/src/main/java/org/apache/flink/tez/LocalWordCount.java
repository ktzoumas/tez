package org.apache.flink.tez;


import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfigurer;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfigurer;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import java.io.IOException;
import java.util.StringTokenizer;

public class LocalWordCount {

	public static class TokenProcessor extends SimpleProcessor {
		IntWritable one = new IntWritable(1);
		Text word1 = new Text("hello, ");
		Text word2 = new Text("hello, ");
		Text word3 = new Text("tez");
		Text word4 = new Text("tez");
		Text word5 = new Text("tez");


		public TokenProcessor(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {
			Preconditions.checkArgument(getInputs().size() == 0);
			Preconditions.checkArgument(getOutputs().size() == 1);

			KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();

			kvWriter.write(word1, one);
			kvWriter.write(word2, one);
			kvWriter.write(word3, one);
			kvWriter.write(word4, one);
			kvWriter.write(word5, one);
		}

	}

	public static class SumProcessor extends SimpleMRProcessor {
		public SumProcessor(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {
			Preconditions.checkArgument(getInputs().size() == 1);
			Preconditions.checkArgument(getOutputs().size() == 1);
			KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();
			KeyValuesReader kvReader = (KeyValuesReader) getInputs().values().iterator().next().getReader();
			while (kvReader.next()) {
				Text word = (Text) kvReader.getCurrentKey();
				int sum = 0;
				for (Object value : kvReader.getCurrentValues()) {
					sum += ((IntWritable) value).get();
				}
				kvWriter.write(word, new IntWritable(sum));
			}
		}
	}

	private static DAG createDAG(TezConfiguration tezConf, String outputPath,
						  int numPartitions) throws IOException {

		DataSinkDescriptor dataSink = MROutput.createConfigurer(new Configuration(tezConf),
				TextOutputFormat.class, outputPath).create();

		Vertex tokenizerVertex = new Vertex("Tokenizer", new ProcessorDescriptor(
				TokenProcessor.class.getName()),
				numPartitions);

		OrderedPartitionedKVEdgeConfigurer edgeConf = OrderedPartitionedKVEdgeConfigurer
				.newBuilder(Text.class.getName(), IntWritable.class.getName(),
						HashPartitioner.class.getName()).build();

		UnorderedKVEdgeConfigurer simpleEdgeConf = UnorderedKVEdgeConfigurer
				.newBuilder(Text.class.getName(), IntWritable.class.getName()).build();

		Vertex summationVertex = new Vertex("Summation",
				new ProcessorDescriptor(SumProcessor.class.getName()),
				numPartitions).addDataSink("Output", dataSink);

		// No need to add jar containing this class as assumed to be part of the Tez jars. Otherwise
		// we would have to add the jars for this code as local files to the vertices.

		DAG dag = new DAG("WordCount");
		dag.addVertex(tokenizerVertex)
				.addVertex(summationVertex)
				.addEdge(
						new Edge(tokenizerVertex, summationVertex,
								//edgeConf.createDefaultEdgeProperty()
								simpleEdgeConf.createDefaultBroadcastEdgeProperty()
						));
		return dag;
	}

	public static void main (String [] args) {
		try {
			final TezConfiguration tezConf = new TezConfiguration();

			tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
			tezConf.set("fs.defaultFS", "file:///");
			tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

			TezClient tezClient = new TezClient("LocalWordCount", tezConf);
			tezClient.start();


			try {
				DAG dag = createDAG(tezConf, args[0], 1);

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
