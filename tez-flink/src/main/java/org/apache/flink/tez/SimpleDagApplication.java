package org.apache.flink.tez;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.input.ShuffledUnorderedKVInput;
import org.apache.tez.runtime.library.output.OnFileUnorderedKVOutput;

import eu.stratosphere.util.LogUtils;

public class SimpleDagApplication {




	public static void main(String[] args) {

		final String jarPath = "/home/cicero/Desktop/simple_tez_app.jar";

		// create some default logger
		LogUtils.initializeDefaultConsoleLogger();

		// make sure the basic configuration has all the essential config files
		final TezConfiguration config = new TezConfiguration();
		TezUtils.addAllTezConfigResources(config /*, "/data/hadoop/hadoop-2.4.1" */);

		// for debugging, set the timeouts high and the app master to wait for an attached debugger
		TezUtils.setTezTimeoutsHigh(config);
		TezUtils.setApplicationMasterRemoteDebugProperties(config, 6687);



		try {
			final Credentials credentials = new Credentials();
			final FileSystem fs = FileSystem.get(config);

			final Map<String, LocalResource> commonLocalResources =
					TezUtils.prepareConfigForExecutionWithJar(jarPath, config, credentials, fs);

			final AMConfiguration amConf = new AMConfiguration(null, commonLocalResources, config, credentials);

			final TezSessionConfiguration sessionConf = new TezSessionConfiguration(amConf, config);

			TezSession tezSession = null;
			try {
				tezSession = new TezSession("Simple DAG Application Session", sessionConf);
				tezSession.start();

				DAG job = getJobGraph(config, commonLocalResources);

				System.out.println("Submitting DAG to Tez Session");
				DAGClient dagClient = tezSession.submitDAG(job);
				System.out.println("Submitted DAG to Tez Session");

				TezUtils.waitForJobCompletionAndPrintStatus(dagClient, 1000, new String[] {"Source", "Sink"});

				System.out.println("Application completed. FinalState=" + dagClient.getDAGStatus(null).getState());
			}
			finally {
				if (tezSession != null) {
					tezSession.stop();
				}
			}
		}
		catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		}

		// all good!
		System.exit(0);
	}


	private static DAG getJobGraph(Configuration conf, Map<String, LocalResource> commonLocalResources) throws IOException {

		// configure source vertex
		Configuration inputConf = new JobConf(conf);
		inputConf.setBoolean("mapred.mapper.new-api", false);
		inputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_KEY_CLASS, LongWritable.class.getName());
		inputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_VALUE_CLASS, Text.class.getName());

		// configure target vertex
		Configuration outputConf = new JobConf(conf);
		outputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_KEY_CLASS, LongWritable.class.getName());
		outputConf.set(TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_VALUE_CLASS, Text.class.getName());
		outputConf.setBoolean("mapred.mapper.new-api", false);
		MultiStageMRConfToTezTranslator.translateVertexConfToTez(outputConf, inputConf);

		byte[] stage1Payload = MRHelpers.createUserPayloadFromConf(inputConf);
		byte[] stage2Payload = MRHelpers.createUserPayloadFromConf(outputConf);


		// Setup source Vertex
		Vertex stage1Vertex = new Vertex("Source",
				new ProcessorDescriptor(SimpleGeneratingSource.class.getName()).setUserPayload(stage1Payload),
				1,
				Resource.newInstance(1024, 1));

		stage1Vertex.setTaskLocalResources(commonLocalResources);

		Vertex stage2Vertex = new Vertex("Sink",
				new ProcessorDescriptor(SimpleDiscardingSink.class.getName()).setUserPayload(stage2Payload),
				1,
				Resource.newInstance(1024, 1));
		stage2Vertex.setTaskLocalResources(commonLocalResources);


		DAG dag = new DAG("Simple DAG");
		Edge edge = new Edge(stage1Vertex, stage2Vertex,
				new EdgeProperty(DataMovementType.BROADCAST, DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
						new OutputDescriptor(OnFileUnorderedKVOutput.class.getName()),
						new InputDescriptor(ShuffledUnorderedKVInput.class.getName())));
		dag.addVertex(stage1Vertex).addVertex(stage2Vertex).addEdge(edge);

		return dag;
	}

	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	// --------------------------------------------------------------------------------------------
	//  The vertex classes
	// --------------------------------------------------------------------------------------------

	public static final class SimpleGeneratingSource implements LogicalIOProcessor {

		@Override
		public void initialize(TezProcessorContext processorContext) {}

		@Override
		public void handleEvents(List<Event> processorEvents) {}

		@Override
		public void close() {}

		@Override
		public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {

			if (inputs.size() != 0) {
				throw new IllegalStateException("SimpleGeneratingSource cannot have inputs.");
			}

			if (outputs.size() != 1) {
				throw new IllegalStateException("SimpleGeneratingSource must have exactly one output.");
			}

			LogicalOutput lo = outputs.values().iterator().next();
			if (!(lo instanceof OnFileUnorderedKVOutput)) {
				throw new IllegalStateException("SimpleGeneratingSource processor can only work with OnFileUnorderedKVOutput");
			}
			lo.start();

			System.out.println("Starting the simple source...");

			OnFileUnorderedKVOutput kvOutput = (OnFileUnorderedKVOutput) lo;
			KeyValueWriter kvWriter = kvOutput.getWriter();

			LongWritable key = new LongWritable(42);
			Text value = new Text("This is the test message");

			kvWriter.write(key, value);
		}
	}

	public static final class SimpleDiscardingSink implements LogicalIOProcessor {

		@Override
		public void initialize(TezProcessorContext processorContext) {}

		@Override
		public void handleEvents(List<Event> processorEvents) {}

		@Override
		public void close() {}

		@Override
		public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {

			if (inputs.size() != 1) {
				throw new IllegalStateException("SimpleDiscardingSink cannot have inputs.");
			}

			if (outputs.size() != 0) {
				throw new IllegalStateException("SimpleDiscardingSink must have exactly one output.");
			}

			LogicalInput li = inputs.values().iterator().next();
			if (!(li instanceof ShuffledUnorderedKVInput)) {
				throw new IllegalStateException("SimpleDiscardingSink processor can only work with ShuffledUnorderedKVInput");
			}
			li.start();

			ShuffledUnorderedKVInput kvInput = (ShuffledUnorderedKVInput) li;

			System.out.println("Starting the simple sink...");


			KeyValueReader kvReader = kvInput.getReader();

			while (kvReader.next()) {
				Object key = kvReader.getCurrentKey();
				Object value = kvReader.getCurrentValue();

				System.out.println("Got " + key + " / " +  value);
			}
		}
	}
}
