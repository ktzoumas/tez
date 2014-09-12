package org.apache.flink.tez.oldstuff;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
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
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.readers.UnorderedKVReader;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import java.io.IOException;
import java.util.Map;

public class HelloWorldApp {

	public static boolean localExecution = true;

	public static final class Stage1Vertex extends SimpleProcessor {

		LongWritable key = new LongWritable(42);
		Text value = new Text("Hello, world");

		public Stage1Vertex(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {

			if ((getOutputs().size() != 1) || (getInputs().size() != 0)) {
				throw new IllegalStateException("Stage 1 cannot have inputs, must have single output.");
			}

			LogicalOutput lo = getOutputs().values().iterator().next();

			if (!(lo instanceof UnorderedKVOutput)) {
				throw new IllegalStateException("Stage 1 output must be of UnorderedKVOutput type");
			}

			Writer rawWriter = lo.getWriter();

			if (!(rawWriter instanceof  KeyValueWriter)) {
				throw new IllegalStateException("Stage 1 writer must be of KeyValueWriter type");
			}

			KeyValueWriter kvWriter = (KeyValueWriter) rawWriter;

			kvWriter.write(key, value);
		}
	}

	public static final class Stage2Vertex extends SimpleMRProcessor {

		private static final Log LOG = LogFactory.getLog(Stage2Vertex.class);

		public Stage2Vertex(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {

			String[] workingDirs = this.getContext().getWorkDirs();

			if ((getOutputs().size() != 1) || (getInputs().size() != 1)) {
				throw new IllegalStateException("Stage 2 must have one input and one output.");
			}

			LogicalInput li = getInputs().values().iterator().next();

			if (!(li instanceof UnorderedKVInput)) {
				throw new IllegalStateException("Stage 2 input must be of UnorderedKVInput type");
			}

			Reader rawReader = li.getReader();

			if (!(rawReader instanceof UnorderedKVReader)) {
				throw new IllegalStateException("Reader must be of KeyValueReader type");
			}

			UnorderedKVReader kvReader = (UnorderedKVReader) rawReader;

			Object key = null;
			Object value = null;

			if (kvReader.next()) {
				key = kvReader.getCurrentKey();
				value = kvReader.getCurrentValue();
				LOG.info ("Stage 2 received key " + key + " and value " + value);
			}
			else {
				throw new IllegalStateException("Stage 2 did not receive any input");
			}

			// Both key and value are null

			LogicalOutput lo = getOutputs().values().iterator().next();

			if (!(lo instanceof MROutput)) {
				throw new IllegalStateException("Stage 2 output must be of MROutput type");
			}

			Writer rawWriter = lo.getWriter();

			if (!(rawWriter instanceof KeyValueWriter)) {
				throw new IllegalStateException("Stage 2 writer must be of KeyValue writer type");
			}

			KeyValueWriter kvWriter = (KeyValueWriter) rawWriter;

			kvWriter.write(new LongWritable(36), "Hello, Tez");
			kvWriter.write(key, value);
		}
	}

	public static DAG createDAG (TezConfiguration tezConf, Map<String, LocalResource> localResources) throws Exception {

		DataSinkDescriptor dataSink = MROutput.createConfigurer(new Configuration(tezConf),
				TextOutputFormat.class, "/tmp/helloworldoutput12/").create();

		Vertex stage1Vertex = new Vertex("Stage1Vertex",
				new ProcessorDescriptor(Stage1Vertex.class.getName()),
				1);

		if (localResources != null)
			stage1Vertex.setTaskLocalFiles(localResources);

		Vertex stage2Vertex = new Vertex("Stage2Vertex",
				new ProcessorDescriptor(Stage2Vertex.class.getName()),
				1)
				.addDataSink("Output", dataSink);

		if (localResources != null)
			stage2Vertex.setTaskLocalFiles(localResources);

		UnorderedKVEdgeConfigurer edgeConf = UnorderedKVEdgeConfigurer
				.newBuilder(LongWritable.class.getName(), Text.class.getName())
				.setFromConfiguration(tezConf)
				.build();

		EdgeProperty edgeProperty = edgeConf.createDefaultOneToOneEdgeProperty();


		Edge edge = new Edge (stage1Vertex, stage2Vertex, edgeProperty);

		DAG dag = new DAG ("HelloWorldDAG");

		dag.addVertex(stage1Vertex).addVertex(stage2Vertex).addEdge(edge);

		return dag;
	}


	public static void configureLocal (TezConfiguration tezConf) {
		tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
		tezConf.set("fs.defaultFS", "file:///");
		tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
	}

	public static Map<String, LocalResource> configureYarn (TezConfiguration tezConf) {
		try {
			String jarPath = "/Users/kostas/Desktop/hello_world_app.jar";

			TezUtils.setTezTimeoutsHigh(tezConf);
			TezUtils.addAllTezConfigResources(tezConf);
			Credentials credentials = new Credentials();
			FileSystem fs = FileSystem.get(tezConf);
			return TezUtils.prepareConfigForExecutionWithJar(jarPath, tezConf, credentials, fs);
		}
		catch (IOException e) {
			System.err.println ("Unable to configure job for remote execution");
			System.exit(1);
		}
		return null;
	}

	public static void main (String [] args) {
		try {
			final TezConfiguration tezConf = new TezConfiguration();

			Map<String, LocalResource> localResources = null;
			if (localExecution) {
				configureLocal(tezConf);
			}
			else {
				localResources = configureYarn(tezConf);
			}

			TezClient tezClient = new TezClient("HelloWorldApp", tezConf);
			tezClient.start();


			try {
				DAG dag = createDAG(tezConf, localResources);

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
