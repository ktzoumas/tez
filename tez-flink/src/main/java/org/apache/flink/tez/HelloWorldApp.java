package org.apache.flink.tez;


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
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfigurer;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

public class HelloWorldApp {

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

	public static final class Stage2Vertex extends SimpleProcessor {

		public Stage2Vertex(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {

			if ((getOutputs().size() != 1) || (getInputs().size() != 1)) {
				throw new IllegalStateException("Stage 2 must have one input and one output.");
			}

			LogicalInput li = getInputs().values().iterator().next();

			if (!(li instanceof UnorderedKVInput)) {
				throw new IllegalStateException("Stage 2 input must be of UnorderedKVInput type");
			}

			Reader rawReader = li.getReader();

			if (!(rawReader instanceof  KeyValueReader)) {
				throw new IllegalStateException("Reader must be of KeyValueReader type");
			}

			KeyValueReader kvReader = (KeyValueReader) rawReader;

			Object key = kvReader.getCurrentKey();
			Object value = kvReader.getCurrentValue();


			System.out.println(value);
			System.out.println("The answer to life, the universe, and everything is: " + key);


			LogicalOutput lo = getOutputs().values().iterator().next();

			if (!(lo instanceof MROutput)) {
				throw new IllegalStateException("Stage 2 output must be of MROutput type");
			}

			Writer rawWriter = lo.getWriter();

			if (!(rawWriter instanceof KeyValueWriter)) {
				throw new IllegalStateException("Stage 2 writer must be of KeyValue writer type");
			}

			KeyValueWriter kvWriter = (KeyValueWriter) rawWriter;

			kvWriter.write(key, value);
		}
	}

	public static DAG createDAG (TezConfiguration tezConf) throws Exception{

		DataSinkDescriptor dataSink = MROutput.createConfigurer(new Configuration(tezConf),
				TextOutputFormat.class, "/tmp/helloworldoutput3/").create();

		Vertex helloWriter = new Vertex("HelloWriter",
				new ProcessorDescriptor(Stage1Vertex.class.getName()),
				1);

		Vertex helloReader = new Vertex("HelloReader",
				new ProcessorDescriptor(Stage2Vertex.class.getName()),
				1)
				.addDataSink("Output", dataSink);

		UnorderedKVEdgeConfigurer edgeConf = UnorderedKVEdgeConfigurer
				.newBuilder(LongWritable.class.getName(), Text.class.getName()).build();

		EdgeProperty edgeProperty = edgeConf.createDefaultOneToOneEdgeProperty();

		Edge edge = new Edge (helloWriter, helloReader, edgeProperty);

		DAG dag = new DAG ("HelloWorldDAG");

		dag.addVertex(helloWriter).addVertex(helloReader).addEdge(edge);

		return dag;
	}



	public static void main (String [] args) {
		try {
			final TezConfiguration tezConf = new TezConfiguration();

			tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
			tezConf.set("fs.defaultFS", "file:///");
			tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

			TezClient tezClient = new TezClient("HelloWorld", tezConf);
			tezClient.start();

			try {
				DAG dag = createDAG(tezConf);

				tezClient.waitTillReady();
				System.out.println("Submitting DAG to Tez Client");
				DAGClient dagClient = tezClient.submitDAG(dag);
				System.out.println("Submitted DAG to Tez Client");

				// monitoring
				//DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
				DAGStatus dagStatus = dagClient.waitForCompletion();

				if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
					System.out.println("LocalOrderedWordCount failed with diagnostics: " + dagStatus.getDiagnostics());
					System.exit(1);
				}
				System.out.println("LocalOrderedWordCount finished successfully");
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
