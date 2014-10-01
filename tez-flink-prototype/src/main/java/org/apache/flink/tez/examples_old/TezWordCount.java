package org.apache.flink.tez.examples_old;

import com.google.common.base.Preconditions;
import org.apache.flink.tez.wordcount_old.ProgramLauncher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.StringTokenizer;


public class TezWordCount extends ProgramLauncher {

    static String INPUT = "Input";
    static String OUTPUT = "Output";
    static String TOKENIZER = "Tokenizer";
    static String SUMMATION = "Summation";

    public static String INPUT_FILE="hdfs://localhost:9000/tmp/sherlock.txt";
    public static String OUTPUT_FILE="hdfs://localhost:9000/tmp/job_output22";
    private static int DOP = 8;

    public TezWordCount() {
        super("Tez Word Count");
    }

    public static void main (String [] args) {
        new TezWordCount().runLocal();
    }

    public static class TokenProcessor extends SimpleProcessor {
        IntWritable one = new IntWritable(1);
        Text word = new Text();

        public TokenProcessor(ProcessorContext context) {
            super(context);
        }

        @Override
        public void run() throws Exception {
            Preconditions.checkArgument(getInputs().size() == 1);
            Preconditions.checkArgument(getOutputs().size() == 1);
            // the recommended approach is to cast the reader/writer to a specific type instead
            // of casting the input/output. This allows the actual input/output type to be replaced
            // without affecting the semantic guarantees of the data type that are represented by
            // the reader and writer.
            // The inputs/outputs are referenced via the names assigned in the DAG.
            KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT).getReader();
            KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(SUMMATION).getWriter();
            while (kvReader.next()) {
                StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    // Count 1 every time a word is observed. Word is the key a 1 is the value
                    kvWriter.write(word, one);
                }
            }
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
            KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
            // The KeyValues reader provides all values for a given key. The aggregation of values per key
            // is done by the LogicalInput. Since the key is the word and the values are its counts in
            // the different TokenProcessors, summing all values per key provides the sum for that word.
            KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(TOKENIZER).getReader();
            while (kvReader.next()) {
                Text word = (Text) kvReader.getCurrentKey();
                int sum = 0;
                for (Object value : kvReader.getCurrentValues()) {
                    sum += ((IntWritable) value).get();
                }
                kvWriter.write(word, new IntWritable(sum));
            }
            // deriving from SimpleMRProcessor takes care of committing the output
            // It automatically invokes the commit logic for the OutputFormat if necessary.
        }
    }

    @Override
    public DAG createDAG(TezConfiguration tezConf) throws Exception {

        DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(tezConf),
                TextInputFormat.class, INPUT_FILE).build();


        DataSinkDescriptor dataSink = MROutput.createConfigBuilder(new Configuration(tezConf),
                TextOutputFormat.class, OUTPUT_FILE).build();


        Vertex tokenizerVertex = Vertex.create(TOKENIZER, ProcessorDescriptor.create(
                TokenProcessor.class.getName())).addDataSource(INPUT, dataSource);


        OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
                .newBuilder(Text.class.getName(), IntWritable.class.getName(),
                        HashPartitioner.class.getName()).build();


        Vertex summationVertex = Vertex.create(SUMMATION,
                ProcessorDescriptor.create(SumProcessor.class.getName()), DOP)
                .addDataSink(OUTPUT, dataSink);


        DAG dag = DAG.create("WordCount");
        dag.addVertex(tokenizerVertex)
                .addVertex(summationVertex)
                .addEdge(
                        Edge.create(tokenizerVertex, summationVertex, edgeConf.createDefaultEdgeProperty()));
        return dag;
    }


}
