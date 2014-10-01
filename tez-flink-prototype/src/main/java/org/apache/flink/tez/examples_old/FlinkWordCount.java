package org.apache.flink.tez.examples_old;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.functions.FunctionAnnotation;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.io.TextInputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.StringTokenizer;


public class FlinkWordCount {


    /**
     * Converts a Record containing one string in to multiple string/integer pairs.
     * The string is tokenized by whitespaces. For each token a new record is emitted,
     * where the token is the first field and an Integer(1) is the second field.
     */
    public static class TokenizeLine extends MapFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void map(Record record, Collector<Record> collector) {
            // get the first field (as type StringValue) from the record
            String line = record.getField(0, StringValue.class).getValue();

            // normalize the line
            line = line.replaceAll("\\W+", " ").toLowerCase();

            // tokenize the line
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();

                // we emit a (word, 1) pair
                collector.collect(new Record(new StringValue(word), new IntValue(1)));
            }
        }
    }

    /**
     * Sums up the counts for a certain given key. The counts are assumed to be at position <code>1</code>
     * in the record. The other fields are not modified.
     */
    @ReduceOperator.Combinable
    @FunctionAnnotation.ConstantFields(0)
    public static class CountWords extends ReduceFunction {

        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
            Record element = null;
            int sum = 0;
            while (records.hasNext()) {
                element = records.next();
                int cnt = element.getField(1, IntValue.class).getValue();
                sum += cnt;
            }

            element.setField(1, new IntValue(sum));
            out.collect(element);
        }

        @Override
        public void combine(Iterator<Record> records, Collector<Record> out) throws Exception {
            // the logic is the same as in the reduce function, so simply call the reduce method
            reduce(records, out);
        }
    }



    public Plan getPlan(String... args) {
        // parse job parameters
        int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
        String dataInput = (args.length > 1 ? args[1] : "");
        String output    = (args.length > 2 ? args[2] : "");

        FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput, "Input Lines");
        MapOperator mapper = MapOperator.builder(new TokenizeLine())
                .input(source)
                .name("Tokenize Lines")
                .build();
        ReduceOperator reducer = ReduceOperator.builder(CountWords.class, StringValue.class, 0)
                .input(mapper)
                .name("Count Words")
                .build();

        @SuppressWarnings("unchecked")
        FileDataSink out = new FileDataSink(new CsvOutputFormat("\n", " ", StringValue.class, IntValue.class), output, reducer, "Word Counts");

        Plan plan = new Plan(out, "WordCount Example");
        plan.setDefaultParallelism(numSubTasks);
        return plan;
    }


    public String getDescription() {
        return "Parameters: <numSubStasks> <input> <output>";
    }


}
