package org.apache.flink.tez.examples_old;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.tez.environment.TezExecutionEnvironment;


public class UnionTest {

    public static void main (String [] args) throws Exception {

        ExecutionEnvironment env = new TezExecutionEnvironment(true);

        DataSet<String> hamlet = env.readTextFile("/tmp/hamlet.txt");
        DataSet<String> sherlock = env.readTextFile("/tmp/sherlock.txt");
        DataSet<String> both = hamlet.union(sherlock);
        DataSet<String> filtered = both
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.contains("a");
                    }
                });

        filtered.writeAsText("/tmp/union_out");

        env.execute();
    }

}
