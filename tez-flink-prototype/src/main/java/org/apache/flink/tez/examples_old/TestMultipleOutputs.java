package org.apache.flink.tez.examples_old;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.tez.environment.TezExecutionEnvironment;

/**
 * Created by kostas on 01/10/14.
 */
public class TestMultipleOutputs {

    public static void main (String [] args) throws Exception {

        final ExecutionEnvironment env = new TezExecutionEnvironment(true);

        DataSet<String> hamlet = env.readTextFile("/tmo/hamlet.txt");

        DataSet<String> out = hamlet
                .filter (new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.contains("a");
                    }
                })
                .join(hamlet)
                .where(new KeySelector<String, Object>() {
                    @Override
                    public Object getKey(String value) throws Exception {
                        return value;
                    }
                })
                .equalTo(new KeySelector<String, Object>() {
                    @Override
                    public Object getKey(String value) throws Exception {
                        return value;
                    }
                })
                .with(new JoinFunction<String, String, String>() {
                    @Override
                    public String join(String first, String second) throws Exception {
                        return first;
                    }
                });

        out.print();

        env.execute();

    }

}
