package org.apache.flink.tez.examples;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.costs.DefaultCostEstimator;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.tez.environment.TezExecutionEnvironment;
import org.apache.flink.tez.plan.TezDAGGenerator;
import org.apache.flink.tez.wordcount_old.ProgramLauncher;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;


public class PiEstimation {

    public static void main(String[] args) throws Exception {

        final long numSamples = args.length > 0 ? Long.parseLong(args[0]) : 1000000;

        final ExecutionEnvironment env = new TezExecutionEnvironment(true);

        // count how many of the samples would randomly fall into
        // the unit circle
        DataSet<Long> count =
                env.generateSequence(1, numSamples)
                        .map(new Sampler())
                        .reduce(new SumReducer());

        // the ratio of the unit circle surface to 4 times the unit square is pi
        DataSet<Double> pi = count
                .map(new MapFunction<Long, Double>() {
                    public Double map(Long value) {
                        return value * 4.0 / numSamples;
                    }
                });

        System.out.println("We estimate Pi to be:");
        pi.print();

        env.execute();
    }

    //*************************************************************************
    //     USER FUNCTIONS
    //*************************************************************************


    /**
     * Sampler randomly emits points that fall within a square of edge x * y.
     * It calculates the distance to the center of a virtually centered circle of radius x = y = 1
     * If the distance is less than 1, then and only then does it returns a 1.
     */
    public static class Sampler implements MapFunction<Long, Long> {

        @Override
        public Long map(Long value) throws Exception{
            double x = Math.random();
            double y = Math.random();
            return (x * x + y * y) < 1 ? 1L : 0L;
        }
    }


    /**
     * Simply sums up all long values.
     */
    public static final class SumReducer implements ReduceFunction<Long>{

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }

}
