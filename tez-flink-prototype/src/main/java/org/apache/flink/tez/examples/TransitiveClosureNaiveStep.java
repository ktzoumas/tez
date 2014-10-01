package org.apache.flink.tez.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.tez.environment.TezExecutionEnvironment;
import org.apache.flink.tez.examples.util.ConnectedComponentsData;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class TransitiveClosureNaiveStep implements ProgramDescription {


    public static void main (String... args) throws Exception{

        if (!parseParameters(args)) {
            return;
        }

        // set up execution environment
        ExecutionEnvironment env = new TezExecutionEnvironment(true);

        DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env);

        DataSet<Tuple2<Long,Long>> nextPaths = edges
                .join(edges)
                .where(1)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    /**
                     left: Path (z,x) - x is reachable by z
                     right: Edge (x,y) - edge x-->y exists
                     out: Path (z,y) - y is reachable by z
                     */
                    public Tuple2<Long, Long> join(Tuple2<Long, Long> left, Tuple2<Long, Long> right) throws Exception {
                        return new Tuple2<Long, Long>(
                                new Long(left.f0),
                                new Long(right.f1));
                    }
                })
                .union(edges)
                .groupBy(0, 1)
                .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
                        out.collect(values.iterator().next());
                    }
                });

        // emit result
        if (fileOutput) {
            nextPaths.writeAsCsv(outputPath, "\n", " ");
        } else {
            nextPaths.print();
        }

        // execute program
        env.execute("Transitive Closure Example");

    }

    @Override
    public String getDescription() {
        return "Parameters: <edges-path> <result-path> <max-number-of-iterations>";
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String edgesPath = null;
    private static String outputPath = null;
    private static int maxIterations = 10;

    private static boolean parseParameters(String[] programArguments) {

        if (programArguments.length > 0) {
            // parse input arguments
            fileOutput = true;
            if (programArguments.length == 3) {
                edgesPath = programArguments[0];
                outputPath = programArguments[1];
                maxIterations = Integer.parseInt(programArguments[2]);
            } else {
                System.err.println("Usage: TransitiveClosure <edges path> <result path> <max number of iterations>");
                return false;
            }
        } else {
            System.out.println("Executing TransitiveClosure example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: TransitiveClosure <edges path> <result path> <max number of iterations>");
        }
        return true;
    }


    private static DataSet<Tuple2<Long, Long>> getEdgeDataSet(ExecutionEnvironment env) {

        if(fileOutput) {
            return env.readCsvFile(edgesPath).fieldDelimiter(' ').types(Long.class, Long.class);
        } else {
            return ConnectedComponentsData.getDefaultEdgeDataSet(env);
        }
    }

}

