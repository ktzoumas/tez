package org.apache.flink.tez.examples;


import org.apache.flink.api.common.Plan;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.costs.DefaultCostEstimator;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.tez.plan.TezDAGGenerator;
import org.apache.flink.tez.wordcount_old.ProgramLauncher;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;

public class GeneratedWordCount extends ProgramLauncher {

    private static String DOP = String.valueOf(8);

    public static String INPUT_FILE="/tmp/sherlock.txt";

    public static String OUTPUT_FILE="/tmp/job_output17";

    public static void main (String [] args) {
        new GeneratedWordCount().runLocal();
    }

    public GeneratedWordCount() {
        super("Generated Word Count");
    }

    @Override
    public DAG createDAG(TezConfiguration tezConf) throws Exception {

        PactCompiler compiler = new PactCompiler(null, new DefaultCostEstimator());

        FlinkWordCount wc = new FlinkWordCount();
        Plan p = wc.getPlan(DOP, INPUT_FILE, OUTPUT_FILE);

        OptimizedPlan plan = compiler.compile(p);

        TezDAGGenerator generator = new TezDAGGenerator(tezConf, new Configuration());

        DAG dag = generator.createDAG(plan);

        return dag;
    }
}
