package org.apache.flink.tez.environment;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.costs.DefaultCostEstimator;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.tez.plan.TezDAGGenerator;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

public class TezExecutionEnvironment extends ExecutionEnvironment{

    TezConfiguration tezConf;

    public TezExecutionEnvironment(boolean local) {
        if (local) {
            this.tezConf = new TezConfiguration();
            tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
            tezConf.set("fs.defaultFS", "file:///");
            tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
        }
        else {
            tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, false);
            tezConf.set("fs.default.name", "hdfs://localhost:9000");
            tezConf.set("tez.lib.uris", "${fs.default.name}/apps/tez-0.6.0-SNAPSHOT/tez-0.6.0-SNAPSHOT.tar.gz");
        }
    }

    @Override
    public JobExecutionResult execute(String jobName) throws Exception {
        try {

            TezClient tezClient = TezClient.create(jobName, tezConf);

            tezClient.start();

            try {
                Plan p = createProgramPlan(jobName);
                PactCompiler compiler = new PactCompiler(null, new DefaultCostEstimator());
                OptimizedPlan plan = compiler.compile(p);
                TezDAGGenerator dagGenerator = new TezDAGGenerator(tezConf, new Configuration());
                DAG dag = dagGenerator.createDAG(plan);


                tezClient.waitTillReady();
                System.out.println("Submitting DAG to Tez Client");
                DAGClient dagClient = tezClient.submitDAG(dag);
                System.out.println("Submitted DAG to Tez Client");

                // monitoring
                DAGStatus dagStatus = dagClient.waitForCompletion();

                if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
                    System.out.println(jobName + " failed with diagnostics: " + dagStatus.getDiagnostics());
                    System.exit(1);
                }
                System.out.println(jobName + " finished successfully");
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
        finally {
            return null;
        }
    }

    @Override
    public String getExecutionPlan() throws Exception {
        return null;
    }


}
