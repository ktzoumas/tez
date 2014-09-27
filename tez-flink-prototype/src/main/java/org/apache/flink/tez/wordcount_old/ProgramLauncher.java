package org.apache.flink.tez.wordcount_old;

import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;


public abstract class ProgramLauncher {

    private String programName;

    protected ProgramLauncher(String programName) {
        this.programName = programName;
    }

    public abstract DAG createDAG(TezConfiguration tezConf) throws Exception;

    public void runLocal () {
        try {
            final TezConfiguration tezConf = new TezConfiguration();

            tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
            tezConf.set("fs.defaultFS", "file:///");
            tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

            TezClient tezClient = TezClient.create(this.programName, tezConf);

            tezClient.start();

            try {
                DAG dag = createDAG(tezConf);

                tezClient.waitTillReady();
                System.out.println("Submitting DAG to Tez Client");
                DAGClient dagClient = tezClient.submitDAG(dag);
                System.out.println("Submitted DAG to Tez Client");

                // monitoring
                DAGStatus dagStatus = dagClient.waitForCompletion();

                if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
                    System.out.println(this.programName + " failed with diagnostics: " + dagStatus.getDiagnostics());
                    System.exit(1);
                }
                System.out.println(this.programName + " finished successfully");
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

    public void runYarn () {
        try {
            final TezConfiguration tezConf = new TezConfiguration();

            tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, false);
            tezConf.set("fs.default.name", "hdfs://localhost:9000");
            tezConf.set("tez.lib.uris", "${fs.default.name}/apps/tez-0.6.0-SNAPSHOT/tez-0.6.0-SNAPSHOT.tar.gz");

            TezClient tezClient = TezClient.create(this.programName, tezConf);

            tezClient.start();

            try {
                DAG dag = createDAG(tezConf);

                tezClient.waitTillReady();
                System.out.println("Submitting DAG to Tez Client");
                DAGClient dagClient = tezClient.submitDAG(dag);
                System.out.println("Submitted DAG to Tez Client");

                // monitoring
                DAGStatus dagStatus = dagClient.waitForCompletion();

                if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
                    System.out.println(this.programName + " failed with diagnostics: " + dagStatus.getDiagnostics());
                    System.exit(1);
                }
                System.out.println(this.programName + " finished successfully");
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
