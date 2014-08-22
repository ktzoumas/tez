package org.apache.flink.tez;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;

public class TezUtils {

	private static final DecimalFormat formatter = new DecimalFormat("###.##%");


	public static void addAllTezConfigResources(Configuration conf) {
		// try to infer the hadoop base directory from the environment or the
		// working directory.
		String hadoopHome = System.getenv("HADOOP_HOME");
		if (hadoopHome == null) {
			hadoopHome = System.getenv("HADOOP_PREFIX");
			if (hadoopHome == null) {
				throw new RuntimeException("Hadoop home directory not set.");
			}
		}
		addAllTezConfigResources(conf, hadoopHome);
	}

	public static void addAllTezConfigResources(Configuration conf, String hadoopBaseDirectory) {
		if (conf == null) {
			throw new IllegalArgumentException("Configuration may not be null.");
		}
		if (hadoopBaseDirectory == null) {
			throw new IllegalArgumentException("Hadoop base directory path may not be null.");
		}
		File f = new File(hadoopBaseDirectory);
		if (!f.exists()) {
			throw new IllegalArgumentException("The given hadoop home directory does not exist.");
		}
		conf.addResource(new Path(hadoopBaseDirectory + "/etc/hadoop/core-site.xml"));
		conf.addResource(new Path(hadoopBaseDirectory + "/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path(hadoopBaseDirectory + "/etc/hadoop/mapred-site.xml"));
		conf.addResource(new Path(hadoopBaseDirectory + "/etc/hadoop/yarn-site.xml"));
		conf.addResource(new Path(hadoopBaseDirectory + "/etc/hadoop/tez-site.xml"));
	}

	public static void setTezTimeoutsHigh(TezConfiguration tezConf) {
		tezConf.setInt(TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS, 6000);
		tezConf.setInt(TezConfiguration.TEZ_SESSION_CLIENT_TIMEOUT_SECS, 6000);
	}

	public static void setApplicationMasterRemoteDebugProperties(TezConfiguration conf, int port) {
		String previousOpts = conf.get(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, null);
		String debugOpts = "-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=" + port + ",suspend=y";
		String opts = previousOpts == null ? debugOpts : (previousOpts + ' ' + debugOpts);

		conf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, opts);
	}


	public static Map<String, LocalResource> prepareConfigForExecutionWithJar(String jarPath, TezConfiguration tezConf, Credentials credentials, FileSystem fs) throws IOException {
		// set up the staging directory (for the jar file)
		Path stagingDir = new Path(fs.getWorkingDirectory(), UUID.randomUUID().toString());
		tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());
		TezClientUtils.ensureStagingDirExists(tezConf, stagingDir);

		// copy the jar file to the staging directory
		Path remoteJarPath = fs.makeQualified(new Path(stagingDir, "dag_job.jar"));
		fs.copyFromLocalFile(new Path(jarPath), remoteJarPath);
		FileStatus remoteJarStatus = fs.getFileStatus(remoteJarPath);
		TokenCache.obtainTokensForNamenodes(credentials, new Path[] { remoteJarPath }, tezConf);

		// create the local resources map
		Map<String, LocalResource> commonLocalResources = new TreeMap<String, LocalResource>();
		LocalResource dagJarLocalRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromPath(remoteJarPath), LocalResourceType.FILE,
				LocalResourceVisibility.APPLICATION, remoteJarStatus.getLen(), remoteJarStatus.getModificationTime());
		commonLocalResources.put("dag_job.jar", dagJarLocalRsrc);

		return commonLocalResources;
	}

	public static void waitForJobCompletionAndPrintStatus(DAGClient client, int pollingInterval, String... vertexNames)
			throws TezException, IOException
	{
		DAGStatus dagStatus = null;

		while (true) {
			dagStatus = client.getDAGStatus(null);
			if (dagStatus.getState() == DAGStatus.State.RUNNING || dagStatus.getState() == DAGStatus.State.SUCCEEDED
					|| dagStatus.getState() == DAGStatus.State.FAILED || dagStatus.getState() == DAGStatus.State.KILLED
					|| dagStatus.getState() == DAGStatus.State.ERROR) {
				break;
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {}
		}

		while (dagStatus.getState() == DAGStatus.State.RUNNING) {
			printDAGStatus(client, vertexNames);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}

			dagStatus = client.getDAGStatus(null);
		}

		printDAGStatus(client, vertexNames, true, true);
	}

	private static void printDAGStatus(DAGClient dagClient, String[] vertexNames) throws IOException, TezException {
		printDAGStatus(dagClient, vertexNames, false, false);
	}

	private static void printDAGStatus(DAGClient dagClient, String[] vertexNames, boolean displayDAGCounters, boolean displayVertexCounters)
			throws IOException, TezException
	{
		Set<StatusGetOpts> opts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
		DAGStatus dagStatus = dagClient.getDAGStatus((displayDAGCounters ? opts : null));
		Progress progress = dagStatus.getDAGProgress();

		double vProgressFloat = 0.0f;
		if (progress != null) {
			System.out.println("");
			System.out.println("DAG: State: "
					+ dagStatus.getState()
					+ " Progress: "
					+ (progress.getTotalTaskCount() < 0 ? formatter.format(0.0f) : formatter.format((double) (progress
					.getSucceededTaskCount()) / progress.getTotalTaskCount())));
			for (String vertexName : vertexNames) {
				VertexStatus vStatus = dagClient.getVertexStatus(vertexName, (displayVertexCounters ? opts : null));
				if (vStatus == null) {
					System.out.println("Could not retrieve status for vertex: " + vertexName);
					continue;
				}
				Progress vProgress = vStatus.getProgress();
				if (vProgress != null) {
					vProgressFloat = 0.0f;
					if (vProgress.getTotalTaskCount() == 0) {
						vProgressFloat = 1.0f;
					} else if (vProgress.getTotalTaskCount() > 0) {
						vProgressFloat = (double) vProgress.getSucceededTaskCount() / vProgress.getTotalTaskCount();
					}
					System.out.println("VertexStatus:" + " VertexName: "
							+ (vertexName.equals("ivertex1") ? "intermediate-reducer" : vertexName) + " Progress: "
							+ formatter.format(vProgressFloat));
				}
				if (displayVertexCounters) {
					TezCounters counters = vStatus.getVertexCounters();
					if (counters != null) {
						System.out.println("Vertex Counters for " + vertexName + ": " + counters);
					}
				}
			}
		}
		if (displayDAGCounters) {
			TezCounters counters = dagStatus.getDAGCounters();
			if (counters != null) {
				System.out.println("DAG Counters: " + counters);
			}
		}
	}
}
