package org.apache.flink.statesharing.examples;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;

/**
 * Created by affo on 31/10/17.
 */
public class LocalQueryableStateEnvironment extends LocalStreamEnvironment {
	private Configuration config;
	public static final int NUM_TMS = 2;
	public static final int NUM_SLOTS_PER_TM = 4;
	public static final int PROXY_PORT_START = 9084;
	public static final int SERVER_PORT_START = 9089;

	public LocalQueryableStateEnvironment(Configuration config) {
		super(config);
		this.config = config;
	}

	public static LocalQueryableStateEnvironment createLocalEnvironment(Configuration config) {
		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 4L);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
		config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1);
		config.setBoolean(QueryableStateOptions.SERVER_ENABLE, true);
		config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1);
		config.setString(QueryableStateOptions.PROXY_PORT_RANGE, PROXY_PORT_START + "-" + (PROXY_PORT_START + NUM_TMS));
		config.setString(QueryableStateOptions.SERVER_PORT_RANGE, SERVER_PORT_START + "-" + (SERVER_PORT_START + NUM_TMS));
		LocalQueryableStateEnvironment env = new LocalQueryableStateEnvironment(config);
		env.setParallelism(NUM_SLOTS_PER_TM * NUM_TMS);
		return env;
	}

	@Override
	public JobExecutionResult execute() throws Exception {
		JobGraph jobGraph = getStreamGraph().getJobGraph();
		jobGraph.setExecutionConfig(getConfig());

		Configuration configuration = new Configuration();
		configuration.addAll(jobGraph.getJobConfiguration());

		configuration.addAll(this.config);

		LocalFlinkMiniCluster exec = new LocalFlinkMiniCluster(configuration, false);

		try {
			exec.start();
			return exec.submitJobAndWait(jobGraph, getConfig().isSysoutLoggingEnabled());
		} finally {
			transformations.clear();
			exec.stop();
		}
	}
}
