package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

import edu.duke.dbmsplus.datahooks.yarnmetrics.sqlwriter.StatsDSQLWriter;

/**
 * Created by rahulswaminathan on 3/31/15.
 * TODO change to a method that can be called in BigFrames before engine running.
 */
public class LaunchDaemons {

    public static void main(String[] args) {

        new ClusterMetricsDaemon().run();
        new SchedulerDaemon().run();
        new MonitorApplicationsDaemon().run();
        new StatsDSQLWriter().run();

    }
}
