package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

import edu.duke.dbmsplus.datahooks.yarnmetrics.sqlwriter.StatsDSQLWriter;

/**
 * Created by rahulswaminathan on 3/31/15.
 * TODO change to a method that can be called in BigFrames before engine running.
 */
public class LaunchDaemons {
	
	private ClusterMetricsDaemon cmDaemon;
	private SchedulerDaemon schDaemon;
	private ApplicationListener appDaemon;
	private NodesListener nodesDaemon;
//	private StatsDSQLWriter sqlWriter; 

	public LaunchDaemons() {
		
		cmDaemon = new ClusterMetricsDaemon();
        schDaemon = new SchedulerDaemon();
        appDaemon = new ApplicationListener();
        nodesDaemon = new NodesListener();
//        sqlWriter = new StatsDSQLWriter();
	}
	
    public void startDaemons() {

        cmDaemon.run();
        schDaemon.run();
        appDaemon.run();
        nodesDaemon.run();
//        sqlWriter.run();

    }
    
    public void stopDaemons() {
    	
    	cmDaemon.stop();
        schDaemon.stop();
        appDaemon.stop();
        nodesDaemon.stop();
//        sqlWriter.stop();
        return;
    }
}
