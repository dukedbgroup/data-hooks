package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Scheduler;
import edu.duke.dbmsplus.datahooks.yarnmetrics.statsd.StatsDLogger;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.HttpGetHandler;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.PropsParser;

import java.util.Random;

/**
 * A daemon that gathers information about the scheduler from the RM rest api. 
 * The run method launches a new thread that gathers information periodically 
 * and posts messages to statsd using the logging api.
 * @author rahulswaminathan, Xiaodan
 */
public class SchedulerDaemon {

	private SchedulerThread runnable;
    private Thread thread;
    
    public SchedulerDaemon() {

    }

    public void run() {
        runnable = new SchedulerThread();
        new Thread(runnable).start();
        thread = new Thread(runnable);
        thread.start();
    }

	public void stop() {
		if (thread != null) {
			runnable.terminate();
		}
	    try {
	         thread.join();
	         } 
	    catch (InterruptedException e) {
	         e.printStackTrace();
	         }
	}
}

class SchedulerThread implements Runnable {

    private volatile boolean running = true;
    private static int WAIT_TIME = 250;
    private StatsDLogger logger;

    public SchedulerThread() {
        logger = new StatsDLogger();
    }


    public void terminate() {
		// TODO Auto-generated method stub
		running = false;
	}


	public void run() {
        PropsParser pp = new PropsParser();
        String url = "http://" + pp.getYarnWEBUI() + "/ws/v1/cluster/scheduler";
        HttpGetHandler hgh = new HttpGetHandler(url);
        System.out.println("scheduler daemon is running");

        while (running) {
            try {
                Thread.sleep(WAIT_TIME);
                String schedulerResponse = hgh.sendGet();
                Scheduler.queue[] list = readClusterSchedulerJsonResponse(schedulerResponse);
                logger.logGauge("totalContainers", getTotalContainers(list));
                logger.logGauge("totalActiveApplications", getTotalActiveApplications(list));
                logger.logGauge("totalApplications", getTotalApplications(list));
                logger.logGauge("maxApplications", getMaxApplications(list));

                //System.out.println(schedulerResponse);
                /// SHOULD POST MESSAGES TO KAFKA

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private Scheduler.queue[] readClusterSchedulerJsonResponse(String clusterSchedulerResponse) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        JsonNode node = mapper.readTree(clusterSchedulerResponse);
        node = node.get("scheduler").get("schedulerInfo").get("queues").get("queue");
        TypeReference<Scheduler.queue[]> typeRef = new TypeReference<Scheduler.queue[]>() {};
        return mapper.readValue(node.traverse(), typeRef);
    }

    private int getTotalContainers(Scheduler.queue[] list) {
        int totalContainers = 0;
        for (Scheduler.queue q : list) {
            totalContainers += q.getNumContainers();
        }
        return totalContainers;
    }

    private int getTotalActiveApplications(Scheduler.queue[] list) {
        int totalActiveApps = 0;
        for (Scheduler.queue q : list) {
            totalActiveApps += q.getNumActiveApplications();
        }
        return totalActiveApps;
    }

    private int getTotalApplications(Scheduler.queue[] list) {
        int totalApps = 0;
        for (Scheduler.queue q : list) {
            totalApps += q.getNumApplications();
        }
        return totalApps;
    }

    private int getMaxApplications(Scheduler.queue[] list) {
        int maxApps = 0;
        for (Scheduler.queue q : list) {
            maxApps += q.getMaxApplications();
        }
        return maxApps;
    }
}