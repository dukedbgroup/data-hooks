package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Scheduler;

import java.util.Random;

/**
 * Created by rahulswaminathan on 1/30/15.
 */
public class SchedulerDaemon {

    /**
     * A daemon that gathers information about the scheduler from the RM rest api. The run method launches a new thread
     * that gathers information periodically and posts messages to statsd using the logging api.
     */
    public SchedulerDaemon() {

    }

    public void run() {
        Runnable run = new SchedulerThread();
        new Thread(run).start();
    }
}

class SchedulerThread implements Runnable {

    private volatile boolean running = true;
    private static int WAIT_TIME = 250;
    private StatsDLogger logger;

    public SchedulerThread() {
        logger = new StatsDLogger();
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