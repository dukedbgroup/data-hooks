package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import edu.duke.dbmsplus.datahooks.yarnmetrics.listener.ApplicationListener.AppThread;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.ClusterMetrics;
import edu.duke.dbmsplus.datahooks.yarnmetrics.statsd.StatsDLogger;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.HttpGetHandler;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.PropsParser;

import org.codehaus.jackson.map.ObjectMapper;

import java.util.Random;
import java.lang.reflect.Field;

/**
 * This class listen to the change of cluster metrics.
 * Write the metrics to MySQL.
 * Send the changes of metrics to StatsD.
 * @author rahulswaminathan, Xiaodan
 */
public class ClusterMetricsDaemon {

    private static final String PREFIX = "my.prefix";
    private static final String SERVER_LOCATION = "localhost";
    private static final int PORT = 8125;
    private ClusterMetricsThread runnable;
    private Thread thread;

    /**
     * Daemon that uses the RM rest api to get information pertaining to cluster metrics. The daemon is started using the
     * run method which launches the listener in a new thread. Information is sent to statsd using the logging api.
     */
    public ClusterMetricsDaemon() {

    }

    public void run() {
        runnable = new ClusterMetricsThread();
        new Thread(runnable).start();
        thread = new Thread(runnable);
        thread.start();
    }

	public void stop() {
		if (thread != null) {
            runnable.terminate();
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
	}
}

class ClusterMetricsThread implements Runnable {

    private volatile boolean running = true;
    private ClusterMetrics current;
    private static int WAIT_TIME = 1000;
    private StatsDLogger logger;

    public ClusterMetricsThread() {
        logger = new StatsDLogger();
    }
    
    private void initCurrent(HttpGetHandler hgh) {
    	try {
    	String clusterMetricsResponse1 = hgh.sendGet();
        ObjectMapper mapper1 = new ObjectMapper();
        current = mapper1.readValue(clusterMetricsResponse1, ClusterMetrics.class);
        System.out.println(clusterMetricsResponse1);
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}
    }

    public void run() {

        PropsParser pp = new PropsParser();
        String url = "http://" + pp.getYarnWEBUI() + "/ws/v1/cluster/metrics";
        HttpGetHandler hgh = new HttpGetHandler(url);
        System.out.println("Cluster metrics daemon is running");
        initCurrent(hgh);
        
        while (running) {
            try {
                Thread.sleep(WAIT_TIME);
                String clusterMetricsResponse = hgh.sendGet();
                ObjectMapper mapper = new ObjectMapper();
                ClusterMetrics metrics = mapper.readValue(clusterMetricsResponse, ClusterMetrics.class);
                updateClusterTable(current, metrics);
                current = metrics;
                //System.out.println(clusterMetricsResponse);
                
                logger.logGauge("allocatedMB", (int) metrics.getClusterMetrics().getAllocatedMB());
                logger.logGauge("appsCompleted", metrics.getClusterMetrics().getAppsCompleted());
                logger.logGauge("appsSubmitted", metrics.getClusterMetrics().getAppsSubmitted());
                logger.logGauge("appsRunning", metrics.getClusterMetrics().getAppsRunning());
                logger.logGauge("availableMB", (int) metrics.getClusterMetrics().getAvailableMB());
                logger.logGauge("activeNodes", metrics.getClusterMetrics().getActiveNodes());
                logger.logGauge("totalNodes", metrics.getClusterMetrics().getTotalNodes());
                logger.logGauge("appsFailed", metrics.getClusterMetrics().getAppsFailed());
                logger.logGauge("containersAllocated", metrics.getClusterMetrics().getContainersAllocated());
                /// SHOULD POST MESSAGES TO KAFKA

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("while loop in cluster metrics daemon exited for some reason");
    }
    
    private void updateClusterTable(ClusterMetrics oldMetrics, ClusterMetrics newMetrics) throws Exception {
    	Class cls = oldMetrics.getClusterMetrics().getClass();
    	Field[] fields = cls.getDeclaredFields();
    	long startTime = System.currentTimeMillis();
    	for (int i = 0; i < fields.length - 1; i++) {
//    		System.out.println(f.toString());
    		fields[i].setAccessible(true);
    		Object oldVal = fields[i].get(oldMetrics.getClusterMetrics());
//    		System.out.println(oldVal.toString());
    		Object newVal = fields[i].get(newMetrics.getClusterMetrics());
//    		System.out.println(oldVal +"===" + newVal);
    		if (!oldVal.toString().equals(newVal.toString())) {
    			//TODO: update to MySQL
    			System.out.println("Update: The field:" + fields[i].getName() + "\nold value: " + oldVal +" \nnew value: " + newVal + "\nTime:" + startTime);
    		}
    	}
    }
    public void terminate() {
    	running = false;
    }
}