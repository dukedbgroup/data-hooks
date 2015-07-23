package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Scheduler;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Scheduler.queue;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Scheduler.resource;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Scheduler.user;
import edu.duke.dbmsplus.datahooks.yarnmetrics.sqlwriter.SQLWrapper;
import edu.duke.dbmsplus.datahooks.yarnmetrics.statsd.StatsDLogger;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.HttpGetHandler;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.PropsParser;

import java.lang.reflect.Field;
import java.util.Random;

/**
 * A daemon that gathers information about the scheduler from the RM rest api. 
 * The run method launches a new thread that gathers information periodically 
 * and posts messages to statsd using the logging api.
 * 
 * NOTE: only support root queue.
 * @author rahulswaminathan, Xiaodan
 */
public class SchedulerDaemon {

	private SchedulerThread runnable;
    private Thread thread;
    
    public SchedulerDaemon() {

    }

    public void run() {
        runnable = new SchedulerThread();
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
    private Scheduler.queue[] current;
    private SQLWrapper schedulerMetricsWriter;

    public SchedulerThread() {
        logger = new StatsDLogger();
        schedulerMetricsWriter = new SQLWrapper();
    }


    public void terminate() {
		// TODO Auto-generated method stub
		running = false;
	}
    
    public void initCurrent(HttpGetHandler hgh){
    	String schedulerResponse = hgh.sendGet();
        try {
			current = readClusterSchedulerJsonResponse(schedulerResponse);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }


	public void run() {
        PropsParser pp = new PropsParser();
        String url = "http://" + pp.getYarnWEBUI() + "/ws/v1/cluster/scheduler";
        HttpGetHandler hgh = new HttpGetHandler(url);
        schedulerMetricsWriter.createSchedulerTable();
        initCurrent(hgh);
        System.out.println("scheduler daemon is running");

        while (running) {
            try {
                
                String schedulerResponse = hgh.sendGet();
                System.out.println(schedulerResponse);
                Scheduler.queue[] list = readClusterSchedulerJsonResponse(schedulerResponse);
                updateSchedulerTable(current, list);
                current = list;
                
                logger.logGauge("totalContainers", getTotalContainers(list));
                logger.logGauge("totalActiveApplications", getTotalActiveApplications(list));
                logger.logGauge("totalApplications", getTotalApplications(list));
                logger.logGauge("maxApplications", getMaxApplications(list));
                /// SHOULD POST MESSAGES TO KAFKA
                Thread.sleep(WAIT_TIME);

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
    
    private void updateSchedulerTable(queue[] oldMetrics, queue[] newMetrics) throws Exception {
    	int length = oldMetrics.length;
    	//compare every queue
    	for (int i = 0; i < length; i++){
    		Class cls = oldMetrics[i].getClass();
    		System.out.println("Metrics info:");
    		System.out.println(cls.toString());
    		Field[] fields = cls.getDeclaredFields();
    		System.out.println("length of Field:" + fields.length);
    		long recordTime = System.currentTimeMillis();
    		String queueName = null;
    		//find queueName
    		for (int j = 0; j < fields.length; j++) {
    			if (fields[j].getName().toString()=="queueName") {
    				fields[j].setAccessible(true);
    				queueName = fields[j].get(newMetrics[i]).toString();
    			}
    		}
    		//Compare value of every field
    		for (int j = 0; j <fields.length; j++) {
    			fields[j].setAccessible(true);
    			System.out.println(j + "th Type:" + fields[j].getType().toString() + "\nfield name:" + fields[j].getName() + "\nvalue:" + fields[j].get(oldMetrics[i]));
    			Object oldVal = fields[j].get(oldMetrics[i]);
    			Object newVal = fields[j].get(newMetrics[i]);
    			if (oldVal == null || newVal == null) {
    				continue;
    			}
    			//compare resource object
    			else if (fields[j].getName().toString() == "resourcesUsed") {
    				System.out.println("Compare resourcesUsed...");
    				compareResourcesUsed((resource)oldVal, (resource)newVal, queueName, recordTime);
    			}
    			//compare user object
    			else if (fields[j].getName().toString() == "user") {
    				System.out.println("Compare resourcesUsed...");
    				compareUser((user)oldVal, (user)newVal, queueName, recordTime);
    			}
    			//compare others
    			else if (!oldVal.toString().equals(newVal.toString())){
    				System.out.println("==========\nQueueName:" + queueName + "Update: The field:" + fields[j].getName() 
    						+ "\nold value: " + oldVal +" \nnew value: " + newVal +
    						"\nTime:" + recordTime + "\n==========");
    				schedulerMetricsWriter.writeSchedulerTable(queueName, fields[j].getName(), recordTime, newVal.toString());
    			}
    		}
    	}
    }
    
    private void compareResourcesUsed(resource oldVal, resource newVal, String queueName, long recordTime) {
    	if (!(oldVal.getMemory() == newVal.getMemory())){
			//write to SQL
			System.out.println("resourcesUsed need to update!");
			schedulerMetricsWriter.writeSchedulerTable(queueName, "resourcesUsed_memory", recordTime, Integer.toString(newVal.getMemory()));
		}
		if (!(oldVal.getvCores() == newVal.getvCores())){
			//write to SQL
			System.out.println("resourcesUsed need to update!");
			schedulerMetricsWriter.writeSchedulerTable(queueName, "resourcesUsed_vCores", recordTime, Integer.toString(newVal.getvCores()));
		}
    }
    
    private void compareUser(user oldVal, user newVal, String queueName, long recordTime) {
        if (!(oldVal.getUsername() == newVal.getUsername())) {
        	//write to SQL
        	schedulerMetricsWriter.writeSchedulerTable(queueName, "user_username", recordTime, newVal.getUsername().toString());
        }
        if (!(oldVal.getNumActiveApplications() == newVal.getNumActiveApplications())) {
        	//write to SQL
        	schedulerMetricsWriter.writeSchedulerTable(queueName, "user_NumActiveApplications", recordTime, Integer.toString(newVal.getNumActiveApplications()));
        }
        if (!(oldVal.getNumPendingApplications() == newVal.getNumActiveApplications())) {
        	//write to SQL
        	schedulerMetricsWriter.writeSchedulerTable(queueName, "user_NumActiveApplications", recordTime, Integer.toString(newVal.getNumActiveApplications()));
        }
        compareResourcesUsed(oldVal.getResourcesUsed(), newVal.getResourcesUsed(), queueName, recordTime);
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