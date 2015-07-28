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
            thread.interrupt();
        }
	}
}

class SchedulerThread implements Runnable {

    private volatile boolean running = true;
    private static int WAIT_TIME = 250;
//    private StatsDLogger logger;
    private Scheduler.queue[] lastState;
    private SQLWrapper schedulerMetricsWriter;

    public SchedulerThread() {
//        logger = new StatsDLogger();
        schedulerMetricsWriter = new SQLWrapper();
    }


    public void terminate() {
		running = false;
	}
    
    public void initCurrent(HttpGetHandler hgh){
    	String schedulerResponse = hgh.sendGet();
        try {
			lastState = readClusterSchedulerJsonResponse(schedulerResponse);
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
            	Thread.sleep(WAIT_TIME);
            	long recordTime = System.currentTimeMillis();
                String schedulerResponse2 = hgh.sendGet();
//                System.out.println(schedulerResponse);
                Scheduler.queue[] list = readClusterSchedulerJsonResponse(schedulerResponse2);
                updateSchedulerTable(lastState, list, recordTime);
  
                lastState = list;
                
//                logger.logGauge("totalContainers", getTotalContainers(list));
//                logger.logGauge("totalActiveApplications", getTotalActiveApplications(list));
//                logger.logGauge("totalApplications", getTotalApplications(list));
//                logger.logGauge("maxApplications", getMaxApplications(list));
                /// SHOULD POST MESSAGES TO KAFKA
               

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Scheduler Listener existed.");
    }

    private Scheduler.queue[] readClusterSchedulerJsonResponse(String clusterSchedulerResponse) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        JsonNode node = mapper.readTree(clusterSchedulerResponse);
        node = node.get("scheduler").get("schedulerInfo").get("queues").get("queue");
        TypeReference<Scheduler.queue[]> typeRef = new TypeReference<Scheduler.queue[]>() {};
        return mapper.readValue(node.traverse(), typeRef);
    }
    
    private void updateSchedulerTable(queue[] oldMetrics, queue[] newMetrics, long recordTime) throws Exception {
    	int length = oldMetrics.length;
    	//compare every queue
    	for (int i = 0; i < length; i++){
    		Class cls = oldMetrics[i].getClass();
    		Field[] fields = cls.getDeclaredFields(); 		
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
    			Object oldVal = fields[j].get(oldMetrics[i]);
    			Object newVal = fields[j].get(newMetrics[i]);
    			
    			if (fields[j].getName() == "resourcesUsed") {
//				System.out.println("Compare resourcesUsed...");
				compareResourcesUsed((resource)oldVal, (resource)newVal, queueName, recordTime);
    			}
    			else if (fields[j].getName() == "users") {
    				//TODO: this part needs to rewrite.
//				System.out.println("Compare users...");
//				compareUser((user)oldVal, (user)newVal, queueName, recordTime);
    			}
    			else if (oldVal == null || newVal == null) {
    				continue;
    			}
    			else if(!(fields[j].get(oldMetrics[i]).toString().equals(fields[j].get(newMetrics[i]).toString()))) {
//    				System.out.println("compare start------------------------");
//        			System.out.println(j + "th Type:" + fields[j].getType().toString() +
//        					"\nfield name:" + fields[j].getName() + "\noldvalue:" + 
//        					fields[j].get(oldMetrics[i]) + "\nnewvalue:" + 
//        					fields[j].get(newMetrics[i]));
//        			System.out.println("compare end------------------------");
        			schedulerMetricsWriter.writeSchedulerTable(queueName, fields[j].getName(), recordTime, newVal.toString());
    			}
    		}
    	}
    }
    
    private void compareResourcesUsed(resource oldVal, 
    		resource newVal, String queueName, long recordTime) {
    	if (oldVal == null || newVal == null) {
    		return;
    	}
    	if (oldVal.getMemory() != newVal.getMemory()){
//			System.out.println("resourcesUsed_memory need to update!");
			schedulerMetricsWriter.writeSchedulerTable(queueName, "resourcesUsed_memory", recordTime, Integer.toString(newVal.getMemory()));
		}
		if (oldVal.getvCores() != newVal.getvCores()){
//			System.out.println("resourcesUsed_vCores need to update!");
			schedulerMetricsWriter.writeSchedulerTable(queueName, "resourcesUsed_vCores", recordTime, Integer.toString(newVal.getvCores()));
		}
    }
    
//    private void compareUser(user oldVal, user newVal, String queueName, long recordTime) {
//        if (oldVal == null && newVal != null) {
//        	System.out.println("new user comes!");
//        	System.out.println(newVal.getUsername() + "\n" + newVal.getNumActiveApplications() + "\n" + newVal.getNumActiveApplications());
//        	schedulerMetricsWriter.writeSchedulerTable(queueName, "user_username", recordTime, newVal.getUsername().toString());
//        	schedulerMetricsWriter.writeSchedulerTable(queueName, "user_NumActiveApplications", recordTime, Integer.toString(newVal.getNumActiveApplications()));
//        	schedulerMetricsWriter.writeSchedulerTable(queueName, "user_NumActiveApplications", recordTime, Integer.toString(newVal.getNumActiveApplications()));
//        }
//        else if (oldVal == null && newVal == null) {
//        	return;
//        }
//        else if (oldVal != null && newVal == null) {
//        	
//        }
//        else {
//	    	if (oldVal.getUsername() != newVal.getUsername()) {
//	        	System.out.println("users_username need to update!");
//	        	schedulerMetricsWriter.writeSchedulerTable(queueName, "user_username", recordTime, newVal.getUsername().toString());
//	        }
//	        if (oldVal.getNumActiveApplications() != newVal.getNumActiveApplications()) {	
//	        	System.out.println("users_numActiveApplications need to update!");
//	        	schedulerMetricsWriter.writeSchedulerTable(queueName, "user_NumActiveApplications", recordTime, Integer.toString(newVal.getNumActiveApplications()));
//	        }
//	        if (oldVal.getNumPendingApplications() != newVal.getNumActiveApplications()) {
//	        	System.out.println("users_numPendingApplications need to update!");
//	        	schedulerMetricsWriter.writeSchedulerTable(queueName, "user_NumActiveApplications", recordTime, Integer.toString(newVal.getNumActiveApplications()));
//	        }
//	        compareResourcesUsed(oldVal.getResourcesUsed(), newVal.getResourcesUsed(), queueName, recordTime);
//        }
//    }
    

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