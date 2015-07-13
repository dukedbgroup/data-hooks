package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

/**
 * Created by rahulswaminathan on 10/21/14.
 */
import java.io.*;
import java.util.*;
import java.net.HttpURLConnection;
import java.net.URL;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
//import org.apache.spark.SparkConf;




import edu.duke.dbmsplus.datahooks.yarnmetrics.listener.ApplicationListener;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Apps;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.ClusterMetrics;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Scheduler;

@JsonIgnoreProperties(ignoreUnknown = true)

public class GetYarnMetrics {

    private final String USER_AGENT = "Mozilla/5.0";
    private String queuesAsString = "";
    private long startTime = 0;
    // num of times the while loop in doStats() is entered
    private int iterationNumber = 0;
    private float totalAllocatedMB = 0;
    private float averageAllocatedMB = 0;
    private String[] queues;
    private String emem;
    private String dmem;
    // total number of jobs each queue performs
    private Integer numIterations;
    private String sparkMaster;
    private String yarnWEBUI;

    private int numAppsFinished = 0;

//    public static void main(String[] args) throws Exception {
//
//        GetYarnMetrics m = new GetYarnMetrics();
//
//        m.start();
//    }

    public GetYarnMetrics() throws Exception {
    	// TODO read new properties here
        Properties props = new Properties();
        String propFileName = "conf.properties";

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        if (inputStream == null) {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }
        props.load(inputStream);

        queues = props.getProperty("queues").split(",");
        emem = props.getProperty("emem");
        dmem = props.getProperty("dmem");
        numIterations = Integer.parseInt(props.getProperty("numIterations"));
        sparkMaster = props.getProperty("sparkMaster");
        yarnWEBUI = props.getProperty("yarnWEBUI");
    }

    public String getYarnWEBUI() {
        return yarnWEBUI;
    }

    public void start() throws Exception{

        Runnable run = new StatsThread(numIterations, dmem, emem, sparkMaster, queues);
        new Thread(run).start();

        doStats(dmem, emem, queues);
    }

    /**
     * Write Metrics to file.
     * @param dmem
     * @param emem
     * @param queues
     * @throws Exception
     */
    private void doStats(String dmem, String emem, String ... queues) throws Exception{
        startTime = System.currentTimeMillis();

        queuesAsString = "";
        String filename = "data_" + dmem + "_" + emem + "_" + numIterations;
		System.out.println(filename);
        for (String str : queues) {
            filename += "_" + str;
            queuesAsString += str + " ";
        }
        BufferedWriter overallWriter = new BufferedWriter(new FileWriter(filename + ".txt", true));
        BufferedWriter schedulerWriter = new BufferedWriter(new FileWriter(filename + "_scheduler.txt", true));
        BufferedWriter metricsWriter = new BufferedWriter(new FileWriter(filename + "_metrics.txt", true));
        BufferedWriter totalWriter = new BufferedWriter(new FileWriter("overall_stats.txt", true));
        final BufferedWriter appStateWriter = new BufferedWriter(new FileWriter(filename + "_app_state.txt", true));

        GetYarnMetrics http = new GetYarnMetrics();

        overallWriter.write("executer memory: " + emem);
        overallWriter.newLine();
        overallWriter.write("driver memory: " + dmem);
        overallWriter.newLine();
        overallWriter.write("number of queues: " + queues.length);
        overallWriter.newLine();
        overallWriter.write("queues: " + queuesAsString);
        overallWriter.newLine();

        Date dateStartTime = new Date(startTime);

        writeMessage("Date started: " + dateStartTime, overallWriter, schedulerWriter, metricsWriter);
        makeNewLines(overallWriter, schedulerWriter, metricsWriter);
        boolean hasStarted = false;

        iterationNumber = 0;

        ApplicationListener applicationListener = new ApplicationListener() {
            @Override
            public void onAppBegin(Apps.app app) {
                try {
                    appStateWriter.newLine();
                    appStateWriter.newLine();
                    writeMessage(app.getId(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("STARTED", appStateWriter);
                    System.out.println("Started: " + app.getId());
                    appStateWriter.newLine();
                    writeMessage("IN QUEUE: " + app.getQueue(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("at time elapsed: " +  (System.currentTimeMillis() - startTime), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT STATE: " + app.getState(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT NUM CONTAINERS: " + app.getRunningContainers(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT MEMORY ALLOCATED IN MB: " + app.getAllocatedMB(), appStateWriter);
                    appStateWriter.newLine();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onAppFinish(Apps.app app) {
                try {
                    System.out.println("Finished: " + app.getId() + " " + numAppsFinished);
                    numAppsFinished++;
                    appStateWriter.newLine();
                    appStateWriter.newLine();
                    writeMessage(app.getId(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("FINISHED IN: " + app.getElapsedTime(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("IN QUEUE: " + app.getQueue(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("at time elapsed: " +  (System.currentTimeMillis() - startTime), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT STATE: " + app.getState(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT NUM CONTAINERS: " + app.getRunningContainers(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT MEMORY ALLOCATED IN MB: " + app.getAllocatedMB(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("TOTAL NUMBER OF FINISHED APPS SO FAR: " + numAppsFinished, appStateWriter);
                    appStateWriter.newLine();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onAppChangeState(Apps.app app) {
                try {
                    System.out.println("Changed state to: " + app.getState() + " " + app.getId());
                    appStateWriter.newLine();
                    appStateWriter.newLine();
                    writeMessage(app.getId(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CHANGED STATE TO: " + app.getState(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("IN QUEUE: " + app.getQueue(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("at time elapsed: " +  (System.currentTimeMillis() - startTime), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT STATE: " + app.getState(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT NUM CONTAINERS: " + app.getRunningContainers(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT MEMORY ALLOCATED IN MB: " + app.getAllocatedMB(), appStateWriter);
                    appStateWriter.newLine();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onAppChangeContainers(Apps.app app) {
                try {
                    System.out.println("Changed num containers to: " + app.getRunningContainers() + " " + app.getId());
                    appStateWriter.newLine();
                    appStateWriter.newLine();
                    writeMessage(app.getId(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CHANGED NUMBER OF RUNNING CONTAINERS TO: " + app.getRunningContainers(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("IN QUEUE: " + app.getQueue(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("at time elapsed: " +  (System.currentTimeMillis() - startTime), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT STATE: " + app.getState(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT NUM CONTAINERS: " + app.getRunningContainers(), appStateWriter);
                    appStateWriter.newLine();
                    writeMessage("CURRENT MEMORY ALLOCATED IN MB: " + app.getAllocatedMB(), appStateWriter);
                    appStateWriter.newLine();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        applicationListener.startListening();
        while (true) {
            Thread.sleep(2000);
            iterationNumber++;
            overallWriter.newLine();
            writeMessage(Integer.toString(iterationNumber), overallWriter, schedulerWriter, metricsWriter);
            makeNewLines(overallWriter, schedulerWriter, metricsWriter);
            String clusterMetricsResponse  = http.sendClusterMetricsGet();
            String clusterSchedulerResponse = http.sendClusterSchedulerGet();

            Scheduler.queue[] schedulerQueues = readClusterSchedulerJsonResponse(clusterSchedulerResponse);
            long currentTimeElapsed = System.currentTimeMillis() - startTime;
            writeMessage("current time elapsed in ms=" + currentTimeElapsed, overallWriter, schedulerWriter, metricsWriter);
            makeNewLines(overallWriter, schedulerWriter, metricsWriter);

            int numApps = getTotalApplications(schedulerQueues);
            writeMessage("Number of applications=" + numApps, overallWriter);
            overallWriter.newLine();
            writeMessage("Total number of containers=" + getTotalContainers(schedulerQueues), overallWriter);
            overallWriter.newLine();
            writeMessage("Number of applications finished=" + numAppsFinished, overallWriter);
            overallWriter.newLine();

            writeQueueInfoToFile(schedulerWriter, schedulerQueues);
            writeClusterMetrics(metricsWriter, clusterMetricsResponse);

            if (!hasStarted && numApps > 0)
                hasStarted = true;

            if (hasStarted && applicationListener.getAppsSet().isEmpty() && numAppsFinished == numIterations * queues.length) {
                applicationListener.stopListening();
                long totalTimeElapsed = currentTimeElapsed;
                makeNewLines(overallWriter, schedulerWriter, metricsWriter);
                writeMessage("Jobs finished in: " + totalTimeElapsed, overallWriter, schedulerWriter, metricsWriter);
                flushAndCloseAllWriters(overallWriter, schedulerWriter, metricsWriter);
                totalWriter.write(queuesAsString + " " + dmem + " " + emem + " " + numIterations +
                        " " + totalAllocatedMB + " " + averageAllocatedMB + " " + totalTimeElapsed);
		        totalWriter.newLine();
		        totalWriter.flush();
		        totalWriter.close();
                break;
            }
        }
    }

    /**
     * Send GET request for ClusterMetrics.
     * @return String
     * @throws Exception
     */
    private String sendClusterMetricsGet() throws Exception {
        String url = "http://" + yarnWEBUI + "/ws/v1/cluster/metrics";
        return sendGetToURL(url);
    }
    
    /**
     * Send GET request for Scheduler Metrics.
     * @return String
     * @throws Exception
     */
    private String sendClusterSchedulerGet() throws Exception {
        String url = "http://" + yarnWEBUI + "/ws/v1/cluster/scheduler";
        return sendGetToURL(url);
    }

    private String sendGetToURL(String url) throws Exception {
    //    HttpClient client = HttpClientBuilder.create().build();
    //    HttpGet request = new HttpGet(url);

	URL obj = new URL(url);
	HttpURLConnection con = (HttpURLConnection) obj.openConnection();
	con.setRequestMethod("GET");
	con.setRequestProperty("User-Agent", USER_AGENT);

        // add request header
    //   request.addHeader("User-Agent", USER_AGENT);

	/*/HttpResponse response = null;
	try {
        	response = client.execute(request);
	} catch(Exception e) {
		System.out.println("Failed Request: " + response);
		e.printStackTrace();
		System.exit(1);
	}

        BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));*/

	int code = con.getResponseCode();
	//System.out.println("Get request to " + url + " responded with a code: " + code);

        BufferedReader rd = new BufferedReader(
                new InputStreamReader(con.getInputStream()));

        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }

	rd.close();
        return result.toString();
    }

    private void writeMessage(String message, BufferedWriter ... writers) throws Exception {
        for (BufferedWriter writer : writers) {
            writer.write(message);
        }
    }

    private void flushAndCloseAllWriters(BufferedWriter ... writers) throws IOException {
        for (BufferedWriter writer : writers) {
            writer.flush();
            writer.close();
        }
    }

    private void makeNewLines(BufferedWriter ... writers) throws IOException {
        for (BufferedWriter writer : writers) {
            writer.newLine();
        }
    }

    private void writeQueueInfoToFile(BufferedWriter writer, Scheduler.queue[] list) throws Exception {
        for (Scheduler.queue q : list) {
            writer.write("QUEUE: " + q.getQueueName());
            writer.newLine();
            writer.write("Resources Used: " + q.getResourcesUsed().getMemory());
            writer.newLine();
            writer.write("Num Containers: " + q.getNumContainers());
            writer.newLine();
            writer.write("Num applications: " + q.getNumApplications());
            writer.newLine();
            writer.write("Num active applications: " + q.getNumActiveApplications());
            writer.newLine();
            writer.newLine();
        }

        writer.write("Total Applications: " + getTotalApplications(list));
        writer.newLine();
        writer.write("Total Active Applications: " + getTotalActiveApplications(list));
        writer.newLine();
        writer.write("Total Containers: " + getTotalContainers(list));
        writer.newLine();
        writer.newLine();
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

    private Apps.app[] readAppsJsonResponse(String appsJsonResponse) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        JsonNode node = mapper.readTree(appsJsonResponse);
        node = node.get("apps").get("app");
        TypeReference<Apps.app[]> typeRef = new TypeReference<Apps.app[]>() {};
        return mapper.readValue(node.traverse(), typeRef);
    }

    private Scheduler.queue[] readClusterSchedulerJsonResponse(String clusterSchedulerResponse) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        JsonNode node = mapper.readTree(clusterSchedulerResponse);
        node = node.get("scheduler").get("schedulerInfo").get("queues").get("queue");
        TypeReference<Scheduler.queue[]> typeRef = new TypeReference<Scheduler.queue[]>() {};
        return mapper.readValue(node.traverse(), typeRef);
    }

    private void writeClusterMetrics(BufferedWriter writer, String clusterMetricsResponse) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ClusterMetrics metrics = mapper.readValue(clusterMetricsResponse, ClusterMetrics.class);
        writer.write("Allocated MB: " + Long.toString(metrics.getClusterMetrics().getAllocatedMB()));
        writer.newLine();
        totalAllocatedMB += metrics.getClusterMetrics().getAllocatedMB();
        averageAllocatedMB = totalAllocatedMB / iterationNumber;
        writer.write("Available MB: " + Long.toString(metrics.getClusterMetrics().getAvailableMB()));
        writer.newLine();
        writer.write("Total MB: " + Long.toString(metrics.getClusterMetrics().getTotalMB()));
        writer.newLine();
        writer.write("Apps Running: " + Long.toString(metrics.getClusterMetrics().getAppsRunning()));
        writer.newLine();
        writer.write("Containers Allocated: " + metrics.getClusterMetrics().getContainersAllocated());
        writer.newLine();
        writer.write("Average Allocated MB: " + averageAllocatedMB);
        writer.newLine();
    }
}

class StatsThread implements Runnable {

    private String dmem;
    private String emem;
    private String[] queues;
    private int numIterations;
    private String sparkMaster;

    public StatsThread(int numIterations, String dmem, String emem, String sparkMaster, String... queues) {
        this.numIterations = numIterations;
        this.dmem = dmem;
        this.emem = emem;
        this.queues = queues;
        this.sparkMaster = sparkMaster;
    }

    public void run() {
        try {
            if (numIterations <= 0)
                return;
            launchSparkJob(dmem,emem,queues);
            numIterations--;
            Random r = new Random();
            while (numIterations > 0) {
                int rand = r.nextInt(10);
                long curTime = System.currentTimeMillis();
                long startTime = curTime + rand * 1000;
                inner: while (true) {
                    if (System.currentTimeMillis() > startTime) {
                        launchSparkJob(dmem, emem, queues);
                        numIterations--;
                        break inner;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void launchSparkJob(String dmem, String emem, String ... queues) throws Exception{
        // Create spark context, spark configuration, add listener, pass all parameters to third party program
        // (Ex. SparkPi)
      //  SparkConf conf = new SparkConf();
        //conf.setMaster(sparkMaster).setSparkHome(System.getenv("SPARK_HOME"));

       // SparkContext sc = new SparkContext(conf);

        for (String queue : queues) {
	//TODO: remove hardcoding
            new ProcessBuilder("/bin/bash",
                    "/home/biguser/yarnapplicationstatistics/run_spark_pi.sh", dmem, emem, queue).start();
        }
    }
}