package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Apps;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Apps.app;
import edu.duke.dbmsplus.datahooks.yarnmetrics.sqlwriter.SQLWrapper;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.HttpGetHandler;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.PropsParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by rahulswaminathan on 11/19/14.
 */
public abstract class ApplicationListener {
    // private final String USER_AGENT = "Mozilla/5.0";
    private Set<Apps.app> appsSet = new HashSet<Apps.app>();
    private long startTime = 0;
    private Set<Apps.app> removedApps = new HashSet<Apps.app>();
    private Map<Apps.app, String> appToStateMap = new HashMap<Apps.app, String>();
    private Map<Apps.app, Integer> appToContainersMap = new HashMap<Apps.app, Integer>();
    private Thread thread;
    private AppThread runnable;

    public void startListening() {
        startTime = System.currentTimeMillis();
        appsSet = new HashSet<Apps.app>();
        runnable = new AppThread();
        thread = new Thread(runnable);
        thread.start();
    }

    public void stopListening() {
        if (thread != null) {
            runnable.terminate();
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public abstract void onAppBegin(Apps.app app);

    public abstract void onAppFinish(Apps.app app);

    public abstract void onAppChangeState(Apps.app app);

    public abstract void onAppChangeContainers(Apps.app app);

    public Set<Apps.app> getAppsSet() {
        return appsSet;
    }

    private String sendAppsGet(long startTime) throws Exception {
        PropsParser pp = new PropsParser();
        String url =
                "http://" + pp.getYarnWEBUI() + "/ws/v1/cluster/apps?startedTimeBegin=" + startTime;
        HttpGetHandler hgh = new HttpGetHandler(url);
        String appsResponse = hgh.sendGet();
        return appsResponse;
    }


    private Apps.app[] readAppsJsonResponse(String appsJsonResponse) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        JsonNode node = mapper.readTree(appsJsonResponse);
        node = node.get("apps").get("app");
        TypeReference<Apps.app[]> typeRef = new TypeReference<Apps.app[]>() {};
        return mapper.readValue(node.traverse(), typeRef);
    }

    class AppThread implements Runnable {

        private volatile boolean running = true;
        private static final int WAIT_TIME = 50;
        private SQLWrapper appMetricsWriter;
        private Apps.app[] state = new Apps.app[10];

        public AppThread() {
            appMetricsWriter = new SQLWrapper();
        }

        public void terminate() {
            running = false;
        }

        public void run() {
            appMetricsWriter.createAppsTable();

            while (running) {
                try {
                    String appsResponse = sendAppsGet(startTime);
                    long recordTime = System.currentTimeMillis();
//                    System.out.println("================Apps Json start================");
//                    System.out.println(appsResponse);
//                    System.out.println("================ Apps Json end ================");
                    Apps.app[] apps = readAppsJsonResponse(appsResponse);

                    for (int i = 0; i < apps.length; i++) {
                        
                        Apps.app app = apps[i];
                        // new app comes
                        if (!appsSet.contains(app) && !removedApps.contains(app)) {
                            appsSet.add(app);
                            appToStateMap.put(app, app.getState());
                            appToContainersMap.put(app, app.getRunningContainers());
                            onAppBegin(app);
                            writeMetricsToAppTable(app, recordTime);
                            state[i] = app;

                        }
                        // app finished.
                        if (app.getState().equals("FINISHED") && !removedApps.contains(app)) {
                            removedApps.add(app);
                            appsSet.remove(app);
                            appToStateMap.put(app, app.getState());
                            onAppFinish(app);
                            writeMetricsToAppTable(app, recordTime);
                        }
                        if (!app.getState().equals(appToStateMap.get(app))) {
                            appToStateMap.put(app, app.getState());
                            onAppChangeState(app);
                        }
                        if (app.getRunningContainers() != appToContainersMap.get(app)) {
                            appToContainersMap.put(app, app.getRunningContainers());
                            onAppChangeContainers(app);
                        }
                        if (appsSet.contains(app)) {
                            updateAppsTable(state[i], app, recordTime);
                        }
                    }
                    state = apps;
                    Thread.sleep(WAIT_TIME);
                } catch (Exception e) {
                    // do nothing if appsResponse is empty
                }
            }
            System.out.println("Application Listenner stopped.");
        }

        private void updateAppsTable(app oldApp, app newApp, long recordTime)
                throws IllegalArgumentException, IllegalAccessException {
            Class cls = oldApp.getClass();
            Field[] fields = cls.getDeclaredFields();

            for (int i = 0; i < fields.length - 1; i++) {
                // System.out.println(f.toString());
                fields[i].setAccessible(true);
                Object oldVal = fields[i].get(oldApp);
                // System.out.println(oldVal.toString());
                Object newVal = fields[i].get(oldApp);
                // System.out.println(oldVal +"===" + newVal);
                if (!oldVal.toString().equals(newVal.toString())) {
                    // TODO: update to MySQL
                    appMetricsWriter.writeAppsTable(oldApp.getId(), fields[i].getName(),
                            recordTime, newVal.toString());
                    // System.out.println("Update: The field:" + fields[i].getName() +
                    // "\nold value: " + oldVal +" \nnew value: " + newVal + "\nTime:" +
                    // recordTime);
                }
            }
        }

        private void writeMetricsToAppTable(app app, long recordTime)
                throws IllegalArgumentException, IllegalAccessException {
            Class cls = app.getClass();
            Field[] fields = cls.getDeclaredFields();
            for (int i = 0; i < fields.length - 1; i++) {
                // System.out.println(f.toString());
                fields[i].setAccessible(true);
                Object val = fields[i].get(app);
                // System.out.println(oldVal +"===" + newVal);
                appMetricsWriter.writeAppsTable(app.getId(), fields[i].getName(), recordTime,
                        val.toString());
                // System.out.println("New App comes or App finished.\n" + "Update: The field:" +
                // fields[i].getName() + "\nold value: " + oldVal +" \nnew value: " + newVal +
                // "\nTime:" + recordTime);
            }
        }
    }
}
