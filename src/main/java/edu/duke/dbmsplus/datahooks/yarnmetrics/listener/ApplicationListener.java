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
        private HashMap<String, Apps.app> state = new HashMap<String, Apps.app>();

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
                    Thread.sleep(WAIT_TIME);
                    String appsResponse = sendAppsGet(startTime);
                    long recordTime = System.currentTimeMillis();
//                    System.out.println("================Apps Json start================");
//                    System.out.println(appsResponse);
//                    System.out.println("================ Apps Json end ================");
                    Apps.app[] apps = readAppsJsonResponse(appsResponse);

                    for (int i = 0; i < apps.length; i++) {
                        
                        Apps.app app = apps[i];
                        
                        if (state.containsKey(app.getId())) {
                            updateAppsTable(state.get(app.getId()), app, recordTime);
                            state.put(app.getId(), app);
                        }
                     // new app comes
                        if (!appsSet.contains(app) && !removedApps.contains(app)) {
                            System.out.println("************new app comes!*********");
                            appsSet.add(app);
                            appToStateMap.put(app, app.getState());
                            appToContainersMap.put(app, app.getRunningContainers());
                            onAppBegin(app);
                            writeMetricsToAppTable(app, recordTime);
                            state.put(app.getId(), app);

                        }
                        // app finished.
                        if (app.getState().equals("FINISHED") && !removedApps.contains(app)) {
                            removedApps.add(app);
                            appsSet.remove(app);
                            appToStateMap.put(app, app.getState());
                            onAppFinish(app);
                            writeMetricsToAppTable(app, recordTime);
                            state.remove(app.getId());
                        }
                        // app state changes.
                        if (!app.getState().equals(appToStateMap.get(app))) {
                            appToStateMap.put(app, app.getState());
                            onAppChangeState(app);
                        }
                        // app container number changes.
                        if (app.getRunningContainers() != appToContainersMap.get(app)) {
                            appToContainersMap.put(app, app.getRunningContainers());
                            onAppChangeContainers(app);
                        }
                        
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    // do nothing if appsResponse is empty
                }
            }
            System.out.println("Application Listenner stopped.");
        }

        private void updateAppsTable(app oldApp, app newApp, long recordTime)
                throws IllegalArgumentException, IllegalAccessException {
            Class cls = oldApp.getClass();
            Field[] fields = cls.getDeclaredFields();

            for (int i = 0; i < fields.length; i++) {
                // System.out.println(f.toString());
                fields[i].setAccessible(true);
                Object oldVal = fields[i].get(oldApp);
                // System.out.println(oldVal.toString());
                Object newVal = fields[i].get(oldApp);
                // System.out.println(oldVal +"===" + newVal);
//                System.out.println("Update: The field:" + fields[i].getName() +
//                        "\nold value: " + oldVal +" \nnew value: " + newVal + "\nTime:" +
//                        recordTime);
                if (oldVal == null || newVal == null) {
                    if (newVal != null) {
                        System.out.println("***********updateAppsTable**********");
                        appMetricsWriter.writeAppsTable(newApp.getId(), fields[i].getName(),
                                recordTime, newVal.toString());
                    }
                }
                else if (!oldVal.toString().equals(newVal.toString())) {
                    // TODO: update to MySQL
                    System.out.println("***********updateAppsTable**********");
                    appMetricsWriter.writeAppsTable(oldApp.getId(), fields[i].getName(),
                            recordTime, newVal.toString());

                }
            }
        }

        private void writeMetricsToAppTable(app app, long recordTime){
            Class cls = app.getClass();
            Field[] fields = cls.getDeclaredFields();
            System.out.println("Length of fields of new app: " + fields.length);
            try {
                for (Field f: fields) {
                    f.setAccessible(true);
                    Object val = f.get(app);
                    System.out.println("the metrics name:" + f.getName() + "\nval: " + val);
                    if (val == null) {
                        appMetricsWriter.writeAppsTable(app.getId(), f.getName(), recordTime, String.valueOf(val));
                        System.out.println("not available: The field:" + f.getName() + "\nvalue:" + val + " not available" + "\nTime:" + recordTime);
    
                    }
                    else {
                        appMetricsWriter.writeAppsTable(app.getId(), f.getName(), recordTime, val.toString());
                        System.out.println("new field:" + f.getName() + "\nvalue: " + val + "\nTime:" + recordTime);
                    }
                }
            } catch (IllegalArgumentException | IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

