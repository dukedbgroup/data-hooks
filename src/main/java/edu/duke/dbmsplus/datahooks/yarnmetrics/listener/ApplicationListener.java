package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;


import java.lang.reflect.Field;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Apps;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Apps.app;
import edu.duke.dbmsplus.datahooks.yarnmetrics.sqlwriter.SQLWrapper;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.HttpGetHandler;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.PropsParser;


/**
 * 
 * @author Xiaodan
 */
public class ApplicationListener {
    
    private Thread thread;
    private AppThread runnable;
    
    public ApplicationListener() {
        
    }

    public void run() {
        runnable = new AppThread();
        thread = new Thread(runnable);
        thread.start();
    }

    public void stop() {
        if (thread != null) {
            runnable.terminate();
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

    class AppThread implements Runnable {
        private long startTime = System.currentTimeMillis();
        private volatile boolean running = true;
        private static int WAIT_TIME = 250;
        private Apps.app[] state;
        private SQLWrapper appMetricsWriter;

        public AppThread() {
            appMetricsWriter = new SQLWrapper();
        }

        public void terminate() {
            running = false;
        }
        
        public void initState(HttpGetHandler hgh){
            String appsResponse = null;
            long recordTime = System.currentTimeMillis();
            while (state == null) {
                appsResponse = hgh.sendGet();
                try {
                    state = readAppsJsonResponse(appsResponse);
                    recordTime = System.currentTimeMillis();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            System.out.println(state[0]);
            for (app app: state) {
                writeToAppsTable(app, recordTime);
            }
        }
     

        public void run() {
            PropsParser pp = new PropsParser();
            String url =
                    "http://" + pp.getYarnWEBUI() + "/ws/v1/cluster/apps?startedTimeBegin=" + startTime;
            HttpGetHandler hgh = new HttpGetHandler(url);
            appMetricsWriter.createAppsTable();
            initState(hgh);
            System.out.println("monitor applications daemon is running");
           
            while (running) {
                try {
                    Thread.sleep(WAIT_TIME);
                    long recordTime = System.currentTimeMillis();
                    String appsResponse = hgh.sendGet();
                    app[] apps = readAppsJsonResponse(appsResponse);
//                    System.out.println("apps is null?" + apps==null);
//                    System.out.println(state[0]);
//                    System.out.println(apps[0]);
//                    System.out.println(appsResponse);
                    
                    for (int i = 0; i < apps.length; i++) {
//                        System.out.println("apps update begin...........");
                        if (state[i].getState() != null && state[i].getState().equals(apps[i].getState()) 
                                && apps[i].getState().equals("FINISHED")) {
                            writeToAppsTable(apps[i], recordTime);
                        }
                        else {
                            updateAppsTable(state[i], apps[i], recordTime);
                        }
                    }
                    state = apps;
                } catch (InterruptedException e) {
                    System.out.println("Application Listenner, Sleep is over.");
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                    // do nothing if appsResponse is empty
                }
            }
            System.out.println("Application Listenner stopped.");
        }
        
        private Apps.app[] readAppsJsonResponse(String appsJsonResponse) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            JsonNode node = mapper.readTree(appsJsonResponse);
            node = node.get("apps").get("app");
            if (node == null) {
//                System.out.println("node is null");
                return null;
            }
            TypeReference<Apps.app[]> typeRef = new TypeReference<Apps.app[]>() {};
            return mapper.readValue(node.traverse(), typeRef);
        }

        private void updateAppsTable(app oldApp, app newApp, long recordTime) {
            Class cls = oldApp.getClass();
            Field[] fields = cls.getDeclaredFields();
            boolean isRecordTime = false;
            try{
                for (int i = 0; i < fields.length; i++) {
                    fields[i].setAccessible(true);
                    //do not compare elapsedTime
                    if (fields[i].getName().equals("elapsedTime")) {
                        continue;
                    }
                    Object oldVal = fields[i].get(oldApp);
                    Object newVal = fields[i].get(newApp);
//                    System.out.println("Update: The field:" + fields[i].getName() +
//                            "\nold value: " + oldVal +" \nnew value: " + newVal + "\nTime:" +
//                            recordTime);
                    if (oldVal == null || newVal == null) {
                        if (newVal != null) {
//                            System.out.println("***********updateAppsTable**********");
                            appMetricsWriter.writeAppsTable(newApp.getId(), fields[i].getName(),
                                    recordTime, newVal.toString());
                            isRecordTime = true;
                        }
                    }
                    else if (!oldVal.toString().equals(newVal.toString())) {
//                        System.out.println("***********updateAppsTable**********");
                        appMetricsWriter.writeAppsTable(newApp.getId(), fields[i].getName(),
                                recordTime, newVal.toString());
                        isRecordTime = true;
                    }
                }
                if (isRecordTime == true) {
//                    System.out.println("***********update elapsedTime**********");
                    appMetricsWriter.writeAppsTable(newApp.getId(), "elapsedTime",
                                    recordTime, String.valueOf(newApp.getElapsedTime()));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void writeToAppsTable(app app, long recordTime){
            Class cls = app.getClass();
            Field[] fields = cls.getDeclaredFields();
//            System.out.println("Length of fields of new app: " + fields.length);
            try {
                for (Field f: fields) {
                    f.setAccessible(true);
                    Object val = f.get(app);
//                    System.out.println("the metrics name:" + f.getName() + "\nval: " + val);
                    if (val == null) {
                        appMetricsWriter.writeAppsTable(app.getId(), f.getName(), recordTime, String.valueOf(val));
//                        System.out.println("not available: The field:" + f.getName() + "\nvalue:" + val + " not available" + "\nTime:" + recordTime);
    
                    }
                    else {
                        appMetricsWriter.writeAppsTable(app.getId(), f.getName(), recordTime, val.toString());
//                        System.out.println("new field:" + f.getName() + "\nvalue: " + val + "\nTime:" + recordTime);
                    }
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

