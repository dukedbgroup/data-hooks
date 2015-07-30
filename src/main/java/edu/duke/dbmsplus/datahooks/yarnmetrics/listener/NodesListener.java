package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import scala.actors.threadpool.Arrays;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Containers;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Containers.container;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.NodeApps;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.NodeApps.app;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Nodes;
import edu.duke.dbmsplus.datahooks.yarnmetrics.sqlwriter.SQLWrapper;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.HttpGetHandler;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.PropsParser;

/**
 * NodesListener request every nodes to get app state and container state in every nodes.
 * It has not been tested in a real distributed mode
 * if the number of nodes is large, we need to modify this class and request nodes concurrently with a threads pool.
 * @author Xiaodan
 *
 */
public class NodesListener {
    private Thread thread;
    private NodesThread runnable;
    
    public NodesListener() {
        
    }

    public void run() {
        runnable = new NodesThread();
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

    class NodesThread implements Runnable {
        
        private volatile boolean running = true;
        private static int WAIT_TIME = 500;
        private ArrayList<String> nodesAddress = new ArrayList<String>();
        private SQLWrapper NodesMetricsWriter;
        //node address, appId, app object
        private HashMap<String, HashMap<String, NodeApps.app>> nodesAppsList;
        private HashMap<String, HashMap<String, Containers.container>> nodesContainersList;
        private HashSet<String> finishedApps;
        private boolean init = true;
        
        public NodesThread() {
            NodesMetricsWriter = new SQLWrapper();
            nodesAppsList = new HashMap<String, HashMap<String, NodeApps.app>>();
            nodesContainersList = new HashMap<String, HashMap<String, Containers.container>>();
            finishedApps = new HashSet<String>();
        }

        public void terminate() {
            running = false;
        }
        
        public ArrayList<String> findAllNodes() {
            ArrayList<String> address = new ArrayList<String>();
        
            PropsParser pp = new PropsParser();
            String url =
                    "http://" + pp.getYarnWEBUI() + "/ws/v1/cluster/nodes";
            HttpGetHandler hgh = new HttpGetHandler(url);
            String nodesrep = hgh.sendGet();
            System.out.println(nodesrep);
            
            try {
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                JsonNode node = mapper.readTree(nodesrep);
                node = node.get("nodes").get("node");
                if (node == null) {
                    return address;
                }
                TypeReference<Nodes.node[]> typeRef = new TypeReference<Nodes.node[]>() {};
                Nodes.node[] res = mapper.readValue(node.traverse(), typeRef);
                for (Nodes.node anode: res) {
                    address.add(anode.getNodeHTTPAddress());
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } 
            return address;
        }
        
        private HashMap<String, NodeApps.app> getApps(String appsResponse) {
            HashMap<String, NodeApps.app> result = new HashMap<String, NodeApps.app>();
            try {
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                JsonNode node = mapper.readTree(appsResponse);
                node = node.get("apps").get("app");
                if (node == null) {
                    return result;
                }
                TypeReference<NodeApps.app[]> typeRef = new TypeReference<NodeApps.app[]>() {};
                NodeApps.app[] res = mapper.readValue(node.traverse(), typeRef);
                for (NodeApps.app app: res) {
                    if ((app.getState().equals("FINISHED") && init) || finishedApps.contains(app.getId())){
                        // if app finished before listerner runs, put it in finishedApp
                        // or the app is already finished after listener runs and has been recorded to MySQL
                        finishedApps.add(app.getId());  
                    }
                    else {
                       result.put(app.getId(), app);
                    }
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }
        
        private HashMap<String, Containers.container> getContainers(String containersResponse) {
            HashMap<String, Containers.container> result = new HashMap<String, Containers.container>();
            try {
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                JsonNode node = mapper.readTree(containersResponse);
                node = node.get("containers").get("container");
                if (node == null) {
                    return result;
                }
                TypeReference<Containers.container[]> typeRef = new TypeReference<Containers.container[]>() {};
                Containers.container[] res = mapper.readValue(node.traverse(), typeRef);
                for (Containers.container container: res) {
                    result.put(container.getId(), container);
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }
        
        public void run() {

            NodesMetricsWriter.createNodesAppsTable();
            NodesMetricsWriter.createNodesContainersTable();
            System.out.println("Nodes listener is running!");
            while(running) {
                long recordTime = System.currentTimeMillis();
                //find all the nodes addresses
                nodesAddress = findAllNodes();
                if (nodesAddress == null || nodesAddress.size() == 0) {
                    System.out.println("No node is found!!!");
                    return;
                }
                //update address of nodes, if some node is offline or online
                HashMap<String, HashMap<String, NodeApps.app>> nodesAppsListTemp = new HashMap<String, HashMap<String, NodeApps.app>>();
                HashMap<String, HashMap<String, Containers.container>> nodesContainersListTemp = new HashMap<String, HashMap<String, Containers.container>>();
                
                //got pairs: <nodesAddress, <appid, app>>
                for(String address: nodesAddress) {
                    //request every address, find apps on nodes, find containers on nodes
                    String url =
                            "http://" + address + "/ws/v1/node/apps";
                    HttpGetHandler hgh = new HttpGetHandler(url);
                    String appsResponse = hgh.sendGet();
                    System.out.println(appsResponse);
                    if (!getApps(appsResponse).isEmpty()) {
                    nodesAppsListTemp.put(address, getApps(appsResponse));
                    }
                }
                //get pairs: <nodesAddress, <containerId, container>>
                for(String address: nodesAddress) {
                    //request every address, find containers on nodes, find containers on nodes
                    String url =
                            "http://" + address + "/ws/v1/node/containers";
                    HttpGetHandler hgh = new HttpGetHandler(url);
                    String containersResponse = hgh.sendGet();
                    System.out.println(containersResponse);
                    if (!getContainers(containersResponse).isEmpty()) {
                    nodesContainersListTemp.put(address, getContainers(containersResponse));
                    }         
                }
                //write new metrics to nodes_apps_metrics
                for(String add: nodesAppsListTemp.keySet()) {
                    //detected a new node running apps
                    if(!nodesAppsList.containsKey(add)) {
                        //find a new node, output
                        System.out.println("get a new node running apps!");
                        System.out.println("its address is: " + add);
                        HashMap<String, NodeApps.app> map = nodesAppsListTemp.get(add);
                        for (NodeApps.app app: map.values()) {
                            writeAppMetrics(add, app, app.getId(),recordTime);
                        }
                    }
                    //this is an old address
                    else {
                        //compare apps info of existing node
                        HashMap<String, NodeApps.app> oldMap = nodesAppsList.get(add);
                        HashMap<String, NodeApps.app> newMap = nodesAppsListTemp.get(add);
                        for(String name: newMap.keySet()){
                            // this is a new app
                            if(!oldMap.containsKey(name)) {
                                //output, find a new app
                                System.out.println("get a new app!");
                                writeAppMetrics(add, newMap.get(name), newMap.get(name).getId(),recordTime);
                            }
                            // a existing app
                            else {
                                //compare
                                NodeApps.app oldApp = oldMap.get(name);
                                NodeApps.app newApp = newMap.get(name);
                                //if this app is finished, write it out then add to finishedApps list.
                                //finished apps won't come next time
                                if (newApp.getState().equals("FINISHED")) {
                                    writeAppMetrics(add, newApp, newApp.getId(), recordTime);
                                    finishedApps.add(newApp.getId());
                                }
                                //compare running app here
                                compareNodesApp(oldApp, newApp, add, recordTime);
                                System.out.println("*******compare exsiting apps*********");
                            }
                        }
                    }
                }
                //write new metrics data to nodes_containers_metrics
                for(String add: nodesContainersListTemp.keySet()) {
                    if(!nodesContainersList.containsKey(add)) {
                        //find a new node running containers, output everything
                        System.out.println("get a new node! Output all containers.");
                        for (Containers.container container: nodesContainersListTemp.get(add).values()) {
                            writeContainerMetrics(add, container, container.getId(), recordTime);
                        }
                        
                    }
                    else {
                        //compare apps info of every existing node
                        HashMap<String, Containers.container> oldMap = nodesContainersList.get(add);
                        HashMap<String, Containers.container> newMap = nodesContainersListTemp.get(add);
                        for(String name: newMap.keySet()){
                            if(!oldMap.containsKey(name)) {
                                //output, find a new container
                                System.out.println("get a new container! Output all containers.");
                                for (Containers.container container: nodesContainersListTemp.get(add).values()) {
                                    writeContainerMetrics(add, container, container.getId(), recordTime);
                                }
                            }
                            else {
                                //compare exsisting container
                                System.out.println("*******compare containers*********");
                                Containers.container oldContainer = oldMap.get(name);
                                Containers.container newContainer = newMap.get(name);
                                compareNodesContainer(oldContainer, newContainer, add, recordTime);
                            }
                        }
                    }
                }
                
                nodesAppsList = nodesAppsListTemp;
                nodesContainersList = nodesContainersListTemp;
                
            }
            init = false;
            try {
                Thread.sleep(WAIT_TIME);
            } catch (InterruptedException e) {
                return;
            }
        }

        private void compareNodesContainer(container oldContainer, container newContainer,
                String nodeAddress, long recordTime) {
            // TODO Auto-generated method stub
            Class cls = oldContainer.getClass();
            Field[] fields = cls.getDeclaredFields();
            for (Field f: fields) {
                if (f.getName().equals("exitCode") || f.getName().equals("containerLogsLink")) {
                    continue;
                }
                try {
                    Object oldVal = f.get(oldContainer);
                    Object newVal = f.get(newContainer);
                    if (!oldVal.toString().equals(newVal.toString())) {
                        NodesMetricsWriter.writeNodesContainersTable(nodeAddress, newContainer.getId(), f.getName(), recordTime, newVal.toString());
                    }
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace(); 
                }
            }
            
        }

        private void writeContainerMetrics(String nodeAddress, container container, String id,
                long recordTime) {
            NodesMetricsWriter.writeNodesContainersTable(nodeAddress, container.getId(), "nodeId", recordTime, container.getNodeId().toString());
            NodesMetricsWriter.writeNodesContainersTable(nodeAddress, container.getId(), "totalMemoryNeededMB", recordTime, String.valueOf(container.getTotalMemoryNeededMB()));
            NodesMetricsWriter.writeNodesContainersTable(nodeAddress, container.getId(), "totalVCoresNeeded", recordTime, String.valueOf(container.getTotalVCoresNeeded()));
            NodesMetricsWriter.writeNodesContainersTable(nodeAddress, container.getId(), "state", recordTime, container.getState().toString());
            NodesMetricsWriter.writeNodesContainersTable(nodeAddress, container.getId(), "user", recordTime, container.getUser().toString());
//            NodesMetricsWriter.writeNodesContainersTable(nodeAddress, container.getId(), "exitCode", recordTime, String.valueOf(container.getExitCode()));
//            NodesMetricsWriter.writeNodesContainersTable(nodeAddress, container.getId(), "containerLogsLink", recordTime, container.getContainerLogsLink().toString());
        }

        private void compareNodesApp(app oldApp, app newApp, String nodeAddress, long recordTime) {
            
            String[] oldCon = oldApp.getContainerids();
            String[] newCon = oldApp.getContainerids();
            //do not have any containers
            if (oldCon == null && newCon == null) {
                return;
            }
            //assigned new containers
            else if (oldCon == null || oldCon.length == 0) {
                for (String containerId: newCon) {
                    NodesMetricsWriter.writeNodesAppsTable(nodeAddress, newApp.getId(), "containerId", recordTime, containerId);
                }
            }
            else {
                //num of containers changed
                if (oldCon.length != newCon.length) {
                    for (String containerId: newCon) {
                        NodesMetricsWriter.writeNodesAppsTable(nodeAddress, newApp.getId(), "containerId", recordTime, containerId);
                    } 
                }
                else {
                    //some containers changed
                    HashSet<String> temp = new HashSet<String>(Arrays.asList(oldCon));
                    for (String containerId: newCon) {
                        if (!temp.contains(containerId)) {
                            NodesMetricsWriter.writeNodesAppsTable(nodeAddress, newApp.getId(), "containerId", recordTime, containerId);
                        }
                    }
                } 
            }
        }

        private void writeAppMetrics(String nodeAddress, NodeApps.app app, String appId, long recordTime) {
            NodesMetricsWriter.writeNodesAppsTable(nodeAddress, appId, "state", recordTime, app.getState().toString());
            NodesMetricsWriter.writeNodesAppsTable(nodeAddress, appId, "user", recordTime, app.getUser().toString());
            if(app.getContainerids() != null && app.getContainerids().length != 0) {
                for (String containerId: app.getContainerids()) {
                    NodesMetricsWriter.writeNodesAppsTable(nodeAddress, appId, "containerId", recordTime, containerId);
                }
            }

        }
    }

