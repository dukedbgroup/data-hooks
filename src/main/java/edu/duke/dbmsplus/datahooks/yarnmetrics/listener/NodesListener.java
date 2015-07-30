package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

import java.io.IOException;
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

import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Containers;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.NodeApps;
import edu.duke.dbmsplus.datahooks.yarnmetrics.pojo.Nodes;
import edu.duke.dbmsplus.datahooks.yarnmetrics.sqlwriter.SQLWrapper;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.HttpGetHandler;
import edu.duke.dbmsplus.datahooks.yarnmetrics.util.PropsParser;

/**
 * NodesListener request every nodes to get app state and container state in every nodes.
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
        }
    }
}

    class NodesThread implements Runnable {
        
        private volatile boolean running = true;
        private static int WAIT_TIME = 250;
        private ArrayList<String> nodesAddress = new ArrayList<String>();
        private SQLWrapper NodesMetricsWriter;
        //node address, appId, app object
        private HashMap<String, HashMap<String, NodeApps.app>> nodesAppsList;
        private HashMap<String, HashMap<String, Containers.container>> nodesContainersList;
        private HashMap<String, NodeApps.app> finishedApps;
        public NodesThread() {
            NodesMetricsWriter = new SQLWrapper();
            nodesAppsList = new HashMap<String, HashMap<String, NodeApps.app>>();
            nodesContainersList = new HashMap<String, HashMap<String, Containers.container>>();
            finishedApps = new HashMap<String, NodeApps.app>();
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
                    if (!app.getState().equals("FINISHED") && finishedApps.isEmpty()){
                        // init finishedApp
                        finishedApps.put(app.getId(), app);  
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

//            NodesMetricsWriter.createNodesAppsTable();
//            NodesMetricsWriter.createNodesContainersTable();
            System.out.println("Nodes listener is running!");
            while(running) {
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
                //if nodesAppsList,nodeContainersAppsList are both empty, it's the first time
                if (nodesAppsList.isEmpty() && nodesContainersList.isEmpty()) {
                    if (!nodesAppsListTemp.isEmpty()){
                        //write all the info to MySQL
                    }
                    if (!nodesContainersListTemp.isEmpty()) {
                      //write all the info in to MySQL
                    }
                    nodesAppsList = nodesAppsListTemp;
                    nodesContainersList = nodesContainersListTemp;
                }
                //nodesAppsList is not empty, nodeContainersAppsList
                else {
                    //compare old state with new one
                    for(String add: nodesAppsListTemp.keySet()) {
                        if(!nodesAppsList.containsKey(add)) {
                            //find a new node, output
                            System.out.println("get a new node!");
                        }
                        else {
                            //compare apps info of every existing node
                            HashMap<String, NodeApps.app> oldMap = nodesAppsList.get(add);
                            HashMap<String, NodeApps.app> newMap = nodesAppsListTemp.get(add);
                            for(String name: newMap.keySet()){
                                if(!oldMap.containsKey(name)) {
                                    //output, find a new app
                                }
                                else {
                                    //compare
                                    NodeApps.app oldApp = oldMap.get(name);
                                    NodeApps.app newApp = newMap.get(name);
//                                    compareNodesApp(oldApp, newApp);
                                }
                            }
                        }
                    }
                    for(String add: nodesContainersListTemp.keySet()) {
                        if(!nodesContainersList.containsKey(add)) {
                            //find a new node, output everything
                        }
                        else {
                            //compare apps info of every existing node
                            HashMap<String, Containers.container> oldMap = nodesContainersList.get(add);
                            HashMap<String, Containers.container> newMap = nodesContainersListTemp.get(add);
                            for(String name: newMap.keySet()){
                                if(!oldMap.containsKey(name)) {
                                    //output, find a new app
                                }
                                else {
                                    //compare
                                }
                            }
                        }
                    }
                    nodesAppsList = nodesAppsListTemp;
                    nodesContainersList = nodesContainersListTemp;
                }
//             
                try {
                    Thread.sleep(WAIT_TIME);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
