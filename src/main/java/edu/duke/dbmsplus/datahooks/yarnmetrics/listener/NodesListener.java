package edu.duke.dbmsplus.datahooks.yarnmetrics.listener;

import java.io.IOException;
import java.util.ArrayList;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

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

        public NodesThread() {
            NodesMetricsWriter = new SQLWrapper();
        }

        public void terminate() {
            running = false;
        }
        
        public ArrayList<String> findAllNodes() {
            ArrayList<String> Address = new ArrayList<String>();
        
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
                    return Address;
                }
                TypeReference<Nodes.node[]> typeRef = new TypeReference<Nodes.node[]>() {};
                Nodes.node[] res = mapper.readValue(node.traverse(), typeRef);
                for (Nodes.node anode: res) {
                    Address.add(anode.getNodeHTTPAddress());
                }
            } catch (JsonParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (JsonMappingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return nodesAddress;
        }
        
        public void run() {
            nodesAddress = findAllNodes();
            if (nodesAddress == null || nodesAddress.size() == 0) {
                System.out.println("No node is found!!!");
                return;
            }
//            NodesMetricsWriter.createNodesAppsTable();
//            NodesMetricsWriter.createNodesContainersTable();
            
            while(running) {
                for(String address: nodesAddress) {
                    //request every address, find apps on nodes, find containers on nodes
                    //compare with the last state
                    //write the difference to MySQL
                }
            }
        }
    }
