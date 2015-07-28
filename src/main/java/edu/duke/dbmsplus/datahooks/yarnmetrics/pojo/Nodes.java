package edu.duke.dbmsplus.datahooks.yarnmetrics.pojo;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Pojo for nodes info. http://<rm http address:port>/ws/v1/cluster/nodes
 * @author Xiaodan
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Nodes {
    private nodes nodes;
    
    public Nodes() {
        
    }
    public Nodes.nodes getNodes() {
        return nodes;
    }
    public void setNodes(Nodes.nodes nodes) {
        this.nodes = nodes;
    }
    
    public static class nodes {
        private node[] node;
        
        public nodes() {
            
        }

        public Nodes.node[] getNode() {
            return node;
        }

        public void setNode(Nodes.node[] node) {
            this.node = node;
        }
    }
    
    public static class node {
        
        private String rack;
        private String state;
        private String id;
        private String nodeHostName;
        private String nodeHTTPAddress;
        private String healthStatus;
        private String healthReport;
        private long lastHealthUpdate;
        private long usedMemoryMB;
        private long availMemoryMB;
        private long usedVirtualCores;
        private long availableVirtualCores;
        private int numContainers;
        
        public node() {
            
        }

        public String getRack() {
            return rack;
        }

        public void setRack(String rack) {
            this.rack = rack;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getNodeHostName() {
            return nodeHostName;
        }

        public void setNodeHostName(String nodeHostName) {
            this.nodeHostName = nodeHostName;
        }

        public String getNodeHTTPAddress() {
            return nodeHTTPAddress;
        }

        public void setNodeHTTPAddress(String nodeHTTPAddress) {
            this.nodeHTTPAddress = nodeHTTPAddress;
        }

        public String getHealthStatus() {
            return healthStatus;
        }

        public void setHealthStatus(String healthStatus) {
            this.healthStatus = healthStatus;
        }

        public String getHealthReport() {
            return healthReport;
        }

        public void setHealthReport(String healthReport) {
            this.healthReport = healthReport;
        }

        public long getLastHealthUpdate() {
            return lastHealthUpdate;
        }

        public void setLastHealthUpdate(long lastHealthUpdate) {
            this.lastHealthUpdate = lastHealthUpdate;
        }

        public long getUsedMemoryMB() {
            return usedMemoryMB;
        }

        public void setUsedMemoryMB(long usedMemoryMB) {
            this.usedMemoryMB = usedMemoryMB;
        }

        public long getAvailMemoryMB() {
            return availMemoryMB;
        }

        public void setAvailMemoryMB(long availMemoryMB) {
            this.availMemoryMB = availMemoryMB;
        }

        public long getUsedVirtualCores() {
            return usedVirtualCores;
        }

        public void setUsedVirtualCores(long usedVirtualCores) {
            this.usedVirtualCores = usedVirtualCores;
        }

        public long getAvailableVirtualCores() {
            return availableVirtualCores;
        }

        public void setAvailableVirtualCores(long availableVirtualCores) {
            this.availableVirtualCores = availableVirtualCores;
        }

        public int getNumContainers() {
            return numContainers;
        }

        public void setNumContainers(int numContainers) {
            this.numContainers = numContainers;
        }
    }
}
