package edu.duke.dbmsplus.datahooks.yarnmetrics.pojo;
/**
 * POJO for requesting state of containers on nodes. http://<nm http address:port>/ws/v1/node/apps
 * @author Xiaodan
 *
 */
public class Containers {
    private containers containers;
    
    public Containers() {
        
    }
    
    public Containers.containers getContainers() {
        return containers;
    }

    public void setContainers(Containers.containers containers) {
        this.containers = containers;
    }

    public static class containers {
        private container[] container;
        
        public containers() {
            
        }

        public Containers.container[] getContainer() {
            return container;
        }

        public void setContainer(Containers.container[] container) {
            this.container = container;
        }
    }
    
    public static class container {
        private String id;
        private String state;
        private String nodeId;
        private String containerLogsLink;
        private String user;
        private int exitCode;
        private String diagnostics;
        private long totalMemoryNeededMB;
        private long totalVCoresNeeded;
        
        public container () {
            
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public String getContainerLogsLink() {
            return containerLogsLink;
        }

        public void setContainerLogsLink(String containerLogsLink) {
            this.containerLogsLink = containerLogsLink;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public int getExitCode() {
            return exitCode;
        }

        public void setExitCode(int exitCode) {
            this.exitCode = exitCode;
        }

        public String getDiagnostics() {
            return diagnostics;
        }

        public void setDiagnostics(String diagnostics) {
            this.diagnostics = diagnostics;
        }

        public long getTotalMemoryNeededMB() {
            return totalMemoryNeededMB;
        }

        public void setTotalMemoryNeededMB(long totalMemoryNeededMB) {
            this.totalMemoryNeededMB = totalMemoryNeededMB;
        }

        public long getTotalVCoresNeeded() {
            return totalVCoresNeeded;
        }

        public void setTotalVCoresNeeded(long totalVCoresNeeded) {
            this.totalVCoresNeeded = totalVCoresNeeded;
        }
    }
}
