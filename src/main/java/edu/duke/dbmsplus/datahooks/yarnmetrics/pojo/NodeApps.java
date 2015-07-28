package edu.duke.dbmsplus.datahooks.yarnmetrics.pojo;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * POJO for request apps on nodes. http://<nm http address:port>/ws/v1/node/apps
 * @author Xiaodan
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeApps {
    private apps apps;
    
    public NodeApps() {
        
    }
    
    public NodeApps.apps getApps() {
        return apps;
    }

    public void setApps(NodeApps.apps apps) {
        this.apps = apps;
    }

    public static class apps {
        private app[] apps;
        
        public apps() {
            
        }

        public NodeApps.app[] getApps() {
            return apps;
        }

        public void setApps(NodeApps.app[] apps) {
            this.apps = apps;
        }
    }
    
    public static class app {
        private String id;
        private String user;
        private String state;
        private String[] containerids;
        
        public app() {
            
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String[] getContainerids() {
            return containerids;
        }

        public void setContainerids(String[] containerids) {
            this.containerids = containerids;
        }
    }

}
