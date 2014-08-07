package edu.duke.dbmsplus.datahooks.execution;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class will manage the layout of the Hive logs, including the job histories and the query plan json, etc.
 * 
 * Other classes should use this class to infer the log directory's internal structure. 
 * 
 * This class also takes String as input that works on HDFS as well by assuming the protocol.
 * The File input only works in local FS.
 * 
 * @author Jie Li
 *
 */
public class HiveLogLayoutManager {
	private static final Log LOG = LogFactory.getLog(HiveLogLayoutManager.class);

	private String topDirPath;
	private static String JOB_LOGS_DIR = "_logs";
	public static final String QUERY_INFO_FILE = "HiveQueryInfo.json";
    public static final String PRE_QUERY_INFO_FILE = "HiveQueryInfoPre.json";

    /** state of logs is determined by placement in a subdir under the topDirPath.
     */
    public enum State { EXECUTING(1, "executing"),  // files being mutated by Hive/Hadoop
                        COMPLETED(2, "completed"),  // Hive query finished
                        ANALYZED(3, "analyzed"),  // logs have been analyzed
                        ARCHIVED(4, "archived")    // archived logs
            ;
        private int v;
        private String name;
        private State(int v, String name) {
            this.v = v;
            this.name = name;
        }
        public int getValue() {
            return v;
        }
        public String getName() {
            return name;
        }
    }

	public HiveLogLayoutManager(File topDir) {
		this.topDirPath = topDir.getAbsolutePath();
	}

	public HiveLogLayoutManager(String topDirPath) {
		this.topDirPath = topDirPath;
	}

	public File getQueryDir(String queryId, State state){
		return new File(getQueryDirPath(queryId, state));
	}

	public String getQueryDirPath(String queryId, State state){
		return getTopDirPath(state) + "/" + queryId;
	}

    public String getTopDirPath(State state) {
        return topDirPath + "/" + state.getName();
    }

    /**
     * Currently only works for local dir.
     * ToDo: also support HDFS?
     * @return
     */
	public List<String> getQueryIds(State state){
        File topDir = new File(getTopDirPath(state));
		List<String> dirs = new ArrayList<String>();

		for (File queryDir : topDir.listFiles()){
			if (queryDir.isDirectory()) {
				dirs.add(queryDir.getName());
			}
		}
		return dirs;
	}

	public File getJobLogDir(String queryId, State state){
		return new File(getQueryDir(queryId, state), JOB_LOGS_DIR);
	}

	public String getJobLogDirPath (String queryId, State state){
		return getQueryDirPath(queryId, state) + "/" + JOB_LOGS_DIR;
	}

	public File getQueryPlanFile(String queryId, State state){
		return new File(getQueryDir(queryId, state), QUERY_INFO_FILE);
	}
    
    public File getPreQueryPlanFile(String queryId, State state){
	return new File(getQueryDir(queryId, state), PRE_QUERY_INFO_FILE);
    }

	public String getQueryPlanFilePath(String queryId, State state){
		return getQueryDirPath(queryId, state) + "/" + QUERY_INFO_FILE;
	}

    public String getPreQueryPlanFilePath(String queryId, State state){
   		return getQueryDirPath(queryId, state) + "/" + PRE_QUERY_INFO_FILE;
   	}

    /**
     * Rename/move the logs for given queryId from one hdfs dir. to another to indicate
     * a state change.
     * @param queryId
     * @param conf
     * @param fromState
     * @param toState
     * @throws IOException
     */
    public void stateChange(String queryId, Configuration conf, State fromState, State toState)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path fromPath = new Path(getQueryDirPath(queryId, fromState));
        Path toPath = new Path(getQueryDirPath(queryId, toState));
        boolean b = fs.rename(fromPath, toPath);
        // ToDo: handle rename failure, but dont complain if toPath exists
    }


/*
    // is this needed?
	public File getTopDir() {
		return topDir;
	}

    // should be immutable
	public void setTopDir(File topDir) {
		this.topDir = topDir;
	}
*/

	/**
	 * Validate if we understand the given Hive log dir.
	 * 
	 * There should be at least a completed/query_dir,
	 *      within with there should be a HiveQueryInfo.json file.
	 * 
	 * @return true if it's a valid Hive log dir; false otherwise.
	 */
    public static boolean validateLogDir(File logDir) {
	if (!logDir.isDirectory()) {
	    LOG.info(logDir.getAbsolutePath() + " is not a directory!");
	    return false;
	}

	// look for a "completed" subdirectory 
	File completedDir = new File(logDir, HiveLogLayoutManager.State.COMPLETED.getName());
	// LOG.info("completedDir = " + completedDir.getAbsolutePath());
	if (!completedDir.exists()){
	    LOG.info(logDir.getAbsolutePath() + " is not a valid Hive log directory because it does not contain a completed subdirectory!");
	    return false;
	}

	// find at least a valid query sub dir under the
	//  "completed" subdirectory 
	for (File queryDir : completedDir.listFiles()){
	    // LOG.info("queryDir = " + queryDir.getAbsolutePath());
	    if (queryDir.isDirectory() && 
		((new File(queryDir, QUERY_INFO_FILE).exists()) ||
		 (new File(queryDir, PRE_QUERY_INFO_FILE).exists()))) {
		LOG.info(logDir + " is a valid Hive log dir with at least a Hive query " + queryDir.getName());
		return true;
	    }
	}

	LOG.info(logDir + " is an invalid Hive log dir with no Hive query info");
	return false;
    }
}
