/**
 * 
 */
package edu.duke.dbmsplus.datahooks.execution;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobStatus;

import edu.duke.dbmsplus.datahooks.conf.MetadataDatabaseCredentials;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveQueryInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveStageInfo;
import edu.duke.dbmsplus.datahooks.execution.profile.HiveStage;
import edu.duke.dbmsplus.datahooks.execution.profile.ProfileDataWriter;

/**
 * Used for thoth data collection.
 * Add this class as hive.exec.post.hooks
 * @author mayuresh
 *
 */
public class ThothPostHook implements ExecuteWithHookContext {

	static volatile ProfileDataWriter WRITER = null;
	
	@Override
	public void run(HookContext hookContext) throws Exception {
		
		if(WRITER == null) {
			// first execution
			// extract metadata db config from Hive config
			Configuration conf = hookContext.getConf();
			System.out.println("--- Reading from hive conf ---");
			try {
				MetadataDatabaseCredentials.CONNECTION_STRING = conf.get(
						MetadataDatabaseCredentials.CONNECTION_STRING_KEY);
				MetadataDatabaseCredentials.USERNAME = conf.get(
						MetadataDatabaseCredentials.USERNAME_KEY);
				MetadataDatabaseCredentials.PASSWORD = conf.get(
						MetadataDatabaseCredentials.PASSWORD_KEY);
			} catch(Exception e) {
				System.out.println("Database credentials are not set");
				e.printStackTrace();
			}

			try{
				WRITER = new ProfileDataWriter();
			} catch(Exception e) {
				System.out.println("Could not establish connection to mysql");
				System.exit(0);
			}
		}

		HiveQueryInfo hiveQueryInfo = null;
        try {
            hiveQueryInfo = HookUtils.collectQueryInfoToObject(hookContext, 
            		null, null, null, null, 0, false, "normal");
        } catch (Throwable e) {
            if (Thread.interrupted() || e instanceof InterruptedException ||
                  (e.getCause()!=null && e.getCause() instanceof InterruptedException)) {
                // looks like cli hive job killed.
                ; // ignore
            } else {

                // Throwing Exception would kill client, so avoid that
                HookUtils.errorMessage("ERROR: HivePostHook.run(): " + e + " " +
                                     org.apache.hadoop.util.StringUtils.stringifyException(e));
            }
        }

        if(hiveQueryInfo == null) {
        	System.out.println("Hive query info is null");
        	return;
        }
        //for (HiveTableInfo table: hiveQueryInfo.getFullTableNameToInfos().values()) {
        //	table.getBaseTableName();
        //	table.getPartitionCols();
        //	table.getPath();
        //}
        List<HiveStageInfo> stages = hiveQueryInfo.getHiveStageInfos();
        if(stages == null) {
        	System.out.println("Hive stage info is null");
        	return;
        }

        Long startTime = hiveQueryInfo.getStartTime();
    	HiveStage stage = null;
        for (HiveStageInfo stageInfo: stages) {
        	// TODO: replace string 'MAPRED' with a constant from StageType
        	if("MAPRED" == stageInfo.getStageType()) {
        		if(stage != null) {
            		// start time and end time associated with stageInfo are set to 0, 
            		// so fetching them from hadoop job client
            		JobClient client = new JobClient(new JobConf());            		
            		JobStatus[] jobs = client.getAllJobs();
            		// FIXME: iterating over all jobs, may be expensive
            		for(JobStatus job: jobs) {
            			if(job.getJobId().equals(stageInfo.getHadoopJobId())) {
            				startTime = job.getStartTime();
            				// finish time for previous stage is assumed to be 
            				// equal to start time of next stage
            				stage.setEndTime(job.getStartTime());
                    		// write the stage info
                    		WRITER.addHiveStage(stage);
                    		break;
            			}
            		}        			
        		}
        		// new stage
        		stage = new HiveStage();
        		stage.setStageId(stageInfo.getStageId());
        		// FIXME: not sure why hadoop job id is null in some cases;
        		// it has a side-effect of stages not getting profiled
        		if(stageInfo.getHadoopJobId() != null) {
        			stage.setHadoopJobId(stageInfo.getHadoopJobId());
        		} else {
        			stage.setHadoopJobId("");
        		}
        		//TODO: serialize and store
        		stage.setHiveTaskInfos("");
        		List<String> children = stageInfo.getDependentChildren();
        		if(children != null && children.size() > 0) {
        			stage.setDependentChildren(StringUtils.join(children, ','));
        		} else {
        			stage.setDependentChildren("");
        		}
        		stage.setStartTime(startTime);
        		// query identifer
        		// FIXME: requires three queries to fetch the correct identifier
        		Long[] queryId = WRITER.fetchCurrentQueryId();
        		if(queryId == null || queryId.length != 3) {
        			return;
        		}
        		stage.setWid(queryId[0]);
        		stage.setCid(queryId[1]);
        		stage.setQid(queryId[2]);
        	} else{
        		System.out.println("Stage " + stageInfo + " is of type: " + stageInfo.getStageType());
        	}
        }
        if(stage != null) {
        	// finish time for last stage is assumed to be 
        	// equal to end time of query
        	stage.setEndTime(hiveQueryInfo.getEndTime());
        	// write the stage info
        	WRITER.addHiveStage(stage);
		}
	}

}
