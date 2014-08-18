/**
 * 
 */
package edu.duke.dbmsplus.datahooks.execution;

import java.util.List;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

import edu.duke.dbmsplus.datahooks.execution.pojo.HiveOperatorInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveQueryInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveStageInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveTableInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveTaskInfo;

/**
 * Used for thoth data collection.
 * Add this class as hive.exec.post.hooks
 * @author mayuresh
 *
 */
public class Hook implements ExecuteWithHookContext {

	@Override
	public void run(HookContext hookContext) throws Exception {
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

for (HiveTableInfo table: hiveQueryInfo.getFullTableNameToInfos().values()) {
	table.getBaseTableName();
	table.getPartitionCols();
	table.getPath();
}
List<HiveStageInfo> stages = hiveQueryInfo.getHiveStageInfos();
for (HiveStageInfo stage: stages) {
	stage.getHadoopJobId();
	stage.getStageId();
	stage.getStageType();
	stage.getStartTime();
	stage.getEndTime();
	List<HiveTaskInfo> tasks = stage.getHiveTaskInfos();
	for (HiveTaskInfo task: tasks) {
		task.getTaskType();
		List<HiveOperatorInfo> operators = task.getHiveOperatorInfos();
		for(HiveOperatorInfo operator: operators) {
			operator.getOperatorType();
		}
	}
}		
	}

}
