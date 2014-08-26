/**
 * 
 */
package edu.duke.dbmsplus.datahooks.execution;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

/**
 * Used for thoth data collection.
 * Add this class as hive.exec.pre.hooks
 * @author mayuresh
 */
public class ThothPreHook implements ExecuteWithHookContext {

	@Override
	public void run(HookContext hookContext) throws Exception {
		HookUtils.populatePreExecutionInfo(hookContext.getConf());
	}

}
