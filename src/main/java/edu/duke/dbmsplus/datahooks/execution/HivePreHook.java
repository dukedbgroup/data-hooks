/**
 * 
 */
package edu.duke.dbmsplus.datahooks.execution;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

/**
 * Setting params to enable job histories collection.
 * Hive property hive.exec.pre.hooks is set to
 * this fully qualified class in order to enable this.
 *
 * @author Jie Li
 *
 */

public class HivePreHook implements ExecuteWithHookContext {

    @Override
	public void run(HookContext hookContext) throws Exception {
	    // System.out.println("HivePreHook"); // no stdout/stderr allowed (too intrusive)
        HiveConf hiveConf = hookContext.getConf();

        try {
	        HookUtils.enableJobHistoryCollection(hiveConf, hookContext.getQueryPlan().getQueryId());
            try {
                HookUtils.populatePreExecutionInfo(hiveConf);
            } catch (Throwable e) {
                  HookUtils.errorMessage("ERROR: HivePreHook.run()_pre: " + e + " " +
                                         org.apache.hadoop.util.StringUtils.stringifyException(e));
            }
            HookUtils.collectQueryInfo(hookContext, null, null, null, null, 0, true, "pre");
        } catch (Throwable e) {
            if (Thread.interrupted() || e instanceof InterruptedException ||
                  (e.getCause()!=null && e.getCause() instanceof InterruptedException)) {
                // looks like cli hive job killed.
                ; // ignore
            } else {
                // Throwing Exception would kill query, so avoid that
                HookUtils.errorMessage("ERROR: HivePreHook.run(): " + e + " " +
                                         org.apache.hadoop.util.StringUtils.stringifyException(e));
            }
        }
    }
 
}
