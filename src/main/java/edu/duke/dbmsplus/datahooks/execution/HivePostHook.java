package edu.duke.dbmsplus.datahooks.execution;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

/**
 * Setting params to enable job histories collection.
 * Hive property hive.exec.post.hooks is set to
 * this fully qualified class in order to enable this.
 * @author Jie Li
 *
 */
public class HivePostHook implements ExecuteWithHookContext {

    @Override
  public void run(HookContext hookContext) throws Exception {
        try {
            HookUtils.collectQueryInfo(hookContext, null, null, null, null, 0, false, "normal");
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
    }

}