package edu.duke.dbmsplus.datahooks.execution;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * Hive property hive.exec.failure.hooks is set to
 * this fully qualified class in order to enable this.
 *
 * Created: 20131218T1759
 *
 * @author pbaclace
 */
public class HiveFailHook implements ExecuteWithHookContext {
    /**
     * @param hookContext The hook context passed to each hooks.
     * @throws Exception
     */
    @Override
    public void run(HookContext hookContext) throws Exception {
        try {
            HiveConf conf = hookContext.getConf();
            Throwable theThrowable = null;
            StringBuilder comments = new StringBuilder(500);
            String tid = null;
            String jid = null;
            String kind = null;
            int retCode = 0;
            // Properties allConfProps = conf.getAllProperties();

            if (HookUtils.STACKTRACES_VIA_HIVE) {
                // restore captureStacktraces orig (JOB_DEBUG_CAPTURE_STACKTRACES) from conf if present
                if (conf.get("starfish.state.orig.stacktraces") != null) {
                    // if orig present, we assume it was false
                    HiveConf.setBoolVar(conf, HiveConf.ConfVars.JOB_DEBUG_CAPTURE_STACKTRACES, false);
                }

                if (conf.getBoolVar(HiveConf.ConfVars.LOCALMODEAUTO)) {
                    // extract more info when  hive.exec.mode.local.auto=true;
                    for (Map.Entry<String, List<String>> entry : SessionState.get().getLocalMapRedErrors().entrySet()) {
                        comments.append("ID: ").append(entry.getKey());
                        for (String line : entry.getValue()) {
                            comments.append(line);
                        }
                        comments.append("; ");
                    }
                }
                // get the stacktrace (in some distributions), put in SessionState by JobDebugger
                for (Map.Entry<String, List<List<String>>> entry :
                  SessionState.get().getStackTraces().entrySet()) {

                    comments.append("{");
                    for (List<String> stackTrace : entry.getValue()) {
                        // Only print the first line of the stack trace as it contains the error message, and other
                        // lines may contain line numbers which are volatile
                        // Also only take the string after the first two spaces, because the prefix is a date and
                        // and time stamp
                        comments.append(StringUtils.substringAfter(StringUtils.substringAfter(stackTrace.get(0), " "), " "));
                        comments.append("; ");
                    }
                    comments.append("}");
                }
            }

            List<TaskRunner> completeTaskList = hookContext.getCompleteTaskList();
            Field taskResultField = accessTaskResultField();
            if (taskResultField != null) {
                for (TaskRunner taskRunner : completeTaskList) {
                    TaskResult taskResult = (TaskResult) taskResultField.get(taskRunner);
                    // get non-running, failed jobs
                    if (!taskResult.isRunning() && taskResult.getExitVal() != 0) {
                        Task<? extends Serializable> task = taskRunner.getTask();
                        tid = task.getId();
                        jid = task.getJobID();
                        kind = task.getClass().getName();
                        retCode = taskResult.getExitVal();
                        // we are only expecting one failure with exception
                    }
                }
            }
            HookUtils.collectQueryInfo(hookContext, theThrowable, tid, jid, kind, retCode, false, comments.toString());
        } catch (Throwable e) {
            if (Thread.interrupted() || e instanceof InterruptedException ||
                  (e.getCause()!=null && e.getCause() instanceof InterruptedException)) {
                // looks like cli hive job killed.
                ; // ignore
                } else {
                    // Throwing Exception would kill client, so avoid that.
                    //  record error
                    HookUtils.errorMessage("ERROR: HiveFailHook.run(): " + e + " " +
                                     org.apache.hadoop.util.StringUtils.stringifyException(e));
            }
        }
    }

    /**
     * Accessess the TaskResult of the completed task
     *
     * @return reflection Field or null.
     */
    private Field accessTaskResultField() {
        Field field = null;
        try {
            field = TaskRunner.class.getDeclaredField("result");
            field.setAccessible(true);
        }
        catch (Exception e) {
            HookUtils.errorMessage("ERROR: HiveFailHook.accessTaskResultField(): Cannot access to TaskResult at " +
                                     TaskRunner.class.getName() + " e=" + e + " " +
                                     org.apache.hadoop.util.StringUtils.stringifyException(e));
        }
        return field;
    }

}
