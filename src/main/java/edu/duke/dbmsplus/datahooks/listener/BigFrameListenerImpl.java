/**
 * 
 */
package edu.duke.dbmsplus.datahooks.listener;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

import bigframe.workflows.events.BigFrameListener;
import bigframe.workflows.events.ComponentCompletedEvent;
import bigframe.workflows.events.ComponentStartedEvent;
import bigframe.workflows.events.QueryCompletedEvent;
import bigframe.workflows.events.QueryStartedEvent;
import bigframe.workflows.events.WorkflowCompletedEvent;
import bigframe.workflows.events.WorkflowStartedEvent;
import edu.duke.dbmsplus.datahooks.conf.HiveServerCredentials;
import edu.duke.dbmsplus.datahooks.connection.JDBCConnector;
import edu.duke.dbmsplus.datahooks.execution.profile.Component;
import edu.duke.dbmsplus.datahooks.execution.profile.ProfileDataWriter;
import edu.duke.dbmsplus.datahooks.execution.profile.Query;
import edu.duke.dbmsplus.datahooks.execution.profile.Workflow;
import edu.duke.dbmsplus.datahooks.semantic.Hook;

/**
 * @author mayuresh
 *
 */
public class BigFrameListenerImpl implements BigFrameListener {

	// complete path of jar of this project
	String jarPath = "/home/biguser/BigFrame/workflows/lib/data-hooks.jar";

	Connection hiveConnection = null;
	Statement hiveStmt = null;

	ProfileDataWriter writer = null;

	AtomicLong workflowId = null, componentId, queryId;
	Long queryStartTime, componentStartTime, workflowStartTime; 

	// TODO: remove this once hiveserver2 is set up
	Boolean runExplain = true;

	public BigFrameListenerImpl(String jarPath) {
		this.jarPath = jarPath;
		writer = new ProfileDataWriter();
		setUpHiveClient();
	}

	/**
	 * Hive 0.12 doesn't keep added jars in session, 
	 * so the queries depending on the jar (i.e. all the queries) fail.
	 * Can't use this until Hive is patched.
	 */
	private void setUpHiveClient() {
		hiveConnection = JDBCConnector.connectHive(
				HiveServerCredentials.CONNECTION_STRING,
				HiveServerCredentials.USERNAME,
				HiveServerCredentials.PASSWORD);
		try {
			hiveStmt = hiveConnection.createStatement();
			hiveStmt.execute("add jar " + jarPath);
			hiveStmt.execute("set hive.exec.driver.run.hooks=" + Hook.class.getName());
			hiveStmt.execute("set hive.semantic.analyzer.hook=" + Hook.class.getName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void cleanup() {
		try {
			if(hiveStmt != null) {
				hiveStmt.close();
			}
			if(hiveConnection != null) {
				hiveConnection.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * prepends query with 'EXPLAIN' and runs on hive
	 * @param query
	 */
	private void runHiveQuery(String query) {
		String explainQuery = "EXPLAIN " + query;
		try {
			//			runQueryInClient(explainQuery);
			System.out.println("** Firing Hive query: " + explainQuery);
			hiveStmt.execute(explainQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see bigframe.workflows.events.BigFrameListener#onComponentCompleted(bigframe.workflows.events.ComponentCompletedEvent)
	 */
	@Override
	public void onComponentCompleted(ComponentCompletedEvent arg0) {
		// end time
		Long endTime = System.currentTimeMillis();

		// component object
		Component component = new Component();
		component.setCid(componentId.get());
		component.setWid(workflowId.get());
		component.setName(arg0.getName());
		component.setEngine(arg0.getEngine());
		component.setStartTime(componentStartTime);
		component.setEndTime(endTime);

		// write to database
		writer.addComponent(component);
	}

	/* (non-Javadoc)
	 * @see bigframe.workflows.events.BigFrameListener#onComponentStarted(bigframe.workflows.events.ComponentStartedEvent)
	 */
	@Override
	public void onComponentStarted(ComponentStartedEvent arg0) {
		// component start time
		componentStartTime = System.currentTimeMillis();

		// set component ID
		componentId.incrementAndGet();
		queryId = new AtomicLong(0);
	}

	/* (non-Javadoc)
	 * @see bigframe.workflows.events.BigFrameListener#onQueryCompleted(bigframe.workflows.events.QueryCompletedEvent)
	 */
	@Override
	public void onQueryCompleted(QueryCompletedEvent arg0) {
		// end time
		Long endTime = System.currentTimeMillis();

		// query object
		Query query = new Query();
		query.setQid(queryId.get());
		query.setCid(componentId.get());
		query.setWid(workflowId.get());
		query.setQueryString(arg0.getQueryString());
		query.setEngine(arg0.getEngine());
		query.setStartTime(queryStartTime);
		query.setEndTime(endTime);

		// write to database
		writer.addQuery(query);
	}

	/* (non-Javadoc)
	 * @see bigframe.workflows.events.BigFrameListener#onQueryStarted(bigframe.workflows.events.QueryStartedEvent)
	 */
	@Override
	public void onQueryStarted(QueryStartedEvent arg0) {
		// record start time
		queryStartTime = System.currentTimeMillis();

		// set query ID
		queryId.incrementAndGet();

		// run query for semantic analysis
		if(runExplain) {
			runHiveQuery(arg0.getQueryString());
		}
	}

	/* (non-Javadoc)
	 * @see bigframe.workflows.events.BigFrameListener#onWorkflowCompleted(bigframe.workflows.events.WorkflowCompletedEvent)
	 */
	@Override
	public void onWorkflowCompleted(WorkflowCompletedEvent arg0) {
		// end time
		Long endTime = System.currentTimeMillis();

		// workflow object
		Workflow workflow = new Workflow();
		workflow.setWid(workflowId.get());
		workflow.setName(arg0.getName());
		workflow.setUserName(arg0.getUserName());
		workflow.setStartTime(workflowStartTime);
		workflow.setEndTime(endTime);

		// write to database
		writer.addWorkflow(workflow);
	}

	/* (non-Javadoc)
	 * @see bigframe.workflows.events.BigFrameListener#onWorkflowStarted(bigframe.workflows.events.WorkflowStartedEvent)
	 */
	@Override
	public void onWorkflowStarted(WorkflowStartedEvent arg0) {
		// record start time
		workflowStartTime = System.currentTimeMillis();

		// set workflow ID
		if(workflowId == null) {
			workflowId = new AtomicLong(writer.fetchLastWorkflowId());
		}
		workflowId.incrementAndGet();
		componentId = new AtomicLong(0);
	}

	/**
	 * FIXME: Temporarily using this so as to use hiveserver which supports 
	 * only one connection per thread. Once hiveserver2 is fixed, hive execution
	 * can run on another connection.
	 * @return
	 */
	public Connection getHiveConnection() {
		return hiveConnection;
	}

	/**
	 * FIXME: Temporarily adding this. Once the hooks are set for a hive 
	 * workflow, there is no need to run an explain query.
	 */
	public void turnOffSemanticQuery() {
		runExplain = false;
		try {
			if(hiveStmt != null) {
				hiveStmt.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
