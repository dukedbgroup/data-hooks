package edu.duke.dbmsplus.datahooks.execution;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExtractDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.ForwardDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.HashTableDummyDesc;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewForwardDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.duke.dbmsplus.datahooks.execution.pojo.HiveColumnInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveFilterInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveOperatorInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveQueryInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveQueryInfo.OutputType;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveStageInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveTableInfo;
import edu.duke.dbmsplus.datahooks.execution.pojo.HiveTaskInfo;
// not in hive10 // import org.apache.hive.common.util.HiveVersionInfo;

/**
 * This class is intended for hook internal use and will not be used by other
 * components.
 * 
 * @author Jie Li, Shivnath Babu, Paul Baclace
 * 
 */
public class HookUtils {
	private static final Log LOG = LogFactory.getLog(HookUtils.class);
	/** base hdfs path for hive logs, default is DEFAULT_HIVE_RESULT_DIR or /tmp/starfish-hive-dir */
	public static final String HIVE_RESULT_DIR_CONF = "starfish.hive.hdfs.dir";
//    public static final String HIVE_RESULT_DIR_CONF2 = "com.unraveldata.hive.hdfs.dir";
	public static final String DEFAULT_HIVE_RESULT_DIR = "/tmp/unravel-hive-dir"; // HDFS

    /** relative path to that specified by starfish.hive.hdfs.dir */
	public static final String HADOOP_USER_JOB_HISTORIES_OLD = "hadoop.job.history.user.location";
    /** relative path to that specified by starfish.hive.hdfs.dir */
	public static final String HADOOP_USER_JOB_HISTORIES_NEW = "mapreduce.job.userhistorylocation";
    public static final String[] EMPTY_STRING_ARRAY = new String[0];
    /** true if Hive JobDebugger is used to extract stacktraces on failure (only usable on Hive 0.12 Apache
     * on Apache Hadoop)
     */
    public final static boolean STACKTRACES_VIA_HIVE = false;
    public static final String TITLE_SELECT = "select";
    public static final String TITLE_OUTPUT_COLS = "outputCols";

    /**
     * Get base dir for hive logs; has sudirs indicating state of processing.
     * @param conf for hive job or null if system props should be used.
     * @return hdfs abs path to base dir for hive logs.
     */
	public static String getHiveResultDir(Configuration conf) {
        String sysValue = System.getProperty(HIVE_RESULT_DIR_CONF);
//        if (sysValue != null) return sysValue;
//        sysValue = System.getProperty(HIVE_RESULT_DIR_CONF2);
        if (sysValue != null) return sysValue;
        if (conf == null) return DEFAULT_HIVE_RESULT_DIR;
		return conf.get(HIVE_RESULT_DIR_CONF, DEFAULT_HIVE_RESULT_DIR);
	}

    // seems unused 
    //private static HiveQueryInfo currentHiveQueryInfo;

	public static void enableJobHistoryCollection(Configuration conf,
			String queryId) throws IOException, URISyntaxException {
		String hiveTopDir = getHiveResultDir(conf);
        HiveLogLayoutManager logLayout = new HiveLogLayoutManager(hiveTopDir);
		String queryDirPath = logLayout
				.getQueryDirPath(queryId, HiveLogLayoutManager.State.EXECUTING);

		FileSystem fs = FileSystem.get(conf);
		if (conf.getBoolean("unravel.emr.profile.enabled", false)) {
		    // System.out.println("enabling collecting profiles to S3...");
		    String s3BucketPath = conf.get("unravel.s3.bucket.path");
		    String awsId = conf.get("fs.s3.awsAccessKeyId");
		    queryDirPath = s3BucketPath + "/" + awsId;
		    fs = FileSystem.get(new URI(queryDirPath), conf);
		}

        // ensure top level state indicating dirs exist with permission 0777
        //
        ensureStateDirsExist(conf, logLayout);


        // The query dest dir must be created by the user that is running the hive job.
        //
		fs.mkdirs(new Path(queryDirPath), new FsPermission((short) 0777));

		// create the query dir by the current user and set permission to everyone
		// otherwise it might be created by the 'mapred' user in a typical enterprise Hadoop env

		// System.out.println("Before: value of parameter " + HADOOP_USER_JOB_HISTORIES_OLD + ": " 
		// + conf.getRaw(HADOOP_USER_JOB_HISTORIES_OLD));
		// System.out.println("Before: value of parameter " + HADOOP_USER_JOB_HISTORIES_NEW + ": " 
		// + conf.getRaw(HADOOP_USER_JOB_HISTORIES_NEW));

		// ToDo: enable/disable this via hive-site.xml property (UNRAVEL-53)
		conf.set(HADOOP_USER_JOB_HISTORIES_OLD, queryDirPath);
		conf.set(HADOOP_USER_JOB_HISTORIES_NEW, queryDirPath);


		// System.out.println("After: value of parameter " + HADOOP_USER_JOB_HISTORIES_OLD + ": " 
		// + conf.getRaw(HADOOP_USER_JOB_HISTORIES_OLD));
		// System.out.println("After: value of parameter " + HADOOP_USER_JOB_HISTORIES_NEW + ": " 
		// + conf.getRaw(HADOOP_USER_JOB_HISTORIES_NEW));
	}

    /**
         * Ensure top level state indicating dirs exist with permission 0777.
         * Idempotent.
         * @param conf
         * @throws IOException
         */
    public static void ensureStateDirsExist(Configuration conf) throws IOException {
        String hiveTopDir = getHiveResultDir(conf);
        HiveLogLayoutManager logLayout = new HiveLogLayoutManager(hiveTopDir);
        ensureStateDirsExist(conf,logLayout);
    }

    /**
     * Ensure top level state indicating dirs exist with permission 0777.
     * Idempotent.
     * @param conf
     * @param logLayout
     * @throws IOException
     */
    public static void ensureStateDirsExist(Configuration conf, HiveLogLayoutManager logLayout) throws IOException {
        // ToDo: this really should be done by a particular installation-time user, not just the first user that goes down this path; consider this a last-chance fixup
        Configuration conf777 = new Configuration(conf);
        FsPermission.setUMask(conf777, new FsPermission((short) 0));
        FileSystem fs777 = FileSystem.get(conf777);

        for (HiveLogLayoutManager.State state : HiveLogLayoutManager.State.values()) {
            try {
                fs777.mkdirs(new Path(logLayout.getTopDirPath(state)), new FsPermission((short)0777));
            } catch (IOException ignored) { }
            try {
                fs777.setPermission(new Path(logLayout.getTopDirPath(state)), new FsPermission((short)0777));
            } catch (IOException ignored) { }
        }
    }

    public static void populatePreExecutionInfo(HiveConf hiveConf) {
        long now = System.currentTimeMillis();
		// record the query start time
        hiveConf.setLong("starfish.query_start", now);
        hiveConf.setLong("unravel.hive.query.start", now);
        // starfish was here
        hiveConf.set("starfish.version", "1.0.0"); // ToDo: put build number
	}

	/**
	 * Dump the HiveQueryInfo into a file, located by the setting HIVE_RESULT_DIR_CONF.
	 * Also
	 * Note, currently we don't want to have the DDL-only queries, like "DROP TABLE x".
	 * @param hiveQueryInfo
	 * @param conf
	 * @throws JsonGenerationException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public static void dumpQueryPlanJson(HiveQueryInfo hiveQueryInfo,
                                         Configuration conf, boolean isPreliminary)
            throws JsonGenerationException,
			JsonMappingException, IOException, URISyntaxException {

		String resultDirPath = getHiveResultDir(conf);

		HiveLogLayoutManager logLayoutManager = new HiveLogLayoutManager(
				resultDirPath);

		String queryPlanFilePath;
        File tmpFile;
		if (isPreliminary) {
            queryPlanFilePath = logLayoutManager.getPreQueryPlanFilePath(hiveQueryInfo
                                                                           .getQueryId(), HiveLogLayoutManager.State.EXECUTING);
            tmpFile = File.createTempFile("HiveQueryInfoPre", ".json");
        } else {
            queryPlanFilePath = logLayoutManager.getQueryPlanFilePath(hiveQueryInfo
            				.getQueryId(), HiveLogLayoutManager.State.EXECUTING);
            tmpFile = File.createTempFile("HiveQueryInfo", ".json");
        }

		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.writeValue(tmpFile, hiveQueryInfo); // write to local temp file

		FileSystem fs = FileSystem.get(conf);
		if (conf.getBoolean("unravel.emr.profile.enabled", false)) {
		    String s3BucketPath = conf.get("unravel.s3.bucket.path");
		    String awsId = conf.get("fs.s3.awsAccessKeyId");
		    queryPlanFilePath = s3BucketPath + "/" + awsId + "/"
			+ HiveLogLayoutManager.QUERY_INFO_FILE;
		    fs = FileSystem.get(new URI(queryPlanFilePath), conf);
		}

        // transfer from local file to hdfs (or S3) (Note: destination is a file)
		fs.copyFromLocalFile(new Path(tmpFile.getAbsolutePath()), new Path(queryPlanFilePath));
        rmrLocal(tmpFile);


        }

    /**
     * Cleanup the query info state held in heap.
     *
     * @param hiveQueryInfo
     */
    public static void cleanupQueryInfo(HiveQueryInfo hiveQueryInfo) {
        ;
    }

    /**
     *
     *
     * @param hookContext
     * @param isPreliminary
     * @return new instance of HiveQueryInfo with values from queryPlan.
     */
	public static HiveQueryInfo buildHiveQueryInfo(HookContext hookContext, boolean isPreliminary) {
        QueryPlan queryPlan = hookContext.getQueryPlan();
		HiveQueryInfo hiveQueryInfo = new HiveQueryInfo();
		String queryString = queryPlan.getQueryString();
		String queryId = queryPlan.getQueryId();

		hiveQueryInfo.setQueryId(queryId);
		hiveQueryInfo.setQueryString(queryString);

        final long endTime = System.currentTimeMillis();
		// for the start time, get it from conf as prior to Hive 0.9 there is no such
		// info in the query plan
        final HiveConf hiveConf = hookContext.getConf();
        long tval = hiveConf.getLong("starfish.query_start", 0L);
        hiveQueryInfo.setStartTime(tval);

		// get the end time
        if (! isPreliminary) {
            hiveQueryInfo.setEndTime(endTime);
        }
        hiveQueryInfo.setVersion(hiveConf.get("starfish.version", "unset"));

        // HiveVersionInfo
        //hiveQueryInfo.setHiveVersion(HiveVersionInfo.getVersion());
        hiveQueryInfo.setHiveVersion("0.10.0");
        //hiveQueryInfo.setHiveCompileDate(HiveVersionInfo.getDate());
        //hiveQueryInfo.setHiveCompiledBy(HiveVersionInfo.getUser());
        //hiveQueryInfo.setHiveRevision(HiveVersionInfo.getRevision());

        // lineage
        LineageInfo lineageInfo = hookContext.getLinfo();
        if (lineageInfo != null &&  ! isPreliminary) {
            // NPE in hive10 // populateLineageInfo(hiveQueryInfo, lineageInfo);
        }

		// read the table info
		populateTableInfo(hiveQueryInfo, queryPlan);

		// read the stage info
		for (Task<? extends Serializable> task : queryPlan.getRootTasks()){
			populateHiveStageInfos(hiveQueryInfo, task);
		}
		return hiveQueryInfo;
	}

    /** Use the LineageInfo.
     * NPE in hive10.
     * @param hiveQueryInfo
     * @param lineageInfo
     */
    static void populateLineageInfo(HiveQueryInfo hiveQueryInfo, LineageInfo lineageInfo) {
        final StringBuilder sb = new StringBuilder(500);
        sb.append("Lineage: ");
        int nlines = 0;
        LinkedList<Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency>> entry_list =
          new LinkedList<Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency>>(lineageInfo.entrySet());
        Collections.sort(entry_list, new DependencyKeyComp());
        Iterator<Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency>> iter = entry_list.iterator();
        while (iter.hasNext()) {
            final Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency> entry = iter.next();
            final LineageInfo.DependencyKey depK = entry.getKey();
            final LineageInfo.Dependency dep = entry.getValue();
            if(dep == null) {
                continue;
            }
            if (nlines++ > 0) {
                sb.append("\n");
            }
            final LineageInfo.DataContainer dc = depK.getDataContainer();

            LineageInfo.DependencyType dt = dep.getType();
            String dtText;
            /*
             * Enum DependencyType. This enum has the following values:
             * 1. SIMPLE - Indicates that the column is derived from another table column
             *             with no transformations e.g. T2.c1 = T1.c1.
             * 2. EXPRESSION - Indicates that the column is derived from a UDF, UDAF, UDTF or
             *                 set operations like union on columns on other tables
             *                 e.g. T2.c1 = T1.c1 + T3.c1.
             * 4. SCRIPT - Indicates that the column is derived from the output
             *             of a user script through a TRANSFORM, MAP or REDUCE syntax
             *             or from the output of a PTF chain execution.
             */
            switch (dt) {
                case SIMPLE: dtText="SIMPLE"; break;
                case SCRIPT: dtText="SCRIPT"; break;
                case EXPRESSION: dtText="EXPRESSION"; break;
                default: dtText="unknown";break;
            }
            String expr = dep.getExpr(); // null as of 2014-01
            if (dc.isPartition()) {
                Partition part = dc.getPartition();
                sb.append(part.getTableName());
                // list of field names // List<String> flist = part.getValues();
                sb.append(" PARTITION(");
                int i = 0;
                for (FieldSchema fs : dc.getTable().getPartitionKeys()) {
                    if (i != 0) {
                        sb.append(",");
                    }
                    sb.append(fs.getName() + "=" + part.getValues().get(i++));
                }
                sb.append(")");
            } else {
                sb.append(dc.getTable().getTableName());
            }
            sb.append("." + depK.getFieldSchema().getName() + " " + dtText + " ");

            sb.append("[");
            int k = 0;
            for(LineageInfo.BaseColumnInfo col: dep.getBaseCols()) {
                FieldSchema field = col.getColumn();
                if (k++ > 0) sb.append(", ");
                sb.append("(")
                  .append(col.getTabAlias().getTable().getTableName())
                  .append(")")
                  .append(col.getTabAlias().getAlias())
                  .append(".")
                  .append(field.getName())
                  .append(" ")
                  .append(field.getType())
                  .append(" ")
                  .append(field.getComment());
            }
            sb.append("]");
        }
        hiveQueryInfo.setDetails(sb.toString());
    }

    private static void populateTableInfo(HiveQueryInfo hiveQueryInfo,
			QueryPlan queryPlan) {
		for (ReadEntity entity : queryPlan.getInputs()) {
			Table table = entity.getTable();
			if (table != null) {
				HiveTableInfo tableInfo = new HiveTableInfo();
				tableInfo.setDatabase(table.getDbName());
				tableInfo.setBaseTableName(table.getTableName());
				tableInfo.setFullTableName(table.getDbName() + "." + table.getTableName());
				tableInfo.setBucketCols(table.getBucketCols());
				tableInfo.setNumBuckets(table.getNumBuckets());

				// partition
				List<String> partCols = new ArrayList<String>();
				for (FieldSchema schema : table.getPartitionKeys()) {
					partCols.add(schema.getName());
				}
				tableInfo.setPartitionCols(partCols);

				// sort
				List<String> sortCols = new ArrayList<String>();
				List<String> sortProps = new ArrayList<String>();
				for (Order order : table.getSortCols()) {
					sortCols.add(order.getCol());
					sortProps
							.add(order.getOrder() == 1 ? HiveTableInfo.SORT_ASC
									: HiveTableInfo.SORT_DSC);
				}
				tableInfo.setSortCols(sortCols);
				tableInfo.setSortProps(sortProps);
				if (table.getPath()!=null)
					tableInfo.setPath(table.getPath().toString());

				hiveQueryInfo.getFullTableNameToInfos().put(tableInfo.getFullTableName(), tableInfo);
			} // if table != null
		}
	}

	private static void populateHiveStageInfos(HiveQueryInfo hiveQueryInfo,
			Task<? extends Serializable> task) {
		if (task == null)
			return;

		if (hiveQueryInfo.findHiveStageInfo(task.getId()) != null) {
			// already processed this stage
			return;
		}

		HiveStageInfo hiveStageInfo = new HiveStageInfo();
                hiveStageInfo.setStageId(task.getId());
		hiveStageInfo.setHadoopJobId(task.getJobID());
		hiveStageInfo.setStageType(task.getType().name());

		if (task instanceof ExecDriver){
			// add all input paths
			// mayuresh: hive 0.12 provides getPathToAliases() functionality only on MapWork, not on MapReduceWork
			hiveStageInfo.getInputPaths().addAll(((ExecDriver)task).getWork().getMapWork().getPathToAliases().keySet());

		    // add alias to path
			Map<String, ArrayList<String>> pathToAlias = ((ExecDriver)task).getWork().getMapWork().getPathToAliases();
 			for (Map.Entry<String, ArrayList<String>> entry: pathToAlias.entrySet()) {
 				String path = entry.getKey();
 				List<String> aliasArray = entry.getValue();
 				for (String alias : aliasArray) {
 					alias = getLastAlias(alias);
 					hiveStageInfo.getAliasToPath().put(alias, path);
 				}
 			}

			// get the mapping between table alias and table names
			Map<String, PartitionDesc> map = ((ExecDriver)task).getWork().getMapWork().getAliasToPartnInfo();
			for (Map.Entry<String, PartitionDesc> entry : map.entrySet()) {
				String alias = entry.getKey();
				alias = getLastAlias(alias);
				String tableName = entry.getValue().getTableName();
				hiveStageInfo.getAliasToFullTableName().put(alias, tableName);
			}
		}

		// System.out.println("Add stage:" + hiveStageInfo.getStageId());

		hiveQueryInfo.getHiveStageInfos().add(hiveStageInfo);

		// Add map/reduce tasks of this stage
		populateHiveTaskInfos(hiveStageInfo.getHiveTaskInfos(), task, hiveStageInfo);

		// Recursively add other hive stages and the graph info
		// Reference ExplainTask#outputDependencies
		if (task.getParentTasks() == null || task.getParentTasks().isEmpty()) {
			hiveStageInfo.setRoot(true);
		}

		// dependency parent/children
		if (task.getChildTasks() != null) {
			for (Task<? extends Serializable> child : task.getChildTasks()) {
				hiveStageInfo.getDependentChildren().add(child.getId());
				populateHiveStageInfos(hiveQueryInfo, child);
			}
		}

		// backup stage
		Task<? extends Serializable> currBackupTask = task.getBackupTask();
		if (currBackupTask != null) {
			hiveStageInfo.getBackupChildren().add(currBackupTask.getId());
			populateHiveStageInfos(hiveQueryInfo, currBackupTask);
		}

		// conditional children
		if (task instanceof ConditionalTask
				&& ((ConditionalTask) task).getListTasks() != null) {
			for (Task<? extends Serializable> con : ((ConditionalTask) task)
					.getListTasks()) {
				hiveStageInfo.getConditionalChildren().add(con.getId());
				populateHiveStageInfos(hiveQueryInfo, con);
			}
		}

		// named output and partitions
		if (task instanceof MoveTask || task instanceof DDLTask) {
			populateNamedOutput(hiveQueryInfo, task);
		}
	}

	private static void populateNamedOutput(HiveQueryInfo queryInfo,
			Task<? extends Serializable> task) {
		if (task instanceof MoveTask) {
			MoveTask moveTask = (MoveTask) task;
			MoveWork moveWork = moveTask.getWork();
			if (moveWork.getLoadTableWork() != null) {
				LoadTableDesc tableDesc = moveWork.getLoadTableWork();
				queryInfo.setNamedOutput(tableDesc.getTable().getTableName());
				queryInfo.setOutputPartitions(tableDesc.getPartitionSpec());
				queryInfo.setOutputType(OutputType.TABLE);
			} else if (moveWork.getLoadFileWork() != null
					&& queryInfo.getOutputType() != OutputType.TABLE) {
				LoadFileDesc fileDesc = moveWork.getLoadFileWork();
				queryInfo.setNamedOutput(fileDesc.getTargetDir());
				queryInfo.setOutputType(OutputType.DIRECTORY);
			}
		} else if (task instanceof DDLTask) {
			DDLTask ddlTask = (DDLTask) task;
			DDLWork ddlWork = ddlTask.getWork();
			CreateTableDesc ctd = ddlWork.getCreateTblDesc();
			if (ctd != null) {
				String databaseName = ctd.getDatabaseName();
				if (databaseName == null) {
					databaseName = "default";
				}
				String tableName = databaseName + "." + ctd.getTableName();
				queryInfo.setNamedOutput(tableName);
				queryInfo.setOutputType(OutputType.TABLE);
			}
		}

	}

	private static void populateHiveTaskInfos(List<HiveTaskInfo> hiveTaskInfos,
                                              Task<? extends Serializable> task,
                                              HiveStageInfo hiveStageInfo) {
		if (!(task instanceof ExecDriver))
			return;

		ExecDriver mrTask = (ExecDriver) task;

		// Map Task
		HiveTaskInfo mapTaskInfo = new HiveTaskInfo();
		mapTaskInfo.setTaskId(task.getId() + "MAP");
		mapTaskInfo.setTaskType("MAP");

		// Add Map Operators
		populateHiveOperatorInfos(mapTaskInfo.getHiveOperatorInfos(),
				getGenericOperatorCollection(mrTask.getWork().getMapWork().getAliasToWork()
						.values()), hiveStageInfo);

		hiveTaskInfos.add(mapTaskInfo);

		// Reduce Task
		HiveTaskInfo reduceTaskInfo = new HiveTaskInfo();
		reduceTaskInfo.setTaskId(task.getId() + "REDUCE");
		reduceTaskInfo.setTaskType("REDUCE");

		// Add Reduce Operators
		Collection<Operator> reduceOps = new ArrayList<Operator>();
		if (mrTask.getWork().getReduceWork().getReducer() != null) {
			reduceOps.add(mrTask.getWork().getReduceWork().getReducer());
		}
		populateHiveOperatorInfos(reduceTaskInfo.getHiveOperatorInfos(),
				reduceOps, hiveStageInfo);

		hiveTaskInfos.add(reduceTaskInfo);
	}

	private static void populateHiveOperatorInfos( List<HiveOperatorInfo> hiveOperatorInfos,
                                                   Collection<Operator> operators,
                                                   HiveStageInfo hiveStageInfo) {
		if (operators == null)
			return;

		for (Operator operator : operators) {
			HiveOperatorInfo hiveOperatorInfo = new HiveOperatorInfo();

			populateHiveOperatorDetail(hiveOperatorInfo, operator, hiveStageInfo);

			// Recursively add operators
			Collection<Operator> children = new ArrayList<Operator>();

			populateHiveOperatorInfos(
					hiveOperatorInfo.getChildHiveOperatorInfos(),
					getGenericOperatorCollection(operator.getChildOperators()), hiveStageInfo);

			hiveOperatorInfos.add(hiveOperatorInfo);
		}
	}

    private static void populateHiveOperatorDetail( HiveOperatorInfo hiveOperatorInfo,
                                                    Operator operator,
                                                    HiveStageInfo hiveStageInfo) {
        hiveOperatorInfo.setOperatorId(operator.getOperatorId());
        hiveOperatorInfo.setOperatorType(operator.getType().toString());

        //ToDo: Hive 0.12

        StringBuilder sb = new StringBuilder(400);
        // Add operator comment
        String comment = null;
        OperatorType type = operator.getType();
        try {
            switch (type) {
                case JOIN:
                    // common join
                    JoinDesc desc = (JoinDesc) operator.getConf();
                    composeJoinComment(sb, desc);


                    for (HiveColumnInfo hCol : hiveStageInfo.getPartitionColumns()) {
                        textAppend(sb, "joinKeys", hCol.getColumnName());
                    }
                    // for common join, the join keys are same as the job's partition keys
                    hiveStageInfo.setJoinKeys(hiveStageInfo.getPartitionColumns());
                    hiveStageInfo.setJoinType(hiveOperatorInfo.getOperatorType());
                    comment = sb.toString();
                    break;
                case MAPJOIN: {
                    String mapJoinVariant = null;
                    // map join, including bucket map join and sort merge bucket map join
                    // for SMBMapJoin, the type is also "MAPJOIN" in Hive, which is confusing
                    // thus we set to "SMB_JOIN" by ourselves
                    if (operator instanceof SMBMapJoinOperator) { // Sorted Merge Bucket Map Join Operator
                        hiveOperatorInfo.setOperatorType(HiveOperatorInfo.SMB_JOIN);
                        mapJoinVariant = "Sorted Merge Bucket";
                    }

                    // for map join, the join keys can be obtained directly
                    JoinDesc joinDesc = (JoinDesc) operator.getConf();
                    for (Map.Entry<Byte, List<ExprNodeDesc>> entry : joinDesc.getExprs().entrySet()) {
                        for (ExprNodeDesc exprNodeDesc : entry.getValue()) {
                            if (exprNodeDesc instanceof ExprNodeColumnDesc) {
                                HiveColumnInfo hiveColumnInfo = new HiveColumnInfo();
                                populateHiveColumnInfo(hiveColumnInfo, (ExprNodeColumnDesc) exprNodeDesc, hiveStageInfo);
                                hiveStageInfo.getJoinKeys().add(hiveColumnInfo);
                            }
                        }
                    }
                    composeJoinComment(sb, joinDesc);

                    if (joinDesc instanceof HashTableSinkDesc) {  //  HashTable Sink Operator
                        HashTableSinkDesc htSink = (HashTableSinkDesc)joinDesc;
                        Map<Byte, List<ExprNodeDesc>> byte2elist = htSink.getKeys();
                        composeKeysComment(sb, byte2elist);
                        textAppend(sb, "Position of Big Table", htSink.getPosBigTable());
                        mapJoinVariant = "HashTable Sink";
                    } else if (joinDesc instanceof MapJoinDesc) {
                        MapJoinDesc mapJoinDesc = (MapJoinDesc)joinDesc;
                        Map<Byte, List<ExprNodeDesc>> byte2elist = mapJoinDesc.getKeys();
                        composeKeysComment(sb, byte2elist);
                        textAppend(sb, "Position of Big Table", mapJoinDesc.getPosBigTable());
                        /*not in hive10 if ( mapJoinDesc.isBucketMapJoin()  &&  mapJoinVariant == null) {
                            mapJoinVariant = "Bucket";
                        }*/
                        }
                    textAppend(sb, "Map Join Variant", mapJoinVariant);

                    hiveStageInfo.setJoinType(hiveOperatorInfo.getOperatorType());
                    comment = sb.toString();
                    break;
                }
                case EXTRACT:
                    ExtractDesc extractDesc = (ExtractDesc) operator.getConf();
                    comment = "extract: " + extractDesc.getCol().getExprString();
                    break;
                case FILTER: {
                    FilterDesc filterDesc = (FilterDesc) operator.getConf();
                    if (filterDesc != null && filterDesc.getPredicate() != null) {
                        comment = filterDesc.getPredicate().getExprString(); //ToDo: is this enough?

                        // also extract the predicate expression
                        if (filterDesc.getPredicate() instanceof ExprNodeGenericFuncDesc) {
                            ExprNodeGenericFuncDesc predicate = (ExprNodeGenericFuncDesc) filterDesc.getPredicate();
                            List<ExprNodeDesc> childExprs = predicate.getChildExprs();
                            if (childExprs != null && childExprs.size() == 2) {
                                ExprNodeDesc child1 = childExprs.get(0);
                                ExprNodeDesc child2 = childExprs.get(1);
                                // one should be column (ExprNodeColumnDesc) and the other one should be
                                // constant (ExprNodeConstantDesc)
                                if (child2 instanceof ExprNodeColumnDesc) {
                                    // swap to make sure the child1 is the column
                                    child2 = childExprs.get(0);
                                    child1 = childExprs.get(1);
                                }
                                if (child1 instanceof ExprNodeColumnDesc && child2 instanceof ExprNodeConstantDesc) {
                                    HiveFilterInfo hiveFilterInfo = new HiveFilterInfo();
                                    populateHiveColumnInfo(hiveFilterInfo, (ExprNodeColumnDesc) child1, hiveStageInfo);
                                    hiveFilterInfo.setExpr(comment);
                                    hiveStageInfo.getConstFilters().add(hiveFilterInfo);
                                }
                            }
                        }
                    }
                    break;
                }
                case FORWARD:
                    ForwardDesc forwardDesc = (ForwardDesc) operator.getConf();
                    break;
                case GROUPBY: {
                    GroupByDesc groupByDesc = (GroupByDesc) operator.getConf();
                    for (ExprNodeDesc enode : groupByDesc.getKeys()) {
                        textAppend(sb, "keys", enode.getExprString());
                    }
                    for (String outCol : groupByDesc.getOutputColumnNames()) {
                        textAppend(sb, TITLE_OUTPUT_COLS, outCol);
                    }

                    for (AggregationDesc agg : groupByDesc.getAggregators()) {
                        textAppend(sb, "aggregations", agg.getExprString());
                    }

                    if (groupByDesc.getAggregators().size() == 0) { // this implies groupby to accomplish distinct
                        textAppend(sb, "distinct", "implied");
                    }
                    //if (groupByDesc.isDistinct()) {
		    //textAppend(sb, "distinct", "explicit");
		    //}
                    comment = sb.toString();
                    break;
                }
                case LIMIT:
                    LimitDesc limitDesc = (LimitDesc) operator.getConf();
                    comment = "limit: " + limitDesc.getLimit();
                    break;
                case SCRIPT:
                    ScriptDesc scriptDesc = (ScriptDesc) operator.getConf();
                    comment = scriptDesc.getScriptCmd();
                    break;
                case SELECT: {
                    SelectDesc selectDesc = (SelectDesc) operator.getConf();
                    List<ExprNodeDesc> enList = selectDesc.getColList();
                    for (ExprNodeDesc exprNodeDesc : enList) {
                        if (exprNodeDesc instanceof ExprNodeFieldDesc) {
                            ExprNodeFieldDesc fieldDesc = (ExprNodeFieldDesc)exprNodeDesc;
                            textAppend(sb, TITLE_SELECT, fieldDesc.getExprString());
                        } else if (exprNodeDesc instanceof ExprNodeColumnDesc) {
                            ExprNodeColumnDesc exprNodeColumnDesc = (ExprNodeColumnDesc)exprNodeDesc;
                            textAppend(sb, TITLE_SELECT, exprNodeColumnDesc.getExprString());
                        } else if (exprNodeDesc instanceof ExprNodeConstantDesc) {
                            ExprNodeConstantDesc exprNodeConstantDesc = (ExprNodeConstantDesc)exprNodeDesc;
                            textAppend(sb, TITLE_SELECT, exprNodeConstantDesc.getExprString());
                        } else if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
                            ExprNodeGenericFuncDesc exprNodeGenericFuncDesc = (ExprNodeGenericFuncDesc)exprNodeDesc;
                            textAppend(sb, TITLE_SELECT, exprNodeGenericFuncDesc.getExprString());
                        }
                    }

                    for (String outCol : selectDesc.getOutputColumnNames()) {
                        textAppend(sb, TITLE_OUTPUT_COLS, outCol);
                    }
                    comment = sb.toString();
                    break;
                }
                case TABLESCAN:  // TableScan: which table
                    TableScanDesc tableScanDesc = (TableScanDesc) operator.getConf();
                    textAppend(sb, "alias", tableScanDesc.getAlias());
                    if (tableScanDesc.getPartColumns() != null) {
                        List<String> clist = tableScanDesc.getPartColumns();
                        for (String col : clist) {
                            textAppend(sb, "partitionCols", col);
                        }
                    }
                    if (tableScanDesc.getFilterExpr() != null) {
                        textAppend(sb, "FilterExpr",  tableScanDesc.getFilterExpr().getExprString());
                    }
                    //if (tableScanDesc.getRowLimit() > 0) {
		    //textAppend(sb, "rowLimit", tableScanDesc.getRowLimit());
		    //}
                    comment = sb.toString();
                    break;
                case FILESINK: {
                    FileSinkDesc fileSinkDesc = (FileSinkDesc) operator.getConf();
                    textAppend(sb, "dirName", fileSinkDesc.getDirName());
                    if (fileSinkDesc.getDirName() != null &&
                          fileSinkDesc.getFinalDirName() != null &&
                          ! fileSinkDesc.getDirName().equals(fileSinkDesc.getFinalDirName())) {
                        textAppend(sb, "finalDirName", fileSinkDesc.getFinalDirName());
                    }
                    textAppend(sb, "numFiles", fileSinkDesc.getNumFiles());
                    if (fileSinkDesc.getPartitionCols() != null) {
                        for (ExprNodeDesc enode : fileSinkDesc.getPartitionCols()) {
                            textAppend(sb, "partitionCols", enode.getExprString());
                        }
                    }
                    comment = sb.toString();
                    break;
                }
                case REDUCESINK: {
                    ReduceSinkDesc reduceSinkDesc = (ReduceSinkDesc) operator.getConf();
                    if (reduceSinkDesc.getPartitionCols() != null) {
                        for (ExprNodeDesc exprNodeDesc : reduceSinkDesc.getPartitionCols()) {
                            if (exprNodeDesc instanceof ExprNodeColumnDesc) {
                                HiveColumnInfo hiveColumnInfo = new HiveColumnInfo();
                                populateHiveColumnInfo(hiveColumnInfo, (ExprNodeColumnDesc) exprNodeDesc, hiveStageInfo);
                                hiveStageInfo.getPartitionColumns().add(hiveColumnInfo);
                            }
                        }
                    }

                    ArrayList<ExprNodeDesc> eNodeList = reduceSinkDesc.getKeyCols();
                    if (eNodeList != null) {
                        for (ExprNodeDesc exprNodeDesc : eNodeList) {
                            textAppend(sb, "key expressions", exprNodeDesc.getExprString());
                        }
                    }
                    eNodeList = reduceSinkDesc.getValueCols();
                    if (eNodeList != null) {
                        for (ExprNodeDesc exprNodeDesc : eNodeList) {
                            textAppend(sb, "value expressions", exprNodeDesc.getExprString());
                        }
                    }
                    eNodeList = reduceSinkDesc.getPartitionCols();
                    if (eNodeList != null) {
                        for (ExprNodeDesc exprNodeDesc : eNodeList) {
                            textAppend(sb, "Map-reduce partition columns", exprNodeDesc.getExprString());
                        }
                    }
                    if (reduceSinkDesc.getOutputKeyColumnNames() != null) {
                        for (String outKey : reduceSinkDesc.getOutputKeyColumnNames()) {
                            textAppend(sb, "outputKey", outKey);
                        }
                    }
                    if (reduceSinkDesc.getOutputValueColumnNames() != null) {
                        for (String outValue : reduceSinkDesc.getOutputValueColumnNames()) {
                            textAppend(sb, "outputCols", outValue);
                        }
                    }
                    if (reduceSinkDesc.getNumReducers() > 0) {
                        textAppend(sb, "numReducers", reduceSinkDesc.getNumReducers());
                    }
                    comment = sb.toString();
                    break;
                }
                case UNION:
                    UnionDesc unionDesc = (UnionDesc) operator.getConf();
                    comment = "union of " + unionDesc.getNumInputs() + " inputs";
                    break;
                case UDTF:
                    UDTFDesc udtfDesc = (UDTFDesc) operator.getConf();
                    comment = "UDTF: " + udtfDesc.getUDTFName();
                    break;
                case LATERALVIEWJOIN:
                    LateralViewJoinDesc lateralViewJoinDesc = (LateralViewJoinDesc) operator.getConf();
                    comment = "Lateral View Join: ";
                    for (String s : lateralViewJoinDesc.getOutputInternalColNames()) {
                        comment += " " + s;
                    }
                    break;
                case LATERALVIEWFORWARD:
                    LateralViewForwardDesc lateralViewForwardDesc = (LateralViewForwardDesc) operator.getConf();
                    comment = "Lateral View Forward";
                    break;
                case HASHTABLESINK:
                    HashTableSinkDesc hashTableSinkDesc = (HashTableSinkDesc) operator.getConf();
                    comment = "conditions: ";
                    for (JoinCondDesc joinCondDesc : hashTableSinkDesc.getConds()) {
                        comment += " " + joinCondDesc.getJoinCondString();
                    }
                    break;
                case HASHTABLEDUMMY:
                    HashTableDummyDesc hashTableDummyDesc = (HashTableDummyDesc) operator.getConf();
                    break;
            }
        } catch (ClassCastException ce) {
            errorMessage("WARN: ignored ClassCastException: " + org.apache.hadoop.util.StringUtils.stringifyException(ce));
        } catch (Throwable e) {
            // ToDo: proactively check for nulls, if needed, to avoid catch overhead
            // just in case a dereference is null (this might be unnecessarily defensive)
            //  Note the NPE
            errorMessage("WARN: ignored NPE: " + org.apache.hadoop.util.StringUtils.stringifyException(e));
            return;
        }
        hiveOperatorInfo.setComment(comment);
    }

    private static void composeKeysComment(StringBuilder sb, Map<Byte, List<ExprNodeDesc>> byte2elist) {
        if (byte2elist != null) {
            for (int i=0; i<5; i++) {
                List<ExprNodeDesc> elist = byte2elist.get(i);
                if (elist != null) {
                    for (ExprNodeDesc enode : elist) {
                        textAppend(sb, "keys", ("" + i + ": " + enode.getExprString()));
                    }
                }
            }
        }
    }

    /**
     *
     * @param sb
     * @param desc
     */
    private static void composeJoinComment(StringBuilder sb, JoinDesc desc) {
        List<JoinCondDesc> jCondList = desc.getCondsList();
        if (jCondList != null) {
            for (JoinCondDesc jDesc : jCondList) {
                textAppend(sb, "condition map", jDesc.getJoinCondString()); // like:  Inner Join 0 to 1
            }
        }

        //condition expressions
        Map<Byte, String> position2expr = desc.getExprsStringMap();
        if (position2expr != null) {
            for (int i=0; i<5; i++) {
                String v = position2expr.get(i);
                if (v == null || v.length() == 0) continue;
                textAppend(sb, "condition expressions", ("" + i + ": " + v));
            }
        }

        String askew = desc.getHandleSkewJoin() ? "true" : "false";
        textAppend(sb, "handleSkewJoin", askew);

        final List<String> outputColumnNames = desc.getOutputColumnNames();
        if (outputColumnNames != null) {
            for (String outCol : outputColumnNames) {
                textAppend(sb, TITLE_OUTPUT_COLS, outCol);
            }
        }

        Map<Byte, String> filterStringMap = desc.getFiltersStringMap();
        if (filterStringMap != null) {
            // "filter predicates"
            for (int i=0; i<5; i++) {
                String s = filterStringMap.get(i);
                if (s != null  &&  s.length() > 0) {
                    textAppend(sb, "filter predicates", ("" + i + ": " + s));
                }
            }
        }

        Map<Integer, String> filterMapString = desc.getFilterMapString();
        if (filterMapString != null) {
            for (int i=0; i<5; i++) {
                String s = filterMapString.get(i);
                if (s != null  &&  s.length() > 0) {
                    textAppend(sb, "filter mappings", ("" + i + ": " + s));
                }
            }
        }

        String nullSafeBlurb = desc.getNullSafeString();
        if (nullSafeBlurb != null) {
            textAppend(sb, "nullSafes", nullSafeBlurb);
        }
    }

    public static Collection<Operator> getGenericOperatorCollection(
			Collection operators) {
		if (operators == null)
			return null;
		Collection<Operator> ret = new ArrayList<Operator>();
		for (Object op : operators) {
			ret.add((Operator) op);
		}
		return ret;
	}

	private static HiveColumnInfo populateHiveColumnInfo(HiveColumnInfo hiveColumnInfo,
                                                         ExprNodeColumnDesc exprNodeColumnDesc, HiveStageInfo hiveStageInfo) {
        //ToDo: is exprNodeColumnDesc.getIsPartitionColOrVirtualCol() useful here? virt. is for stats
		hiveColumnInfo.setTableAlias(exprNodeColumnDesc.getTabAlias());
		hiveColumnInfo.setColumnName(exprNodeColumnDesc.getColumn());

		// get the table name from the alias
		String fullTableName = hiveStageInfo.getAliasToFullTableName().get(hiveColumnInfo.getTableAlias());
		if (fullTableName != null) {
			hiveColumnInfo.setFullTableName(fullTableName);
		}

		return hiveColumnInfo;
	}

	private static String getLastAlias(String aliasString){
		if (aliasString == null) {
			return null;
		}

		String [] alias = aliasString.split(":");
		return alias[alias.length-1];
	}

    static public void rmrLocal(File file) {
        if (! file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rmrLocal(f);
            }
        }
        file.delete();
    }

    public static void collectQueryInfo(HookContext hookContext, Throwable throwable,
                                        String tid, String jid, String taskKind,
                                        int retCode, boolean isPreliminary, String comments)
                 throws IOException, URISyntaxException {

        HiveQueryInfo hiveQueryInfo = null;
        try {
            hiveQueryInfo = HookUtils.buildHiveQueryInfo(hookContext, isPreliminary);
         hiveQueryInfo.setComments(comments);
        } catch (Throwable e) {
            HookUtils.errorMessage("ERROR: HivePostHook.collectQueryInfo()_1: " + e + " " +
                                                 org.apache.hadoop.util.StringUtils.stringifyException(e));
        }

         // if error occurred, then set corresponding fields
         if (hiveQueryInfo != null  &&  retCode != 0) {
             if (throwable != null) { // Hive +0.12 reports this sometimes
                 ArrayList<String> stackFramesWithCausedBy = new ArrayList<String>(50);
                 addFramesRecursively(stackFramesWithCausedBy, throwable);
                 hiveQueryInfo.setFailExStack(stackFramesWithCausedBy.toArray(EMPTY_STRING_ARRAY));
             }
             // set error fields
             hiveQueryInfo.setFailTID(tid);
             hiveQueryInfo.setFailJID(jid);
             hiveQueryInfo.setFailTaskKind(taskKind);
             hiveQueryInfo.setFailRetCode(retCode);
         }

         // serialize it to json in a file
         HookUtils.dumpQueryPlanJson(hiveQueryInfo, hookContext.getConf(), isPreliminary);


        if (! isPreliminary) {
            String resultDirPath = getHiveResultDir(hookContext.getConf());
            HiveLogLayoutManager logLayoutManager = new HiveLogLayoutManager(
            				resultDirPath);
            // transition info in hdfs to hdfs dir indicating files are complete
            logLayoutManager.stateChange(hookContext.getQueryPlan().getQueryId(),
                                          hookContext.getConf(),
                                          HiveLogLayoutManager.State.EXECUTING,
                                          HiveLogLayoutManager.State.COMPLETED);
        }
         // cleanup
         HookUtils.cleanupQueryInfo(hiveQueryInfo);
     }

     private static void addFramesRecursively(ArrayList<String> stackFramesWithCausedBy, Throwable throwable) {
         StackTraceElement[] stackTrace = throwable.getStackTrace();
         stackFramesWithCausedBy.add("Caused By: " + throwable.getClass().getSimpleName() + ", " + throwable + " (" + throwable.getMessage() + ")");
         for (StackTraceElement ste : stackTrace) {
             stackFramesWithCausedBy.add(ste.getClassName() + "." + ste.getMethodName() + ":" +
                                           ste.getFileName() + ":" + ste.getLineNumber());
         }
         Throwable cause = throwable.getCause();
         if (cause != null) {
             addFramesRecursively(stackFramesWithCausedBy, cause);
         }
     }

    /**
     * Write an error message as a log line to a local file with a unique name every second.
     * @param msg
     */
    static public void errorMessage(String msg) {
        final java.text.SimpleDateFormat fileTstamp =
                  new java.text.SimpleDateFormat("yyyyMMdd'T'HHmmss_SSS'Z'Z");
        String fileDest = "/tmp/unravel_hive_hook_"
                            + fileTstamp.format(new Date()) + ".log";
        stringToFile(msgFormat(msg), fileDest);
    }

    static public String msgFormat(String msg) {
        final Date now = new Date();
        final java.text.SimpleDateFormat sdfLogOutput =
          new java.text.SimpleDateFormat("yyyy'/'MM'/'dd'T'HH':'mm':'ss.SSS'Z'Z");
        StringBuilder sb = new StringBuilder(120);
        sb.append(sdfLogOutput.format(now));
        sb.append(" ");
        sb.append(msg);
        sb.append("\n");
        return sb.toString();
    }

    /**
     * Write contents of string to local file using default encoding.
     * @param contents
     * @param destinationPath
     * @return true if success.
     */
    static public  boolean stringToFile(String contents, String destinationPath) {
        final byte[] buff = contents.getBytes();
        BufferedOutputStream bos;
        try {
            bos = new BufferedOutputStream(new FileOutputStream(destinationPath));
        } catch (IOException e) {
            return false;
        }
        try {
            bos.write(buff);
            bos.flush();
            bos.close();
            bos = null;
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    /**
     * Simple text append for comment strings to get something like:
     * <pre>
     keys: f1, f2
     title: value1
     * </pre>
     * The commas and newline char are managed here. There is no final newline char.
     * @param b buffer to append.
     * @param title an identifier with trailing colon and space (or some kind of punctuation
     *              that does not occur in values).
     * @param value a value to append to a comma separated list that started with a title,
     *              nothing is done if value is null.
     */
    static public void textAppend(StringBuilder b, String title, String value) {
        if (value == null  ||  value.length() == 0) return;
        final StringBuilder titleColon = new StringBuilder(60);
        titleColon.append(title).append(": ");
        if (b.lastIndexOf(titleColon.toString()) >= 0) { // title already present
            b.append(", ").append(value);
        } else { // start a new title and append the value
            if (b.length() > 0) b.append("\n");
            b.append(titleColon.toString()).append(value);
        }
    }

    static public void textAppend(StringBuilder b, String title, int value) {
        textAppend(b, title, Integer.toString(value));
    }

    /** a Comparator for Dependency info from hooks.LineageInfo.
     * The benefit of this is deterministic result ordering for testability.
     * (This is snipped from org.apache.hadoop.hive.ql.hooks.PostExecutePrinter).
     */
    public static class DependencyKeyComp implements
          Comparator<Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency>> {

      @Override
      public int compare(Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency> o1,
          Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency> o2) {
        if (o1 == null && o2 == null) {
          return 0;
        }
        else if (o1 == null && o2 != null) {
          return -1;
        }
        else if (o1 != null && o2 == null) {
          return 1;
        }
        else {
          // Both are non null.
          // First compare the table names.
          int ret = o1.getKey().getDataContainer().getTable().getTableName()
            .compareTo(o2.getKey().getDataContainer().getTable().getTableName());

          if (ret != 0) {
            return ret;
          }

          // The table names match, so check on the partitions
          if (!o1.getKey().getDataContainer().isPartition() &&
              o2.getKey().getDataContainer().isPartition()) {
            return -1;
          }
          else if (o1.getKey().getDataContainer().isPartition() &&
              !o2.getKey().getDataContainer().isPartition()) {
            return 1;
          }

          if (o1.getKey().getDataContainer().isPartition() &&
              o2.getKey().getDataContainer().isPartition()) {
            // Both are partitioned tables.
            ret = o1.getKey().getDataContainer().getPartition().toString()
            .compareTo(o2.getKey().getDataContainer().getPartition().toString());

            if (ret != 0) {
              return ret;
            }
          }

          // The partitons are also the same so check the fieldschema
          return (o1.getKey().getFieldSchema().getName().compareTo(
              o2.getKey().getFieldSchema().getName()));
        }
      }
    }

}