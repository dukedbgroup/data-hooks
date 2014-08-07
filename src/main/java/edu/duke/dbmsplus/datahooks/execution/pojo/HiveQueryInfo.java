package edu.duke.dbmsplus.datahooks.execution.pojo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HiveQueryInfo {

    private static Log log = LogFactory.getLog(HiveQueryInfo.class);

	private String queryId;

	private String queryString;

	// timing
	private long startTime = 0l;
	private long endTime = 0l;

	// from table full name to table info
	private Map<String, HiveTableInfo> fullTableNameToInfos;

	private List<HiveStageInfo> hiveStageInfos;

	private String queryRecommendation;
	private int numMRJobs;
    private int totalMapTasks;
    private int totalReduceTasks;
    private long totalMapSlotDuration;	
    private long totalReduceSlotDuration;	
    
    private String userName;
    private long totalDfsBytesRead;
    private long totalDfsBytesWritten;
    
	private String namedOutput;
	private Map<String, String> outputPartitions;
	private OutputType outputType = OutputType.FETCH;

	public static enum OutputType {TABLE, DIRECTORY, FETCH}

    //
    //  info captured by failure hook:
    //   if failRetCode is non-zero, then failure hook fired
    //
    /** failure ret code if non-zero */
    private int failRetCode; // non-zero if failure hook fired
    /** failed task ID, example: stage-1 */
    private String failTID;
    /** failed job ID, example: job_999999  */
    private String failJID;
    /** failure TaskKind (subclass name, kind of hive Task) */
    private String failTaskKind;
    /** failure exception stacktrace, optional, never occurs before hive 0.12 */
    private String[] failExStack;
    /** version of our hive hook */
    private String version;

    private String comments;

    /** preliminary lineage info */
    private String details;

    /** like 0.12.0 */
    private String hiveVersion;
    /** a username (single token) */
    private String hiveCompiledBy;
    /** in unix date format like: "Wed Jan 8 15:38:33 PST 2014"   */
    private String hiveCompileDate;
    /** big hex number from subversion, lowercase  */
    private String hiveRevision;      
    
    // used by the json serialization
    public HiveQueryInfo() {
	hiveStageInfos = new ArrayList<HiveStageInfo>();
	fullTableNameToInfos = new HashMap<String, HiveTableInfo>();
	queryRecommendation = null;
	numMRJobs = 0;
	failRetCode = 0;
	failTID = null;
	failJID = null;
	failTaskKind = null;
	failExStack = new String[0];
	totalMapTasks = 0;
	totalReduceTasks = 0;
	totalMapSlotDuration = 0l;	
	totalReduceSlotDuration = 0l;
	userName = null;
	totalDfsBytesRead = 0l;
	totalDfsBytesWritten = 0l;
	//annotation = null;
    }
    
    public HiveQueryInfo(String queryId, String queryString,
			 String queryRecommendation, int numMRJobs) {
	this.queryId = queryId;
	this.queryString = queryString;
	this.queryRecommendation = queryRecommendation;
	this.numMRJobs = numMRJobs;
	//this.annotation = null;
    }

	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}

	public int getNumMRJobs() {
		return numMRJobs;
	}

	public void setNumMRJobs(int numMRJobs) {
	    this.numMRJobs = numMRJobs;
	}
    
    public int getTotalMapTasks() {
	return totalMapTasks;
    }
    public void setTotalMapTasks(int totalMapTasks) {
	this.totalMapTasks = totalMapTasks;
    }
    
    public int getTotalReduceTasks() {
	return totalReduceTasks;
    }
    public void setTotalReduceTasks(int totalReduceTasks) {
	this.totalReduceTasks = totalReduceTasks;
    }
    
    public long getTotalMapSlotDuration() {
	return totalMapSlotDuration;
    }
    public void setTotalMapSlotDuration(long totalMapSlotDuration) {
	this.totalMapSlotDuration = totalMapSlotDuration;
    }

    public long getTotalReduceSlotDuration() {
	return totalReduceSlotDuration;
    }
    public void setTotalReduceSlotDuration(long totalReduceSlotDuration) {
	this.totalReduceSlotDuration = totalReduceSlotDuration;
    }
    
    public String getUserName() {
	return userName;
    }
    
    public void setUserName(String userName) { 
	this.userName = userName; 
    }
    
    public long getTotalDfsBytesRead() {
	return totalDfsBytesRead;
    }
    public void setTotalDfsBytesRead(long totalDfsBytesRead) { 
	this.totalDfsBytesRead = totalDfsBytesRead;
    }
    
    public long getTotalDfsBytesWritten() {
	return totalDfsBytesWritten;
    }
    public void setTotalDfsBytesWritten(long totalDfsBytesWritten) { 
	this.totalDfsBytesWritten = totalDfsBytesWritten;
    }

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public Map<String, HiveTableInfo> getFullTableNameToInfos() {
		return fullTableNameToInfos;
	}

	public void setFullTableNameToInfos(
			Map<String, HiveTableInfo> fullTableNameToInfos) {
		this.fullTableNameToInfos = fullTableNameToInfos;
	}

	public List<HiveStageInfo> getHiveStageInfos() {
		return hiveStageInfos;
	}

	public void setHiveStageInfos(List<HiveStageInfo> hiveStageInfos) {
		this.hiveStageInfos = hiveStageInfos;
	}

    public int getFailRetCode() {
        return failRetCode;
    }

    public void setFailRetCode(int failRetCode) {
        this.failRetCode = failRetCode;
    }

    public String getFailTID() {
        return failTID;
    }

    public void setFailTID(String failTID) {
        this.failTID = failTID;
    }

    public String getFailJID() {
        return failJID;
    }

    public void setFailJID(String failJID) {
        this.failJID = failJID;
    }

    public String getFailTaskKind() {
        return failTaskKind;
    }

    public void setFailTaskKind(String failTaskKind) {
        this.failTaskKind = failTaskKind;
    }

    public String[] getFailExStack() {
        return failExStack;
    }

    public void setFailExStack(String[] failExStack) {
        this.failExStack = failExStack;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String getHiveVersion() {
        return hiveVersion;
    }

    public void setHiveVersion(String hiveVersion) {
        this.hiveVersion = hiveVersion;
    }

    public String getHiveCompiledBy() {
        return hiveCompiledBy;
    }

    public void setHiveCompiledBy(String hiveCompiledBy) {
        this.hiveCompiledBy = hiveCompiledBy;
    }

    public String getHiveCompileDate() {
        return hiveCompileDate;
    }

    public void setHiveCompileDate(String hiveCompileDate) {
        this.hiveCompileDate = hiveCompileDate;
    }

    public String getHiveRevision() {
        return hiveRevision;
    }

    public void setHiveRevision(String hiveRevision) {
        this.hiveRevision = hiveRevision;
    }
    
    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    /**
	 * Below are utility methods.
	 */

	public String getQueryRecommendation() {
		return queryRecommendation;
	}

	public void setQueryRecommendation(String queryRecommendation) {
		this.queryRecommendation = queryRecommendation;
	}

	/**
	 * Find the HiveStageInfo with the stageId. Can be null if not found.
	 * 
	 * @param stageId
	 * @return
	 */
	public HiveStageInfo findHiveStageInfo(String stageId) {
		if (stageId == null) {
			return null;
		}
		for (HiveStageInfo stage : hiveStageInfos) {
			if (stageId.equals(stage.getStageId())){
				return stage;
			}
		}
		return null;
	}

	/**
	 * Find all stage info which contains a common join.
	 * 
	 * @return a list of all stage info which contains a common join
	 */
	public List<HiveStageInfo> findCommonJoinStageInfo() {
		List<HiveStageInfo> ret = new ArrayList<HiveStageInfo>();
		for (HiveStageInfo stage : hiveStageInfos) {
			// Find all operators
			for (HiveTaskInfo task : stage.getHiveTaskInfos()) {
				for (HiveOperatorInfo operator : task.getHiveOperatorInfos()) {
					if (HiveOperatorInfo.JOIN_TYPE.equals(operator.getOperatorType())){
						ret.add(stage);
					}
				}
			}
		}
		return ret;
	}

	public List<HiveStageInfo> findMapJoinStageInfo() {
		List<HiveStageInfo> ret = new ArrayList<HiveStageInfo>();
		for (HiveStageInfo stage : hiveStageInfos) {
			// Find all operators
			if (stage.hasMapJoin()) {
				ret.add(stage);
			}
		}
		return ret;
	}

	/**
	 * Find all complete MapReduce stages.
	 * 
	 * @return a list of all MapRed stage info
	 */
	public List<HiveStageInfo> findCompleteMapRedStageInfo() {
		List<HiveStageInfo> ret = new ArrayList<HiveStageInfo>();
		for (HiveStageInfo stage : hiveStageInfos) {
			if (stage.getHadoopJobId() != null) {
				ret.add(stage);
			}
		}
		return ret;
	}

	/**
	 * Return the object as a json String. It uses the default POJO/Json convension.
	 *
	 * @return the json. If errors happen, return null.
	 */
	public String toJson() {
	    // log.info("In HiveQueryInfo.toJson()");
	    ObjectMapper objectMapper = new ObjectMapper();
	    String jsonStr = null;
	    try {
		jsonStr = objectMapper.writeValueAsString(this);
	    } catch (JsonProcessingException e) {
		log.info("Exception in HiveQueryInfo.toJson()" + e);
		return null;
	    }

 // 
	    // As part of JSON serialization, we will be replacing 
	    // all single quotes (') in the serialized JSON strings 
	    // with a marker. The reason is 
	    // that we want to use ' and ' to enclose each JSON string
	    // before it is parsed in the Rails and Javascript side
	    //  (i.e., parseJson('json string goes here')). Notes:
	    // 
	    //  -- The reason we need all this nonsense in the first 
	    //     place is the following: if the JSON contains a field
	    //     with internal quotes (e.g., a SQL query of the 
	    //     form: insert overwrite table actor_action partition(dt="20130518", network_abbr="fs") select ... --> note the quotes in dt and network_abbr) 
	    //     then the JSON representation will escape the internal
	    //     quotes (for the above example: insert overwrite table actor_action partition(dt=\"20130518\", network_abbr=\"fs\") select ...). 
	    //     Now when the raw (html_safe) part of Rails is used 
	    //     as part of displaying this JSON using Javascript, 
	    //     Rails will add a backslash (\) to escape each backslash 
	    //     (\) that it sees. This in turn leads to \\" in the JSON.
	    //     (for the above example: insert overwrite table actor_action partition(dt=\\"20130518\\", network_abbr=\\"fs\\") select ...)
	    //     Essentially, \\" --> " because the second
	    //     \ has been escaped by the first \. 
	    //     Consequently, the JSON string gets closed by the 
	    //     premature "
	    // (for the above example, it will be like: "querystring": "insert overwrite table actor_action partition(dt=\\"20130518\\", network_abbr=\\"fs\\") select ..." --> the \\" before 20130518 will close then JSON string, and
	    //  the JSON string will become invalid, and Javascript will
	    //  throw an errror)
	    // 
	    //  -- My first attempt was to try to replace the \\"
	    //     (produced by raw / html_safe for a \" in the JSON)
	    //      with a \" on the Rails / Javascript side. That did
	    //      not work because, the JSON string has to be represented
	    //      in the HTML page (generated by the ERB) in some way 
	    //      after the raw / html_safe is applied. For that, 
	    //      you have to use double quotes or single quotes 
	    //      (i.e., "JSON string goes here" or 'JSON string goes here')
	    //      Double quotes don't work because of the \\" premature
	    //      closure. Single quotes don't work because of the 
	    //      issue listed a couple of steps below, again because
	    //      of premature closure. 
	    //     
	    //  -- For the record, I did try WITHOUT using the raw / html_safe
	    //     in the Rails ERB views when the JSON to display 
	    //     is added to the page. That does not work either because 
	    //     then we get HTML escapes like quot; and other stuff
	    //     in the JSON. That also causes the Javascript to fail. 
	    //     Some pointers in the following links (but their solution 
	    //     did not work as is because of the single quotes (') 
	    //     issue listed below -- the HiveQL query string I was dealing
	    //     with had single quotes like: ... WHERE action != 'TWITTER_MENTION' and  action != 'TWITTER_RETWEET' ...): 
	    //        -- https://www.ruby-forum.com/topic/2435325
	    //        -- http://stackoverflow.com/questions/7205902/how-to-safely-embed-json-with-script-in-html-document
	    //        -- http://jfire.io/blog/2012/04/30/how-to-securely-bootstrap-json-in-a-rails-view/
	    // 
	    //        -- As and aside, on the Rails ERB / Javascript side, 
	    //           I used the solution listed at:
	    // http://stackoverflow.com/questions/9535521/elegant-multiple-char-replace-in-javascript-jquery
	    // 
	    //  -- We cannot simply escape the ' like \' because the raw 
	    //     (html_safe) part of Rails will add a backslash (\) 
	    //     to escape each backslash (\) that it sees. This 
	    //     leads to \\' which makes Javascript think that 
	    //     the \ is escaped, and the ' will appear by itself. That, 
	    //     causes a premature closure of the 'json string goes here'
	    // 

	    /**
	       
	  ***** IT TURNS OUT THAT THE \\ was being added due to a bug 
	  ***** in the PostgreSQL JDBC driver used by JRuby!
	  ***** See: https://github.com/jruby/activerecord-jdbc-adapter/issues/247
	  ***** and: https://github.com/jruby/activerecord-jdbc-adapter/pull/250
	  ***** Fixed by upgrading the PostgreSQL JDBC driver to 1.3.0

	      if (jsonStr != null) {
	      // log.info("jsonStr before replace = " + jsonStr);
	      // To get a backslash in a Java regex, we have to use "\\\\"
	      // jsonStr = jsonStr.replaceAll("'","\\\\'"); // DOES NOT WORK
	      jsonStr = jsonStr.replaceAll("'","gQ5b1Xq");
	      // log.info("jsonStr after replace = " + jsonStr);
	      }
	    */

	    return jsonStr; 
	}
    
    public static HiveQueryInfo deserializeFromStrings(String jsonStr) {
	// See the serialization function for why we need this  
	//    replaceAll code 
	String serStr = jsonStr.replaceAll("gQ5b1Xq","'");

	HiveQueryInfo hiveQueryInfo = null; 
	ObjectMapper mapper = new ObjectMapper();
	// add this code so the older JSON representations will also work 
	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	try {
	    hiveQueryInfo = mapper.readValue(serStr, HiveQueryInfo.class);
	} catch (JsonParseException e) {
	    // TODO Auto-generated catch block
	    log.info("Unable to parse the HiveQueryInfo json + ", e);
	    log.info("jsonStr = " + jsonStr);
	    hiveQueryInfo = null;
	} catch (JsonMappingException e) {
	    // TODO Auto-generated catch block
	    log.info("Unable to parse the HiveQueryInfo json + ", e);
	    hiveQueryInfo = null;
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    log.info("Unable to parse the HiveQueryInfo json + ", e);
	    hiveQueryInfo = null;	    
	}
	return hiveQueryInfo;
    }
    
    public static HiveQueryInfo deserializeFromFile(File jsonFile) {

	// Note: Unlike, the case of deserializeFromStrings(...), 
	//  we do not need the "replaceAll("gQ5b1Xq","'")" because
	//  the JSON as stored in the file is what was collected 
	//  directly by the Hive Hook

	HiveQueryInfo hiveQueryInfo = null; 
	ObjectMapper mapper = new ObjectMapper();
	// add this code so the older JSON representations will also work 
	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	try {
	    hiveQueryInfo = mapper.readValue(jsonFile, HiveQueryInfo.class);
	} catch (JsonParseException e) {
	    // TODO Auto-generated catch block
	    log.info("Unable to parse the HiveQueryInfo json + ", e);
	    hiveQueryInfo = null;
	} catch (JsonMappingException e) {
	    // TODO Auto-generated catch block
	    log.info("Unable to parse the HiveQueryInfo json + ", e);
	    hiveQueryInfo = null;
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    log.info("Unable to parse the HiveQueryInfo json + ", e);
	    hiveQueryInfo = null;	    
	}
	return hiveQueryInfo;
  }

	public String getNamedOutput() {
		return namedOutput;
	}

	public void setNamedOutput(String namedOutput) {
		this.namedOutput = namedOutput;
	}

	public Map<String, String> getOutputPartitions() {
		return outputPartitions;
	}

	public void setOutputPartitions(Map<String, String> outputPartitions) {
		this.outputPartitions = outputPartitions;
	}

	public OutputType getOutputType() {
		return outputType;
	}

	public void setOutputType(OutputType outputType) {
		this.outputType = outputType;
	}

}