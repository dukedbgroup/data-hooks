/**
 * 
 */
package edu.duke.dbmsplus.datahooks.semantic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import edu.duke.dbmsplus.datahooks.querymetadata.Dataset;
import edu.duke.dbmsplus.datahooks.querymetadata.Filter;
import edu.duke.dbmsplus.datahooks.querymetadata.GroupingAttribute;
import edu.duke.dbmsplus.datahooks.querymetadata.JoinAttribute;
import edu.duke.dbmsplus.datahooks.querymetadata.Query;
import edu.duke.dbmsplus.datahooks.querymetadata.QueryMetadataWriter;
import edu.duke.dbmsplus.datahooks.querymetadata.Projection;

/**
 * Set this class as hive.semantic.analyzer.hook and as hive.exec.driver.run.hooks
 * FIXME: This class is too bulky, push functionality inside parser package. 
 * @author mkunjir
 *
 */
public class Hook implements HiveSemanticAnalyzerHook, HiveDriverRunHook {
	
	// stores a map of table references used in queries to tables e.g. ..from input as i.. would add <i, input>.
	// FIXME: doesn't keep a map of complex references involving subqueries. e.g. (select .. from .. where ..) as a
	private Map<String, String> tabRefToName = new HashMap<String, String>();
	private List<String> unRefTables = new ArrayList<String>();

	static QueryMetadataWriter WRITER = null;
	static AtomicLong COUNTER = null;
	static List<String> OPERATORS = new ArrayList<String>();	
	static {
		OPERATORS.add("=");
		OPERATORS.add("<=");
		OPERATORS.add(">=");
		OPERATORS.add("<");
		OPERATORS.add(">");
		OPERATORS.add("!=");
		OPERATORS.add("<>");
	}

	// flag to set when a column is found
	boolean found = false;
	// set following when a column is found
	String tabName = "", colName = "";
	// following two are to parse array attributes
	boolean arrayAttribute = false;
	int index = 0;
	// following are to parse UDFs, dim_lookup in particular
	boolean dim_lookup = false;
	String functionName = "";
	String dimTabName = "";
	// following two are to find subquery aliases
	boolean subqueryInProgress = false;
	String subqueryAlias = "";

	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook#postAnalyze(org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext, java.util.List)
	 */
	@Override
	public void postAnalyze(HiveSemanticAnalyzerHookContext hookContext,
			List<Task<? extends Serializable>> tasks) throws SemanticException {

	}
	
	private void parseTOKGROUP(ASTNode node) {
		extractColumn(node);
//		System.out.println("Grouping on: " + tabName + ":" + colName);
		if(found) {
			GroupingAttribute group = new GroupingAttribute();
			group.setQueryId(COUNTER.longValue());
			group.setColName(colName);
			group.setTabName(tabName);
			group.setArray(arrayAttribute);
			group.setArrayIndex(index);
			group.setDimLookup(dim_lookup);
			group.setDimTabName(dimTabName);
			WRITER.addGroupingAttribute(group);
			resetFlags();
			return;
		}
		if(node.getChildren() != null) {
			for(Node n: node.getChildren()) {
				parseTOKGROUP((ASTNode) n);
			}
		}
	}
	
	private void parseTOKWHERE(ASTNode node, String operator, String value) {
//		System.out.println(":where: " + node);
		extractColumn(node);
		if(found) {
			Filter filter = new Filter();
			filter.setQueryId(COUNTER.longValue());
			filter.setTabName(tabName);
			filter.setColName(colName);
			filter.setArray(arrayAttribute);
			filter.setArrayIndex(index);	//setting even if not an array
			filter.setDimLookup(dim_lookup);
			filter.setDimTabName(dimTabName);	//setting even if not a dim lookup
			filter.setOperator(operator);
			filter.setValue(value);
			arrayAttribute = false;
			dim_lookup = false;
			WRITER.addFilter(filter);
//			System.out.println("-Where clause- " + tabName + ":" + colName + operator + value);
//			if(arrayAttribute == true) {
//				System.out.println("The column is an array and selection is on index: " + index);
//				arrayAttribute = false;
//			}
//			if(dim_lookup == true) {
//				System.out.println("The clause is doing a dim_lookup in table: " + dimTabName);
//				dim_lookup = false;
//			}
			resetFlags();
			return;
		}
		
		if(node.getChildren() != null) {
			for(Node n: node.getChildren()) {
				parseTOKWHERE((ASTNode) n, operator, value);
			}
		}		
		
	}

	/**
	 * All the complications including array attributes, UDFs appear here 
	 * @param node
	 */
	private void extractColumn(ASTNode node) {
		if(".".equals(node.toString())) {
			String tabRef = node.getChild(0).getChild(0).toString();
			if(tabRefToName.containsKey(tabRef)) {
				tabName = tabRefToName.get(tabRef);
			} else {
				tabName = "null";
			}			
			colName = node.getChild(1).toString();
			found = true;
		} else if("[".equals(node.toString())) {
			arrayAttribute = true;
			try {
//				System.out.println(":index: " + node.getChild(1));
				index = Integer.parseInt(node.getChild(1).toString());
			} catch(Exception e) {
				index = -1;
			}
		} else if(HiveParser.TOK_TABLE_OR_COL == node.getToken().getType()) {
			if(!unRefTables.isEmpty()) {
				tabName = unRefTables.get(unRefTables.size() - 1); // hack: assuming that column belongs to the last unreferrenced table
			} else {
				tabName = "null";
			}
			colName = node.getChild(0).toString();
			found = true;
		} else if(HiveParser.TOK_FUNCTION == node.getToken().getType()) {
			functionName = node.getChild(0).toString();
			if("dim_lookup".equals(functionName)) {
				dim_lookup = true;
				dimTabName = node.getChild(1).toString();
			}
		}
	}
	
	//TODO: support IN, NOT IN conditions
	private void parseTOKWHERE(ASTNode node) {
		if(OPERATORS.contains(node.toString())) {
			parseTOKWHERE((ASTNode) node.getChild(0), node.toString(), 
					node.getChild(1).toString());
		} else if(HiveParser.TOK_FUNCTION == node.getToken().getType()) {
			ASTNode child = (ASTNode) node.getChild(0);
			if(HiveParser.TOK_ISNOTNULL == child.getType()) {
				parseTOKWHERE((ASTNode) node.getChild(1), "ISNOTNULL", "null");
			} else if(HiveParser.TOK_ISNULL == child.getType()) {
				parseTOKWHERE((ASTNode) node.getChild(1), "ISNULL", "null");
			}
		}
		else {
			if(node.getChildren() != null) {
				for(Node n: node.getChildren()) {
					parseTOKWHERE((ASTNode) n);
				}
			}		
		}
	}
	
	//FIXME: Assuming every join is equality join
	private void parseTOKJOIN(ASTNode node) {
//		System.out.println("processing join on: " + node);
		extractColumn(node);
		if(found) {
			JoinAttribute join = new JoinAttribute();
			join.setQueryId(COUNTER.longValue());
			join.setColName(colName);
			join.setTabName(tabName);
			join.setArray(arrayAttribute);
			join.setArrayIndex(index);
			join.setDimLookup(dim_lookup);
			join.setDimTabName(dimTabName);
			WRITER.addJoinAttribute(join);
//			System.out.println("Join attribute " + colName + " on table " + tabName);
			resetFlags();
			return;
		}
		if(node.getChildren() != null) {
			for(Node n: node.getChildren()) {
				parseTOKJOIN((ASTNode) n);
			}
		}		
	}

	/**
	 * Call this every time a column is found
	 */
	private void resetFlags() {
		found = false;
		arrayAttribute = false;
		dim_lookup = false;
		functionName = "";
	}
	
	//FIXME: The function just extracts column names in the tree rooted at given node 
	// without caring for any other functions or 'case' statements downstream 
	//FIXME: Nested functions are not handled well; we only store bottom level function in agg
	// e.g. CAST(dim_lookup('tactic_tier', ad_info[1], 'tier') AS INT) AS tier
	private void parseTOKSEL(ASTNode node, String agg) {
//		System.out.println(":selection: " + node);
		extractColumn(node);
		
		if(found) {
			Projection projection = new Projection();
			projection.setQueryId(COUNTER.longValue());
			projection.setTabName(tabName);
			projection.setColName(colName);
			//HACK: overloading agg with function name
			if(!"".equals(functionName)) {
				projection.setAgg(functionName);
			} else {
				projection.setAgg(agg);
			}
			projection.setArray(arrayAttribute);
			projection.setArrayIndex(index);	//setting even if not an array
			projection.setDimLookup(dim_lookup);
			projection.setDimTabName(dimTabName);	//setting even if not a dim lookup
			arrayAttribute = false;
			dim_lookup = false;
			WRITER.addProjection(projection);
			// need to reset flags before extracting new column
			resetFlags();
			return;
		}
		if(node.getChildren() != null) {
			for(Node n: node.getChildren()) {
				parseTOKSEL((ASTNode) n, agg);
			}
		}				
		
	}
	
	private void parseTOKSEL(ASTNode node) {
		Tree child = node.getChild(0);
		//FIXME: count(distinct ) is returned as only a 'count' operation
		if(HiveParser.TOK_FUNCTIONDI == child.getType() || 
				HiveParser.TOK_FUNCTIONSTAR == child.getType() || 
				HiveParser.TOK_FUNCTION == child.getType()) {
			String agg = child.getChild(0).toString();			
			parseTOKSEL((ASTNode) child, agg);
		} else {
			parseTOKSEL((ASTNode) child, "null");
		}
	}
	
	private void parseTOKFROM(ASTNode node) {
//		System.out.println("processing from on : " + node);
		boolean joinInProgress = false;
		switch(node.getToken().getType()) {
		case HiveParser.TOK_JOIN: 
		case HiveParser.TOK_LEFTOUTERJOIN:
		case HiveParser.TOK_RIGHTOUTERJOIN:
		case HiveParser.TOK_FULLOUTERJOIN:
		case HiveParser.TOK_LEFTSEMIJOIN: {
			//TODO: Mark the query as a 'join' query
			joinInProgress = true;
			break;
		}
		case HiveParser.TOK_SELEXPR: {
			parseTOKSEL(node);
			return;
		}
		case HiveParser.TOK_WHERE: {
			parseTOKWHERE((ASTNode) node.getChild(0));
			return;
		}
		case HiveParser.TOK_GROUPBY: {
			for(int i=0; i<node.getChildCount(); i++) {
				parseTOKGROUP((ASTNode) node.getChild(i));
			}
			return;
		}
		case HiveParser.TOK_TABREF: {
			String tabName = node.getChild(0).getChild(0).toString();
			Boolean isTabSample = false;
			int selBuckets = 0, totalBuckets = 0;
			int numChildren = node.getChildCount();
			if(numChildren > 1 && 
					HiveParser.TOK_TABLEBUCKETSAMPLE != node.getChild(numChildren-1).getType()) {
				// the last child is table reference
				String tabRef = node.getChild(numChildren-1).toString();
				tabRefToName.put(tabRef, tabName);
//				System.out.println("Found table ref: " + tabRef + " for " + tabName);
			} else {
				unRefTables.add(tabName);
//				System.out.println("Found table: " + tabName);
				if(subqueryInProgress) {
					//FIXME: following is assuming that subquery is accesssing a single table
					tabRefToName.put(subqueryAlias, tabName);
					subqueryInProgress = false;
				}
				if(numChildren > 1 && HiveParser.TOK_TABLEBUCKETSAMPLE == node.getChild(1).getType()) {
					isTabSample = true;
					Tree tabSampleNode = node.getChild(1);
					//FIXME: assuming that tablesample requests for buckets on bucketed column
					//e.g. a query like TABLESAMPLE(1 out of 64 on rand(colName)) would be parsed 
					//by ignoring 'on rand(colName)' part
					if(tabSampleNode.getChildCount() > 0) {
						selBuckets = Integer.parseInt(tabSampleNode.getChild(0).toString());
					}
					if(tabSampleNode.getChildCount() > 1) {
						totalBuckets = Integer.parseInt(node.getChild(1).getChild(1).toString());
					}
					if(tabSampleNode.getChildCount() > 2) {
						//TODO: parse column, find a way to use extractColumn
					}
				}
			}
			Dataset ds = new Dataset();
			ds.setQueryId(COUNTER.longValue());
			ds.setTabName(tabName);
			ds.setIsTabSample(isTabSample);
			ds.setSelBuckets(selBuckets);
			ds.setTotalBuckets(totalBuckets);
			WRITER.addDataset(ds);
			resetFlags();
			return;
		}
		case HiveParser.TOK_SUBQUERY: {
			subqueryInProgress = true;
			subqueryAlias = node.getChild(1).toString();
			break;
		}
		}
		if(node.getChildren() != null) {
			for(int i=0; i<node.getChildCount(); i++) {
				if(joinInProgress && i==2) {
					parseTOKJOIN((ASTNode) node.getChild(i));
				} else {
					parseTOKFROM((ASTNode) node.getChild(i));
				}
			}
		}
	}
	
	private void recursiveParse(ASTNode node) {
//		System.out.println("Parsing: " + node.getToken());
		switch(node.getToken().getType()) {
		case HiveParser.TOK_FROM: 
			parseTOKFROM((ASTNode) node.getChild(0));
			return;
		}
		if(HiveParser.TOK_SELEXPR == node.getToken().getType()) {
			parseTOKSEL(node);
			return;
		}
		if(HiveParser.TOK_WHERE == node.getToken().getType()) {
			parseTOKWHERE((ASTNode) node.getChild(0));
			return;
		}
		if(HiveParser.TOK_GROUPBY == node.getToken().getType()) {
			for(int i=0; i<node.getChildCount(); i++) {
				parseTOKGROUP((ASTNode) node.getChild(i));
			}
			return;
		}
		if(node.getChildren() != null) {
			for(Node n: node.getChildren()) {
				recursiveParse((ASTNode) n);
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook#preAnalyze(org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext, org.apache.hadoop.hive.ql.parse.ASTNode)
	 */
	@Override
	public synchronized ASTNode preAnalyze(HiveSemanticAnalyzerHookContext hookContext, ASTNode astNode)
			throws SemanticException {
		tabRefToName = new HashMap<String, String>();
		unRefTables = new ArrayList<String>();
		System.out.println("---parsing query #" + COUNTER + "---");
		recursiveParse(astNode);
		System.out.println("---done parsing query #" + COUNTER + "---");
		return astNode;
	}

	@Override
	public void postDriverRun(HiveDriverRunHookContext arg0) throws Exception {
		
	}

	@Override
	public synchronized void preDriverRun(HiveDriverRunHookContext runContext) throws Exception {
		if(WRITER == null) {
			// first execution
			try{
				WRITER = new QueryMetadataWriter();
			} catch(Exception e) {
				System.out.println("Could not establish connection to mysql");
				System.exit(0);
			}
			COUNTER = new AtomicLong(WRITER.fetchLastQueryId());
//			System.out.println("---Initializing counter to: " + COUNTER);
		}
		System.out.println("---adding query #" + COUNTER + "---");
		COUNTER.incrementAndGet();
		Query query = new Query();
		query.setQueryId(COUNTER.longValue());
		query.setQueryString(runContext.getCommand());
		WRITER.addQuery(query);
		System.out.println("---done adding query #" + COUNTER +"---");
	}

}

