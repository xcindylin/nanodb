package edu.caltech.nanodb.qeval;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.Null;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.plans.*;
import org.apache.commons.lang.ObjectUtils;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;

import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class generates execution plans for performing SQL queries.  The
 * primary responsibility is to generate plans for SQL <tt>SELECT</tt>
 * statements, but <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also
 * use this class to generate simple plans to identify the tuples to update
 * or delete.
 */
public class SimplePlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


    private StorageManager storageManager;


    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }


    /** Freestyle
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                "Not yet implemented:  enclosing queries!");
        }
        // Parse through the fromClause and handle all cases
        FromClause fromClause = selClause.getFromClause();
        PlanNode planNode = handleFromClause(selClause, fromClause);

        // Replace all Aggregates with references
        SimpleExpProc processor = new SimpleExpProc();
        handleAggregates(processor, selClause);

        // If there are any aggregates, we create a hashedGroupAggregateNode
        // computing the aggregate functions
        if (processor.getAggregates().size() > 0 || selClause.getGroupByExprs().size() > 0) {
            planNode = new HashedGroupAggregateNode(planNode,
                    selClause.getGroupByExprs(), processor.getAggregates());
            planNode.prepare();
        }

        // If we have a Having expression, we create a filternode with the
        // the having predicate
        if (selClause.getHavingExpr() != null) {
            planNode = new SimpleFilterNode(planNode, selClause.getHavingExpr());
            planNode.prepare();
        }

        // If the selectclause contains nontirvial values, we create a project
        // node for it
        if (!(selClause.isTrivialProject())) {
            planNode = new ProjectNode(planNode, selClause.getSelectValues());
            planNode.prepare();
        }

        // If we have order by expresssions, we sort by those expressions
        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        if (orderByExprs != null && !orderByExprs.isEmpty()) {
            planNode = new SortNode(planNode, orderByExprs);
            planNode.prepare();
        }

        return planNode;
    }

    /** Freestyle
     *
     * handleAggregates parses through the inputted SelectClause. For each
     * expression within the SelectClause, we call traverse, which calls
     * enter and leave in our SimpleExpProc. If the expression contains
     * an aggregate then we replace the aggregate with a placeholder
     * @param processor Our ExpressionProcessor that finds aggregates and
     *                  records their references
     * @param selClause User Inputted Select Clause to be parsed
     */
    public void handleAggregates(SimpleExpProc processor, SelectClause selClause) {
        // Check before size
        int prevSize = processor.getAggregates().size();

        // If there are any aggregates in where clause, throw error
        if (selClause.getWhereExpr() != null) {
            selClause.getWhereExpr().traverse(processor);
            if (processor.getAggregates().size() > prevSize)
                throw new IllegalArgumentException("Cannot have" +
                        " aggregates inside WHERE clause");
        }

        // If there are any aggregates in ON clause, throw error
        if (selClause.getFromClause() != null) {
            if (selClause.getFromClause().isJoinExpr()) {
                if (selClause.getFromClause().getOnExpression() != null) {
                    selClause.getFromClause().getOnExpression().traverse(processor);
                    if (processor.getAggregates().size() > prevSize)
                        throw new IllegalArgumentException("Cannot have" +
                                " aggregates inside ON clause");
                }
            }
        }

        // Parse through select expressions, replace aggregate expressions
        // with references
        List<SelectValue> values = selClause.getSelectValues();
        for(SelectValue sv : values) {
            if (sv.isExpression()) {
                Expression e = sv.getExpression().traverse(processor);
                sv.setExpression(e);
            }
        }

        // Parse through having expression, replace aggregate expression
        // with reference
        if (selClause.getHavingExpr() != null) {
            Expression e = selClause.getHavingExpr().traverse(processor);
            selClause.setHavingExpr(e);
        }

    }

    /** Freestyle
     * handleFromClause handles all the different cases of a fromClause:
     * 1. There is no fromClause
     * 2. It is just from one table
     * 3. THere is a subquery
     * 4. It is multiple tables joining together.
     * and returns a planNode
     * @param selClause SelectClause is the user inputted select clause that
     *                  we get the fromClause from
     * @param fromClause The fromClause that we parse through to
     * @return PlanNode that contains the result of the from clause cases
     * @throws IOException
     */
    public PlanNode handleFromClause(SelectClause selClause,
                                     FromClause fromClause) throws IOException {

        PlanNode planNode = null;
        // If there is no fromClause, then we just project whatever expression
        // is within our select clause
        if (fromClause == null) {
            planNode = makeSimpleProject(selClause.getSelectValues());
        }
        // If the fromClause only has a basetable, then we just do a simple
        // select, which consists of a filescan with a predicate
        else if (fromClause.isBaseTable()) {
            Expression predicate = null;
            if (selClause != null)
                predicate = selClause.getWhereExpr();
            planNode = makeSimpleSelect(fromClause.getTableName(),
                    predicate, null);
            // If the user specifies a new name for the table, we rename
            if (fromClause.isRenamed()) {
                planNode = new RenameNode(planNode, fromClause.getResultName());
            }
        }
        // If the fromClause carries a subquery, then we call makePlan for the
        // subquery
        else if (fromClause.isDerivedTable()) {
            planNode = makePlan(fromClause.getSelectClause(), null);

            // subqueries must have a renamed table
            if (fromClause.isRenamed())
                planNode = new RenameNode(planNode, fromClause.getResultName());
            else
                throw new IllegalArgumentException("Derived Tables from" +
                " subqueries need a name");
        }
        // If it is a join, we call our join handler.
        else if (fromClause.isJoinExpr()) {
            planNode = handleJoinExpr(fromClause);
        }

        return planNode;
    }

    /** Freestyle
     * handleJoinExpr handles each join case. It recurses down so that
     * there are only joining two tables. Then it calls NestedLoopJoinNode
     * to join the two tables and builds up the expression back up
     * @param fromClause fromClause retrieved from SelectClause inputted
     * @return planNode with the correct join
     * @throws IOException
     */
    public PlanNode handleJoinExpr(FromClause fromClause) throws IOException {
        FromClause leftClause = fromClause.getLeftChild();
        FromClause rightClause = fromClause.getRightChild();

        PlanNode leftPlan = handleFromClause(null, leftClause);
        PlanNode rightPlan = handleFromClause(null, rightClause);

        Expression joinExpr = fromClause.getPreparedJoinExpr();
        PlanNode joinNode;

        joinNode  = new NestedLoopsJoinNode(leftPlan, rightPlan,
                fromClause.getJoinType(), joinExpr);
        joinNode.prepare();


        return joinNode;

    }
    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
        List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();

        return selectNode;
    }

    /** Freestyle
     * makeSimpleProject Creates a projectNode with the given projectionSpec
     * @param projectionSpec All the columns to be projected
     * @return ProjectNode with the given projectionSpec
     * @throws IOException
     */
    public ProjectNode makeSimpleProject(List<SelectValue> projectionSpec)
            throws IOException {
        ProjectNode projectNode = new ProjectNode(projectionSpec);
        projectNode.prepare();
        return projectNode;

    }
}
