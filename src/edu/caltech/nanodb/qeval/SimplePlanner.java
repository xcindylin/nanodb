package edu.caltech.nanodb.qeval;


import java.awt.print.PrinterJob;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.Null;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.plans.*;
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


    /**
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
        // TODO:  Implement!



        System.out.println("done");

        // For HW1, we have a very simple implementation that defers to
        // makeSimpleSelect() to handle simple SELECT queries with one table,
        // and an optional WHERE clause.

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                "Not yet implemented:  enclosing queries!");
        }

        System.out.println(selClause.toString() + "\n");

        FromClause fromClause = selClause.getFromClause();
        PlanNode planNode = handleFromClause(selClause, fromClause);

        List<SelectValue> values = selClause.getSelectValues();
        SimpleExpProc processor = new SimpleExpProc();
        for(SelectValue sv : values) {
            if (sv.isExpression()) {
//                System.out.println("Expressions: " + sv.toString());
                Expression e = sv.getExpression().traverse(processor);
                sv.setExpression(e);
//                System.out.println("Expressions: " + sv.toString());
            }
        }

        if (processor.getAggregates().size() > 0 || selClause.getGroupByExprs().size() > 0) {
            planNode = new HashedGroupAggregateNode(planNode,
                    selClause.getGroupByExprs(), processor.getAggregates());
            planNode.prepare();
        }

//        for (SelectValue sv: selClause.getSelectValues()) {
//            System.out.println(sv.getExpression().toString());
//        }
//
//        if (!(selClause.isTrivialProject())) {
//            planNode = new ProjectNode(planNode, selClause.getSelectValues());
//            planNode.prepare();
//        }


        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        if (orderByExprs != null && !orderByExprs.isEmpty()) {
            planNode = new SortNode(planNode, orderByExprs);
            planNode.prepare();
        }

        return planNode;
    }


    public PlanNode handleFromClause(SelectClause selClause,
                                     FromClause fromClause) throws IOException {

        PlanNode planNode = null;
        if (fromClause == null) {
            planNode = makeSimpleProject(selClause.getSelectValues());
        } else if (fromClause.isBaseTable()) {
            Expression predicate = null;
            if (selClause != null)
                predicate = selClause.getWhereExpr();
            planNode = makeSimpleSelect(fromClause.getTableName(), predicate, null);
        } else if (fromClause.isDerivedTable()) {
            planNode = makePlan(fromClause.getSelectClause(), null);
        } else if (fromClause.isJoinExpr()) {
            planNode = handleJoinExpr(fromClause);
        }

        if (fromClause.isRenamed() && planNode != null) {
            return new RenameNode(planNode, fromClause.getResultName());
        }
        else {
            return planNode;
        }

//        throw new UnsupportedOperationException("Clause not handled " +
//                selClause.toString());


    }

    public PlanNode handleJoinExpr(FromClause fromClause) throws IOException {
        FromClause leftClause = fromClause.getLeftChild();
        FromClause rightClause = fromClause.getRightChild();

        System.out.println(leftClause.toString());
        System.out.println(rightClause.toString());

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

    // Freestyle
    public ProjectNode makeSimpleProject(List<SelectValue> projectionSpec) throws IOException {
        ProjectNode projectNode = new ProjectNode(projectionSpec);
        System.out.println("Start");
        for(SelectValue i: projectionSpec) {

            System.out.println(i.toString());

        }
        System.out.println("Start");

        projectNode.prepare();
        return projectNode;

    }
}
