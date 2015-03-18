package edu.caltech.nanodb.qeval;


import java.io.IOException;
import java.util.*;

//import com.sun.javaws.exceptions.InvalidArgumentException;
import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.expressions.PredicateUtils;
import edu.caltech.nanodb.plans.*;
import edu.caltech.nanodb.relations.JoinType;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations
 * don't currently span multiple subqueries.
 */
public class CostBasedJoinPlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CostBasedJoinPlanner.class);


    private StorageManager storageManager;


    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in
     * the <tt>FROM</tt>-clause of the query.  However, the planner will
     * attempt to push conjuncts down the plan as far as possible, so even if
     * a leaf is a base table, the plan may be a bit more complex than just a
     * single file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not
         * be used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to
         *        this leaf, or it may be nonempty if some conjuncts apply
         *        solely to this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<PlanNode>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {
        PlanNode planNode;

        // Parse through the fromClause
        FromClause fromClause = selClause.getFromClause();

        // If there is no fromClause, then we just project whatever expression
        // is within our select clause
        if (fromClause == null) {
            planNode = makeSimpleProject(selClause.getSelectValues());
            planNode.prepare();
            return planNode;
        }


        // Extract all the conjuncts from the where and having clause
        HashSet<Expression> extraConjuncts = handleWhere(selClause);

        // Create optimal join plan using the from clause and the conjuncts
        JoinComponent optimalJoin = makeJoinPlan(fromClause, extraConjuncts);
        planNode = optimalJoin.joinPlan;

        // Compute the unused conjuncts and apply them after the join
        extraConjuncts.removeAll(optimalJoin.conjunctsUsed);

        if (extraConjuncts.size() > 0) {
            Expression allPred = PredicateUtils.makePredicate(extraConjuncts);
            planNode = PlanUtils.addPredicateToPlan(optimalJoin.joinPlan, allPred);
        }

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

        // If we have a top level having expression, we create a filternode with the
        // the having predicate
        if (selClause.getHavingExpr() != null) {
            planNode = new SimpleFilterNode(planNode, selClause.getHavingExpr());
            planNode.prepare();
        }

        // If the selectclause contains nontrivial values, we create a project
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

        // If we have a limit clause, then we call the LimitOffsetNode
        if (selClause.getLimit() > 0) {
            planNode = new LimitOffsetNode(planNode, selClause.getLimit(),
                    selClause.getOffset());
            planNode.prepare();
        }

        return planNode;

    }

    /**
     * handleWhereHaving takes in a select clause and extracts all the conjuncts
     * from its where clause
     *
     * @param selClause User Inputted Select Clause to be parsed
     * @return extraConjuncts Hashset of Expressions that contain all the
     * conjuncts from where clause.
     */
    private HashSet<Expression> handleWhere(SelectClause selClause) {
        HashSet<Expression> extraConjuncts = new HashSet<Expression>();
        Expression where = selClause.getWhereExpr();

        PredicateUtils.collectConjuncts(where, extraConjuncts);

        return extraConjuncts;
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

    /**
     * Given the top-level {@code FromClause} for a SELECT-FROM-WHERE block,
     * this helper generates an optimal join plan for the {@code FromClause}.
     *
     * @param fromClause the top-level {@code FromClause} of a
     *        SELECT-FROM-WHERE block.
     * @param extraConjuncts any extra conjuncts (e.g. from the WHERE clause,
     *        or HAVING clause)
     * @return a {@code JoinComponent} object that represents the optimal plan
     *         corresponding to the FROM-clause
     * @throws IOException if an IO error occurs during planning.
     */
    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts) throws IOException {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra conjuncts
        // into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<Expression>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<FromClause>();

        collectDetails(fromClause, conjuncts, leafFromClauses);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        if (extraConjuncts != null)
            conjuncts.addAll(extraConjuncts);

        // Make a read-only set of the input conjuncts, to avoid bugs due to
        // unintended side-effects.
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:  " +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.

        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts);

        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization
     * out of a <tt>FROM</tt> clause.
     *
     * collectDetails takes in a fromClause and a hashset of conjuncts and
     * a list of leafFromClauses. There is no return value because the method
     * fills in the conjuncts into the inputted conjuncts hashset and the
     * leaf from clauses into leafFromClauses.
     *
     * collectDetails parses through the inputted fromClause and if there is no
     * fromClause then there are no conjuncts or leaves. If the fromcluase is
     * a base table, derived table, or outer join then collectDetails puts
     * it into leafFromClauses. Otherwise, if the fromclause is a joinexpr,
     * we recursively call the function on the children. and extracting its
     * conjuncts.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {

        // If there is no fromClause, then we add no conjuncts or leaves
        if (fromClause == null) {
            return;
        }
        // If fromClause is a base table, derived table, or outer join, we
        // add it as a leaf
        else if (fromClause.isBaseTable() || fromClause.isDerivedTable() ||
                fromClause.isOuterJoin()) {
            leafFromClauses.add(fromClause);
        }
        // If fromclause is a join expression, collect details on each of its
        // children and then extract the conjunct
        else if (fromClause.isJoinExpr()) {
            collectDetails(fromClause.getLeftChild(), conjuncts,
                    leafFromClauses);
            collectDetails(fromClause.getRightChild(), conjuncts,
                    leafFromClauses);
            Expression joinExpr = fromClause.getPreparedJoinExpr();
            PredicateUtils.collectConjuncts(joinExpr, conjuncts);
        }
        else {
            throw new IllegalArgumentException("Doesn't recognize from clause");
        }
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        ArrayList<JoinComponent> leafComponents = new ArrayList<JoinComponent>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<Expression>();

            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     *
     * makeLeafPlan takes in a leaf from clause and applies conjuncts as soon as
     * they are available. If it is a base table, we can apply the conjuncts
     * that are compatible with the given schema. Otherwise, if it a a derived
     * table, we can recursively call makeJoinPlan to get the planNode.
     * Otherwise, if it is an full outer join, equivalence rules say we cannot
     * apply conjuncts. However, if it is a left outer or a right outer, we
     * can apply conjuncts with the corresponding table.
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts additional conjuncts that can be applied when
     *        constructing the from-clause plan.
     *
     * @param leafConjuncts this is an output-parameter.  Any conjuncts applied
     *        in this plan from the <tt>conjuncts</tt> collection should be added
     *        to this out-param.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts)
        throws IOException {

        PlanNode planNode;

        // If fromClause is a base table, then we see which conjuncts apply
        // to the given base table and apply them with a file scan
        if (fromClause.isBaseTable()) {
            // Get table name
            String tableName = fromClause.getResultName();
            // Open the table.
            TableInfo tableInfo =
                    storageManager.getTableManager().openTable(tableName);
            planNode = new FileScanNode(tableInfo, null);

            // Prepare to set up the schema for planNode
            planNode.prepare();

            // Find the conjuncts that apply with the given schema
            PredicateUtils.findExprsUsingSchemas(conjuncts, false, leafConjuncts,
                    planNode.getSchema());

            // Add all the expressions together and apply
            Expression allPred = PredicateUtils.makePredicate(leafConjuncts);
            planNode = PlanUtils.addPredicateToPlan(planNode, allPred);

            // If table is renamed then rename it
            if (fromClause.isRenamed()) {
                planNode = new RenameNode(planNode, tableName);
            }
        }
        // If the from clause is a derived table then we handle it by
        // calling makePlan recursively
        else if (fromClause.isDerivedTable()) {
            String tableName = fromClause.getResultName();
            planNode = makePlan(fromClause.getSelectClause(), null);

            // If its a derived table, it should be renamed
            planNode = new RenameNode(planNode, tableName);
        }
        // Otherwise, the leaf is an outer join
        else {
            PlanNode leftChild;
            PlanNode rightChild;
            // If leaf is a full outer join, we can't apply any of the conjuncts
            if (fromClause.hasOuterJoinOnLeft() &&
                    fromClause.hasOuterJoinOnRight()) {
                // call make join plan for each child
                leftChild =
                        makeJoinPlan(fromClause.getLeftChild(), null).joinPlan;
                rightChild =
                        makeJoinPlan(fromClause.getRightChild(), null).joinPlan;
            }
            // If leaf is a left outer join, we see which conjuncts can be
            // applied to the left table
            else if (fromClause.hasOuterJoinOnLeft()) {
                String tableName = fromClause.getLeftChild().getResultName();
                // Open the table and prepare to find the schema of the table
                TableInfo tableInfo =
                        storageManager.getTableManager().openTable(tableName);
                planNode = new FileScanNode(tableInfo, null);
                planNode.prepare();

                // Find the conjuncts that apply for the left child and apply
                // them
                PredicateUtils.findExprsUsingSchemas(conjuncts, false,
                        leafConjuncts, planNode.getSchema());

                leftChild = makeJoinPlan(fromClause.getLeftChild(),
                                leafConjuncts).joinPlan;
                rightChild =
                        makeJoinPlan(fromClause.getRightChild(), null).joinPlan;
            }
            // If leaf is a right outer join, we see which conjuncts can be
            // applied to the right table
            else {
                String tableName = fromClause.getRightChild().getResultName();
                // Open the table.
                TableInfo tableInfo =
                        storageManager.getTableManager().openTable(tableName);
                planNode = new FileScanNode(tableInfo, null);
                planNode.prepare();

                // Find the conjuncts that apply for the right child and apply
                // them
                PredicateUtils.findExprsUsingSchemas(conjuncts, false,
                        leafConjuncts, planNode.getSchema());

                leftChild =
                        makeJoinPlan(fromClause.getLeftChild(), null).joinPlan;
                rightChild =
                        makeJoinPlan(fromClause.getRightChild(),
                                leafConjuncts).joinPlan;

            }
            // Join the left and the right
            planNode = new NestedLoopsJoinNode(leftChild, rightChild,
                    fromClause.getJoinType(), fromClause.getOnExpression());
        }

        planNode.prepare();
        return planNode;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans =
            new HashMap<HashSet<PlanNode>, JoinComponent>();

        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            logger.debug("Current set of join-plans has " + joinPlans.size() +
                " plans in it.");

            // This is the set of "next plans" we will generate.  Plans only
            // get stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<HashSet<PlanNode>, JoinComponent>();


            // For each plan in joinPlans
            for (Map.Entry<HashSet<PlanNode>, JoinComponent> entry:
                    joinPlans.entrySet()) {

                // Extract all the n leaves that make up the optimal plan of
                // n leaves. and the optimal plan itself
                HashSet<PlanNode> leavesN = entry.getKey();
                JoinComponent planN = entry.getValue();

                // For all the leaf plans
                for (JoinComponent leaf: leafComponents) {

                    // If the optimal plan contains the leaf, gp to the
                    // next leaf
                    if (leavesN.contains(leaf.joinPlan)) {
                        continue;
                    }

                    // Get conjuncts used in leftChild
                    HashSet<Expression> currConjuncts =
                            new HashSet<Expression>(planN.conjunctsUsed);
                    // Get conjuncts used in leaf
                    HashSet<Expression> rightConjuncts =
                            new HashSet<Expression>(leaf.conjunctsUsed);
                    // Get all the conjuncts used in both (union)
                    currConjuncts.addAll(rightConjuncts);
                    // Get all conjuncts
                    HashSet<Expression> allConjuncts =
                            new HashSet<Expression>(conjuncts);
                    // Compute all conjuncts - conjuncts used = unused conjuncts
                    allConjuncts.removeAll(currConjuncts);

                    // Find out which conjuncts from the unused conjuncts
                    // can be applied to the given schema
                    HashSet<Expression> conjunctsToApply =
                            new HashSet<Expression>();
                    PredicateUtils.findExprsUsingSchemas(allConjuncts,
                            false, conjunctsToApply, planN.joinPlan.getSchema(),
                            leaf.joinPlan.getSchema());

                    // Combine all conjuncts and apply them
                    currConjuncts.addAll(conjunctsToApply);
                    Expression allPred =
                            PredicateUtils.makePredicate(conjunctsToApply);

                    // Create new planNode that joins planN and leaf
                    PlanNode planN1 = new NestedLoopsJoinNode(planN.joinPlan,
                            leaf.joinPlan, JoinType.INNER, allPred);

                    planN1.prepare();

                    // Get new plan's cost
                    PlanCost planN1Cost = planN1.getCost();

                    // Add the new leaf to the N leaves
                    HashSet<PlanNode> currLeaves =
                            new HashSet<PlanNode>(leavesN);
                    currLeaves.add(leaf.joinPlan);

                    // If the join plans contain the new N+1 leaves
                    if (nextJoinPlans.containsKey(currLeaves)) {
                        // Check if the new plan has a cost less than the
                        // current plan
                        JoinComponent currJoin = nextJoinPlans.get(currLeaves);
                        float currCost = currJoin.joinPlan.getCost().cpuCost;
                        if (planN1Cost.cpuCost < currCost) {
                            nextJoinPlans.put(currLeaves,
                                    new JoinComponent(planN1, currLeaves, currConjuncts));
                        }
                    }
                    // If the join plans do not contain the n+1 leaves, then
                    // add the new plan
                    else {
                        nextJoinPlans.put(currLeaves,
                                new JoinComponent(planN1, currLeaves, currConjuncts));
                    }
                }
            }

            // Now that we have generated all plans joining N leaves, time to
            // create all plans joining N + 1 leaves.
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
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

    @Override
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
