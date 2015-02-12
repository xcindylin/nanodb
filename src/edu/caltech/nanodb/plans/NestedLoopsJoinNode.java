package edu.caltech.nanodb.plans;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.qeval.PlanCost;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This plan node implements a nested-loops join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopsJoinNode extends ThetaJoinNode {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SortMergeJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;

    private Tuple NULL_TUPLE;

    private boolean matched;

    private boolean start;

    private boolean rightEmpty;

    private boolean doneRight;
    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;

    public NestedLoopsJoinNode(PlanNode leftChild, PlanNode rightChild,
                               JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);
    }


    /**
     * Checks if the argument is a plan node tree with the same
     structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopsJoinNode) {
            NestedLoopsJoinNode other = (NestedLoopsJoinNode) obj;

            return predicate.equals(other.predicate) &&
                    leftChild.equals(other.leftChild) &&
                    rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the nested-loops plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoops[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopsJoinNode node = (NestedLoopsJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    @Override
    public void prepare() {
        // Need to prepare the left and right child-nodes before we can do
        // our own work.
        leftChild.prepare();
        rightChild.prepare();

        // Use the parent class' helper-function to prepare the schema.
        prepareSchemaStats();

        // Grab the left and right child's cost, then update the cost based on the number
        // kind of join

        PlanCost leftChildCost = leftChild.getCost();
        PlanCost rightChildCost = rightChild.getCost();
        if (leftChildCost != null && rightChildCost != null) {
            float numTuples = leftChildCost.numTuples;
            float tupleSize = leftChildCost.tupleSize;
            // If a semijoin, then we assume best case time where each left tuple
            // matches with a right tuple in constant time so cost is
            // just number of left tuples
            // Otherwise we have to go through each right tuple for each left tuple
            if (joinType != JoinType.SEMIJOIN) {
                numTuples *= rightChildCost.numTuples;
                // If doing a left outer or right outer and the right side is empty
                // then cost is just number of left tuples
                if (rightChildCost.numTuples == 0 && (joinType == JoinType.LEFT_OUTER ||
                    joinType == JoinType.RIGHT_OUTER || joinType == JoinType.FULL_OUTER ||
                    joinType == JoinType.ANTIJOIN)) {
                    numTuples = leftChildCost.numTuples;
                }
            } else {
                numTuples = leftChildCost.numTuples;
            }

            // Comparing a pair of tuples takes constant time, so cpu cost is number of tuples
            float cpuCost = numTuples;

            // If not doing semi and anti then add tuple size of right
            if (joinType != JoinType.ANTIJOIN && joinType != JoinType.SEMIJOIN) {
                tupleSize += rightChildCost.tupleSize;
            }

            // Simply sum blockIOs of both left and right
            long numBlockIOs = leftChildCost.numBlockIOs + rightChildCost.numBlockIOs;
            cost = new PlanCost(numTuples, cpuCost, tupleSize, numBlockIOs);
        }
        else {
            logger.info(
                    "Child's cost not available; not computing this node's cost.");
            cost = null;
        }
    }


    public void initialize() {
        if (joinType == JoinType.RIGHT_OUTER)
            super.swap();
        super.initialize();
        done = false;
        leftTuple = null;
        rightTuple = null;
        matched = false;
        NULL_TUPLE = new TupleLiteral(rightChild.getSchema().numColumns());
        start = true;
        rightEmpty = false;
        doneRight = false;

    }


    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     *
     * @throws IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {
        if (done)
            return null;
        while (getTuplesToJoin()) {
            if (canJoinTuples() || rightTuple.equals(NULL_TUPLE)) {
                matched = true;
                if (joinType == JoinType.RIGHT_OUTER) {
                    return joinTuples(rightTuple, leftTuple);
                }
                if (joinType == JoinType.SEMIJOIN) {
                    return leftTuple;
                }
                if (joinType == JoinType.ANTIJOIN && doneRight) {
                    doneRight = false;
                    return leftTuple;
                }
                return joinTuples(leftTuple, rightTuple);
            }
        }
        return null;

    }


    /**
     * This helper function implements the logic that sets {@link #leftTuple}
     * and {@link #rightTuple} based on the nested-loops logic.
     *
     * @return {@code true} if another pair of tuples was found to join, or
     *         {@code false} if no more pairs of tuples are available to join.
     */
    private boolean getTuplesToJoin() throws IOException {
        // If just starting then advance left
        if (leftTuple == null) {
            leftTuple = leftChild.getNextTuple();
        }
        // Handle empty left case
        if (leftTuple == null) {
            done = true;
            return false;
        }

        // If matched and doing semi, then advance left and reset right
        if (matched && joinType == JoinType.SEMIJOIN) {
            leftTuple = leftChild.getNextTuple();
            matched = false;
            if (leftTuple == null) {
                done = true;
                return false;
            }
            rightChild.initialize();
        }

        rightTuple = rightChild.getNextTuple();
        if (rightTuple == null) {
            doneRight = true;
            // If haven't matched and doing an outer join, set right to null
            if (!matched && (joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER)) {
                rightTuple = NULL_TUPLE;
                matched = false;
                return true;
            }
            // If haven't matched and doing antijoin, reset right
            if (!matched && (joinType == JoinType.ANTIJOIN)) {
                rightChild.initialize();
                rightTuple = NULL_TUPLE;
                return true;
            }
            // Reset right and advance left and check if done with left
            rightChild.initialize();
            rightTuple = rightChild.getNextTuple();
            if (rightTuple == null && (joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER)) {
                rightTuple = NULL_TUPLE;
            }
            if (rightTuple == null && (joinType == JoinType.INNER)) {
                done = true;
                return false;
            }
            leftTuple = leftChild.getNextTuple();
            matched = false;
            if (leftTuple == null) {
                done = true;
                return false;
            }

        }




        return true;
    }



    private boolean canJoinTuples() {
        // If the predicate was not set, we can always join them!
        if (predicate == null)
            return true;

        environment.clear();
        environment.addTuple(leftSchema, leftTuple);
        environment.addTuple(rightSchema, rightTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition() {
        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark() throws IllegalStateException {
        leftChild.resetToLastMark();
        rightChild.resetToLastMark();

        // TODO:  Prepare to reevaluate the join operation for the tuples.
        //        (Just haven't gotten around to implementing this.)
    }


    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
