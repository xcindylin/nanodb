package edu.caltech.nanodb.qeval;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.functions.ScalarFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Freestyle
 * SimpleExpProc stands for Simple Expression Processor.
 * This is used called to handle aggregate functions
 */
public class SimpleExpProc implements ExpressionProcessor{
    // Mapping between placeholder names and their aggregate function
    // counterparts
    private Map<String, FunctionCall> aggregates;

    public SimpleExpProc() {
        this.aggregates = new HashMap<String, FunctionCall>();
    }

    // Enter checks the expression is an aggregate function
    // if it is then we put it in our mapping, generating a placeholder name
    @Override
    public void enter(Expression node) {

        if (node instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) node;
            ScalarFunction f = call.getFunction();
            if(f instanceof AggregateFunction) {
                // Check if there is an aggregate function within another
                // aggregate function
                for(Expression arg: ((FunctionCall) node).getArguments()) {
                    if (arg instanceof FunctionCall) {
                        FunctionCall enclosedC = (FunctionCall) node;
                        ScalarFunction enclosedF = enclosedC.getFunction();
                        if (enclosedF instanceof AggregateFunction)
                            throw new IllegalArgumentException("Aggregate functions cannot be within other Aggregate Functions");
                    }

                }
                // Put in in our mapping
                aggregates.put("#" + aggregates.size(), (FunctionCall) node);
            }
        }
    }

    // Leave takes in an expression and if it is an aggregate function
    // return its placeholder name
    // otherwise return itself
    @Override
    public Expression leave(Expression node) {
        if (node instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) node;
            ScalarFunction f = call.getFunction();
            if(f instanceof AggregateFunction) {
                return new ColumnValue(new ColumnName("#" + (aggregates.size()-1)));
            }
        }

        return node;
    }


    /**
     * Get the Aggregates
     * @return The mapping between the placeholder variable names and the
     * actual functions
     */
    public Map<String, FunctionCall> getAggregates() {
        return aggregates;
    }
}
