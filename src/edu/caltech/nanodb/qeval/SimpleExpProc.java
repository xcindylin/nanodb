package edu.caltech.nanodb.qeval;

import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.functions.ScalarFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by david on 2/1/15.
 */
public class SimpleExpProc implements ExpressionProcessor{
    private Map<String, FunctionCall> aggregates;
    private Set<FunctionCall> seenFunctions;

    public SimpleExpProc() {
        this.aggregates = new HashMap<String, FunctionCall>();
        this.seenFunctions = new HashSet<FunctionCall>();
    }

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

                aggregates.put("#" + aggregates.size(), (FunctionCall) node);

            }
        }
    }

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


    public Map<String, FunctionCall> getAggregates() {
        return aggregates;
    }
}
