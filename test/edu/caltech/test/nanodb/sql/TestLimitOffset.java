package edu.caltech.test.nanodb.sql;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import org.testng.annotations.Test;

/**
* This class exercises the database with both grouping and aggregation
* statements, to see if both kinds of expressions can work properly with each
* other.
**/
@Test
public class TestLimitOffset extends SqlTestCase {
    public TestLimitOffset() {
        super ("setup_testLimitOffset");
    }
    
    /**
    * This test checks that at least one value was successfully inserted into
    * the test table.
    *
    * @throws Exception if any query parsing or execution issues occur.
    */
    public void testTableNotEmpty() throws Throwable {
        testTableNotEmpty("test_limitoffset");
    }
    
    /**
    * This tests grouping and aggregation with having, to see if the query
    * produces the expected results.
    * 
    * @throws Exception if any query parsing or execution issues occur.
    */
    public void testLimitHaving() throws Throwable {
        CommandResult result;
        
        result = server.doCommand(
            "SELECT a FROM test_limitoffset LIMIT 2", true);

        TupleLiteral[] expected1 = {
            createTupleFromNum( 1 ),
            createTupleFromNum( 2 )
        };
        System.out.println(expected1[0].toString());
        assert checkSizeResults(expected1, result);
        assert checkOrderedResults(expected1, result);
        
        
        result = server.doCommand(
              "SELECT a FROM test_limitoffset LIMIT 20", true);
        TupleLiteral[] expected2 = {
            createTupleFromNum( 1 ),
            createTupleFromNum( 2 ),
            createTupleFromNum( 3),
            createTupleFromNum( 4),
            createTupleFromNum( 5),
            createTupleFromNum( 6),
            createTupleFromNum( 7),
            createTupleFromNum( 8),
            createTupleFromNum( 9),
            createTupleFromNum( 10)
        };
        assert checkSizeResults(expected2, result);
        assert checkOrderedResults(expected2, result);
        
        result = server.doCommand(
                "SELECT a FROM test_limitoffset LIMIT 3 OFFSET 3", true);
        TupleLiteral[] expected3 = {
            createTupleFromNum( 4),
            createTupleFromNum( 5),
            createTupleFromNum( 6)
        };
        assert checkSizeResults(expected3, result);
        assert checkOrderedResults(expected3, result);

        result = server.doCommand(
                "SELECT a FROM test_limitoffset LIMIT 3 OFFSET 9", true);
        TupleLiteral[] expected4 = {
            createTupleFromNum( 10)
        };
        assert checkSizeResults(expected4, result);
        assert checkOrderedResults(expected4, result);
    }
}
