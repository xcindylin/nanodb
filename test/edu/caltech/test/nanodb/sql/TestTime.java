package edu.caltech.test.nanodb.sql;

import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
 * This class exercises the database with times including
 * comparisons and arithmetic
 **/
@Test
public class TestTime extends SqlTestCase {
    public TestTime() {
        super ("setup_testTime");
    }

    /**
     * This test checks that at least one value was successfully inserted into
     * the test table.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testTableNotEmpty() throws Throwable {
        testTableNotEmpty("test_time");
    }

    /**
     * This tests TIME comparison, to see if the query
     * produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testTimeComparison() throws Throwable {
        CommandResult result;

        result = server.doCommand(
                "SELECT * FROM test_time WHERE a < \'14:32:40\'", true);
        System.err.println("TIME RESULTS = " + result.getTuples());
        TupleLiteral[] expected1 = {
                new TupleLiteral("07:20:35"),
                new TupleLiteral("14:31:40")
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
                "SELECT * FROM test_time WHERE a < \'12:20:35\'", true);
        System.err.println("TIME RESULTS = " + result.getTuples());
        TupleLiteral[] expected2 = {
                new TupleLiteral("07:20:35")
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
                "SELECT * FROM test_time WHERE a > \'12:20:35\'", true);
        System.err.println("TIME RESULTS = " + result.getTuples());
        TupleLiteral[] expected3 = {
                new TupleLiteral("14:31:40")
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);

        result = server.doCommand(
                "SELECT * FROM test_time WHERE a >= \'07:20:35\'", true);
        System.err.println("TIME RESULTS = " + result.getTuples());
        TupleLiteral[] expected4 = {
                new TupleLiteral("07:20:35"),
                new TupleLiteral("14:31:40")
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);
    }

    /**
     * This tests TIME arithmetic, to see if the query
     * produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testTimeArithmetic() throws Throwable {
        CommandResult result;

        result = server.doCommand(
                "SELECT a - \'5:50:20\' FROM test_time", true);
        System.err.println("TIME RESULTS = " + result.getTuples());
        TupleLiteral[] expected1 = {
                new TupleLiteral("01:30:15"),
                new TupleLiteral("08:41:20")
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
                "SELECT a + \'00:00:10\' FROM test_time", true);
        System.err.println("TIME RESULTS = " + result.getTuples());
        TupleLiteral[] expected2 = {
                new TupleLiteral("07:20:45"),
                new TupleLiteral("14:31:50")
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
                "SELECT a + a FROM test_time", true);
        System.err.println("TIME RESULTS = " + result.getTuples());
        TupleLiteral[] expected3 = {
                new TupleLiteral("14:41:10"),
                new TupleLiteral("05:03:20")
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);

    }
}
