package edu.caltech.test.nanodb.sql;

import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
 * This class exercises the database with dates including
 * comparisons.
 **/
@Test
public class TestDate extends SqlTestCase {
    public TestDate() {
        super ("setup_testDate");
    }

    /**
     * This test checks that at least one value was successfully inserted into
     * the test table.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testTableNotEmpty() throws Throwable {
        testTableNotEmpty("test_date");
    }

    /**
     * This tests date comparison, to see if the query
     * produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testDateComparison() throws Throwable {
        CommandResult result;

        result = server.doCommand(
                "SELECT * FROM test_date WHERE a < \'2000-10-10\'", true);
        System.err.println("DATE RESULTS = " + result.getTuples());
        TupleLiteral[] expected1 = {
                new TupleLiteral("2000-05-10"),
                new TupleLiteral("2000-07-05")
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
                "SELECT * FROM test_date WHERE a < \'2000-06-05\'", true);
        System.err.println("DATE RESULTS = " + result.getTuples());
        TupleLiteral[] expected2 = {
                new TupleLiteral("2000-05-10")
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
                "SELECT * FROM test_date WHERE a > \'2000-06-29\'", true);
        System.err.println("DATE RESULTS = " + result.getTuples());
        TupleLiteral[] expected3 = {
                new TupleLiteral("2000-07-05")
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);

        result = server.doCommand(
                "SELECT * FROM test_date WHERE a >= \'2000-05-10\'", true);
        System.err.println("DATE RESULTS = " + result.getTuples());
        TupleLiteral[] expected4 = {
                new TupleLiteral("2000-05-10"),
                new TupleLiteral("2000-07-05")
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);
    }
}
