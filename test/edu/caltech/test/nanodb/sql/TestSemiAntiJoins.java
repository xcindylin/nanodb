package edu.caltech.test.nanodb.sql;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
 * This class exercises the database with some simple <tt>JOIN</tt>
 * statements against a two tables, to see if simple semijoin
 * and antijoin statements work properly.
 */
@Test
public class TestSemiAntiJoins extends SqlTestCase {

    public TestSemiAntiJoins() { super("setup_testSemiAntiJoins"); }

    /**
     * This test performs a simple <tt>SEMI JOIN</tt> statement
     * without empty tables to see if the query produces the
     * expected results. Also tests for when a given row of one
     * table is joined with several rows of another table.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testSemiJoinNoEmpty() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, "red"),
                new TupleLiteral(3, null),
                new TupleLiteral(5, "orange")
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_a SEMI JOIN test_join_b" +
                        " ON test_join_a.a = test_join_b.a", true);
        assert checkOrderedResults(expected, result);
    }


    /**
     * This test performs a simple <tt>SEMI JOIN</tt> statement
     * with the left table empty to see if the query produces the
     * expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testSemiJoinLeftEmpty() throws Throwable {
        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_c SEMI JOIN test_join_a" +
                        " ON test_join_c.a = test_join_a.a", true);
        assert result.getTuples().size() == 0;
    }


    /**
     * This test performs a simple <tt>SEMI JOIN</tt> statement
     * with the right table empty to see if the query produces the
     * expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testSemiJoinRightEmpty() throws Throwable {
        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_a SEMI JOIN test_join_c" +
                        " ON test_join_a.a = test_join_c.a", true);
        assert result.getTuples().size() == 0;
    }

    /**
     * This test performs a simple <tt>ANTI JOIN</tt> statement
     * without empty tables to see if the query produces the
     * expected results. Also tests for when a given row of one
     * table is joined with several rows of another table.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testAntiJoinNoEmpty() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(4, "green")
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_a ANTI JOIN test_join_b" +
                        " ON test_join_a.a = test_join_b.a", true);
        assert checkOrderedResults(expected, result);
    }


    /**
     * This test performs a simple <tt>ANTI JOIN</tt> statement
     * with the left table empty to see if the query produces the
     * expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testAntiJoinLeftEmpty() throws Throwable {
        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_c ANTI JOIN test_join_a" +
                        " ON test_join_c.a = test_join_a.a", true);
        assert result.getTuples().size() == 0;
    }


    /**
     * This test performs a simple <tt>ANTI JOIN</tt> statement
     * with the right table empty to see if the query produces the
     * expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testAntiJoinRightEmpty() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, "red"),
                new TupleLiteral(3, null),
                new TupleLiteral(4, "green"),
                new TupleLiteral(5, "orange")
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_a ANTI JOIN test_join_c" +
                        " ON test_join_a.a = test_join_c.a", true);
        assert checkOrderedResults(expected, result);
    }
}
