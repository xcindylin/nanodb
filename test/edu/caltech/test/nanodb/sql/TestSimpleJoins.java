package edu.caltech.test.nanodb.sql;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
 * This class exercises the database with some simple <tt>JOIN</tt>
 * statements against a two tables, to see if simple inner join,
 * left outer join, and right outer join statements work properly.
 */
@Test
public class TestSimpleJoins extends SqlTestCase {

    public TestSimpleJoins() { super("setup_testSimpleJoins"); }

    /**
     * This test performs a simple <tt>INNER JOIN</tt> statement
     * without empty tables to see if the query produces the
     * expected results. Also tests for when a given row of one
     * table is joined with several rows of another table.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoinNoEmpty() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, "red", 1, "yellow"),
                new TupleLiteral(1, "red", 1, "orange"),
                new TupleLiteral(3, null, 3, null),
                new TupleLiteral(5, "orange", 5, "red"),
                new TupleLiteral(5, "yellow", 5, "red")
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_a INNER JOIN test_join_b" +
                        " ON test_join_a.a = test_join_b.a", true);
        assert checkOrderedResults(expected, result);
    }


    /**
     * This test performs a simple <tt>INNER JOIN</tt> statement
     * with the left table empty to see if the query produces the
     * expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoinLeftEmpty() throws Throwable {
        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_c INNER JOIN test_join_a" +
                        " ON test_join_c.a = test_join_a.a", true);
        assert result.getTuples().size() == 0;
    }


    /**
     * This test performs a simple <tt>INNER JOIN</tt> statement
     * with the right table empty to see if the query produces the
     * expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoinRightEmpty() throws Throwable {
        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_a INNER JOIN test_join_c" +
                        " ON test_join_a.a = test_join_c.a", true);
        assert result.getTuples().size() == 0;
    }


    /**
     * This test performs a simple <tt>INNER JOIN</tt> statement
     * with the right and left tables empty to see if the query
     * produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoinEmpty() throws Throwable {
        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_c INNER JOIN test_join_d" +
                        " ON test_join_c.a = test_join_d.a", true);
        assert result.getTuples().size() == 0;
    }


    /**
     * This test performs a simple <tt>LEFT OUTER JOIN</tt> statement
     * without empty tables to see if the query produces the
     * expected results. Also tests for when a given row of one
     * table is joined with several rows of another table.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testLeftOuterJoinNoEmpty() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, "red", 1, "yellow"),
                new TupleLiteral(1, "red", 1, "orange"),
                new TupleLiteral(3, null, 3, null),
                new TupleLiteral(4, "green", null, null),
                new TupleLiteral(5, "orange", 5, "red"),
                new TupleLiteral(5, "yellow", 5, "red")
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_a LEFT OUTER JOIN test_join_b" +
                        " ON test_join_a.a = test_join_b.a", true);
        assert checkOrderedResults(expected, result);
    }

    /**
     * This test performs a simple <tt>LEFT OUTER JOIN</tt> statement
     * with the left table empty to see if the query produces the
     * expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testLeftOuterJoinLeftEmpty() throws Throwable {
        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_c LEFT OUTER JOIN test_join_a" +
                        " ON test_join_c.a = test_join_a.a", true);
        assert result.getTuples().size() == 0;
    }


    /**
     * This test performs a simple <tt>LEFT OUTER JOIN</tt> statement
     * with the right table empty to see if the query produces the
     * expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testLeftOuterJoinRightEmpty() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, "red", null, null),
                new TupleLiteral(3, null, null, null),
                new TupleLiteral(4, "green", null, null),
                new TupleLiteral(5, "orange", null, null),
                new TupleLiteral(5, "yellow", null, null)
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_a LEFT OUTER JOIN test_join_c" +
                        " ON test_join_a.a = test_join_c.a", true);
        assert checkOrderedResults(expected, result);
    }


    /**
     * This test performs a simple <tt>LEFT OUTER JOIN</tt> statement
     * with the right and left tables empty to see if the query
     * produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testLeftOuterJoinEmpty() throws Throwable {
        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_c LEFT OUTER JOIN test_join_d" +
                        " ON test_join_c.a = test_join_d.a", true);
        assert result.getTuples().size() == 0;
    }


    /**
     * This test performs a simple <tt>RIGHT OUTER JOIN</tt> statement
     * without empty tables to see if the query produces the
     * expected results. Also tests for when a given row of one
     * table is joined with several rows of another table.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testRightOuterJoinNoEmpty() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, "yellow", 1, "red"),
                new TupleLiteral(1, "orange", 1, "red"),
                new TupleLiteral(2, "green", null, null),
                new TupleLiteral(3, null, 3, null),
                new TupleLiteral(5, "red", 5, "orange"),
                new TupleLiteral(5, "red", 5, "yellow")
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_a RIGHT OUTER JOIN test_join_b" +
                        " ON test_join_a.a = test_join_b.a", true);
        assert checkOrderedResults(expected, result);
    }

    /**
     * This test performs a simple <tt>RIGHT OUTER JOIN</tt> statement
     * with the left table empty to see if the query produces the
     * expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testRightOuterJoinLeftEmpty() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, "red", null, null),
                new TupleLiteral(3, null, null, null),
                new TupleLiteral(4, "green", null, null),
                new TupleLiteral(5, "orange", null, null),
                new TupleLiteral(5, "yellow", null, null)
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_c RIGHT OUTER JOIN test_join_a" +
                        " ON test_join_c.a = test_join_a.a", true);
        assert checkOrderedResults(expected, result);
    }


    /**
     * This test performs a simple <tt>RIGHT OUTER JOIN</tt> statement
     * with the right table empty to see if the query produces the
     * expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testRightOuterJoinRightEmpty() throws Throwable {
        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_a RIGHT OUTER JOIN test_join_c" +
                        " ON test_join_a.a = test_join_c.a", true);
        assert result.getTuples().size() == 0;
    }


    /**
     * This test performs a simple <tt>RIGHT OUTER JOIN</tt> statement
     * with the right and left tables empty to see if the query
     * produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testRightOuterJoinEmpty() throws Throwable {
        CommandResult result = server.doCommand(
                "SELECT * FROM test_join_c RIGHT OUTER JOIN test_join_d" +
                        " ON test_join_c.a = test_join_d.a", true);
        assert result.getTuples().size() == 0;
    }
}
