#!/usr/bin/env bash
#-----------------------------------------------------------------------------
#-- Test 2B:  Basic rollback and recovery.  Identical to Test 2A, but flush
#--           all data before crashing.
rm -rf datafiles
rm temp.txt
rm temp2.txt
echo "CREATE TABLE testwal (a INTEGER, b VARCHAR(30), c FLOAT);
BEGIN;
INSERT INTO testwal VALUES (1, 'abc', 1.2);
INSERT INTO testwal VALUES (2, 'defghi', -3.6);
SELECT * FROM testwal;  -- Should list both records
COMMIT;
SELECT * FROM testwal;  -- Should list both records

BEGIN;
INSERT INTO testwal VALUES (-1, 'zxywvu', 78.2);

SELECT * FROM testwal;  -- Should list all three records

ROLLBACK;

SELECT * FROM testwal;  -- Should only list original two records

FLUSH;
CRASH;" | ./nanodb | grep '^|' > temp.txt
# Verify selects list both records
echo "| A | B      | C    |
| 1 | abc    |  1.2 |
| 2 | defghi | -3.6 |
| A | B      | C    |
| 1 | abc    |  1.2 |
| 2 | defghi | -3.6 |
| A  | B      | C    |
|  1 | abc    |  1.2 |
|  2 | defghi | -3.6 |
| -1 | zxywvu | 78.2 |
| A | B      | C    |
| 1 | abc    |  1.2 |
| 2 | defghi | -3.6 |" | diff temp.txt - > temp2.txt
if [[ -s 'temp2.txt' ]] ; then
echo "Test 2.B.1 Failed"
else
echo "Test 2.B.1 Passed"
fi ;

rm temp.txt
rm temp2.txt
echo "SELECT * FROM testwal;
INSERT INTO testwal VALUES (4, 'hmm hmm', 261.32);
SELECT * FROM testwal;
FLUSH;
CRASH;" | ./nanodb | grep '^|' > temp.txt
# Verify selects list both, three, and three
echo "| A | B      | C    |
| 1 | abc    |  1.2 |
| 2 | defghi | -3.6 |
| A | B       | C      |
| 1 | abc     |    1.2 |
| 2 | defghi  |   -3.6 |
| 4 | hmm hmm | 261.32 |"| diff temp.txt - > temp2.txt
if [[ -s 'temp2.txt' ]] ; then
echo "Test 2.B.2 Failed"
else
echo "Test 2.B.2 Passed"
fi ;

rm temp.txt
rm temp2.txt
echo "SELECT * FROM testwal;" | ./nanodb | grep '^|' > temp.txt
echo "| A | B       | C      |
| 1 | abc     |    1.2 |
| 2 | defghi  |   -3.6 |
| 4 | hmm hmm | 261.32 |" | diff temp.txt - > temp2.txt
if [[ -s 'temp2.txt' ]] ; then
echo "Test 2.B.3 Failed"
else
echo "Test 2.B.3 Passed"
fi ;
