#!/usr/bin/env bash

#-----------------------------------------------------------------------------
#-- Test 1:  Basic commit guarantee.  Insert rows, commit, crash DB, make sure
#--          they're still there.
rm -rf datafiles
rm temp.txt
rm temp2.txt
echo "CREATE TABLE testwal (a INTEGER, b VARCHAR(30), c FLOAT);
BEGIN;
INSERT INTO testwal VALUES (1, 'abc', 1.2);
INSERT INTO testwal VALUES (2, 'defghi', -3.6);
SELECT * FROM testwal;
COMMIT;
SELECT * FROM testwal;
CRASH;" | ./nanodb | grep '^|' > temp.txt
# Verify selects list both records
echo "| A | B      | C    |
| 1 | abc    |  1.2 |
| 2 | defghi | -3.6 |
| A | B      | C    |
| 1 | abc    |  1.2 |
| 2 | defghi | -3.6 |" | diff temp.txt - > temp2.txt
if [[ -s 'temp2.txt' ]] ; then
echo "Test 1.1 Failed"
else
echo "Test 1.1 Passed"
fi ;

rm temp.txt
rm temp2.txt
echo "SELECT * FROM testwal;
BEGIN;
INSERT INTO testwal VALUES (3, 'jklmnopqrst', 5.5);
SELECT * FROM testwal;
COMMIT;
SELECT * FROM testwal;
CRASH;" | ./nanodb | grep '^|' > temp.txt
# Verify selects list both, three, and three
echo "| A | B      | C    |
| 1 | abc    |  1.2 |
| 2 | defghi | -3.6 |
| A | B           | C    |
| 1 | abc         |  1.2 |
| 2 | defghi      | -3.6 |
| 3 | jklmnopqrst |  5.5 |
| A | B           | C    |
| 1 | abc         |  1.2 |
| 2 | defghi      | -3.6 |
| 3 | jklmnopqrst |  5.5 |" | diff temp.txt - > temp2.txt
if [[ -s 'temp2.txt' ]] ; then
echo "Test 1.2 Failed"
else
echo "Test 1.2 Passed"
fi ;

rm temp.txt
rm temp2.txt
echo "SELECT * FROM testwal;" | ./nanodb | grep '^|' > temp.txt
echo "| A | B           | C    |
| 1 | abc         |  1.2 |
| 2 | defghi      | -3.6 |
| 3 | jklmnopqrst |  5.5 |" | diff temp.txt - > temp2.txt
if [[ -s 'temp2.txt' ]] ; then
echo "Test 1.3 Failed"
else
echo "Test 1.3 Passed"
fi ;
