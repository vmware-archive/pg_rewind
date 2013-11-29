#!/bin/bash
#
# Tests using a local data directory as the source
#
TESTDIR=.

set -e

bash testbase.sh

pg_ctl -w -D $TESTDIR/data-standby stop -m fast

cp $TESTDIR/data-master/postgresql.conf master-postgresql.conf.tmp
./pg_rewind --source-pgdata=$TESTDIR/data-standby -D $TESTDIR/data-master
mv master-postgresql.conf.tmp $TESTDIR/data-master/postgresql.conf

pg_ctl -w -D $TESTDIR/data-master start

psql postgres -c "SELECT * from tbl1";

# should print:
#
#                d
# -----------------------------
#  in master
#  in master, before promotion
#  in standby, after promotion
# (3 rows)
