#!/bin/bash
#
#
# This test script initdb's and configures a cluster and creates a table
# with some data in it. Then, it makes a standby of it with pg_basebackup,
# and promotes the standby. Some more rows are inserted in both the old
# and new cluster.
#
# The result is two clusters, so that the old "master" cluster can be
# resynchronized with pg_rewind to catch up with the new "standby" cluster.

# Adjust these paths for your environment
TESTDIR=.
#PATH=/home/heikki/pgsql.master/bin:$PATH

# exit on error
set -e

# Set up master
initdb -D $TESTDIR/data-master
echo "wal_level=hot_standby" >> $TESTDIR/data-master/postgresql.conf
echo "max_wal_senders=2" >> $TESTDIR/data-master/postgresql.conf
echo "wal_keep_segments=20" >> $TESTDIR/data-master/postgresql.conf
echo "checkpoint_segments=50" >> $TESTDIR/data-master/postgresql.conf
echo "shared_buffers=1MB" >> $TESTDIR/data-master/postgresql.conf
echo "log_line_prefix='M  %m %p '" >> $TESTDIR/data-master/postgresql.conf
echo "hot_standby=on" >> $TESTDIR/data-master/postgresql.conf
echo "autovacuum=off" >> $TESTDIR/data-master/postgresql.conf
echo "max_connections=50" >> $TESTDIR/data-master/postgresql.conf

# Accept replication connections
echo "local   replication    all                                trust" >> $TESTDIR/data-master/pg_hba.conf
echo "host   replication     all             127.0.01/32                   trust" >> $TESTDIR/data-master/pg_hba.conf
echo "host   replication     all             ::1/128                   trust" >> $TESTDIR/data-master/pg_hba.conf

pg_ctl -w -D $TESTDIR/data-master start

# 1. Do some inserts in master1
psql -c "create table tbl1(d text)" postgres
psql -c "insert into tbl1 values ('in master'); " postgres
psql -c "checkpoint; " postgres

# Set up standby
pg_basebackup -D $TESTDIR/data-standby -x

sed -i "s/log_line_prefix=.*/log_line_prefix='S %m %p '/g" $TESTDIR/data-standby/postgresql.conf
echo "port=5433" >> $TESTDIR/data-standby/postgresql.conf

cat > $TESTDIR/data-standby/recovery.conf <<EOF
primary_conninfo=''
standby_mode=on
recovery_target_timeline='latest'
EOF

pg_ctl -w -D $TESTDIR/data-standby start

psql -c "insert into tbl1 values ('in master, before promotion');" postgres

sleep 1
pg_ctl -w -D $TESTDIR/data-standby promote

psql -c "insert into tbl1 values ('in master, after promotion');" postgres

# We now have a very simple scenario with a few diverged WAL record. Now for
# some complications.

echo "Basic scenario has now been set up. You can do more stuff on the master."
echo "When ready to rewind, press enter"
read FOO

pg_ctl -w -D $TESTDIR/data-master stop -m fast

sleep 5
PGPORT=5433 psql -c "insert into tbl1 values ('in standby, after promotion');" postgres
