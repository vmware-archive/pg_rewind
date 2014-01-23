#!/bin/bash
#
# pg_rewind.sh
#
# Test driver for pg_rewind. This test script initdb's and configures a
# cluster and creates a table with some data in it. Then, it makes a
# standby of it with pg_basebackup, and promotes the standby. Some more
# rows are inserted in both the old and new cluster.
#
# The result is two clusters, so that the old "master" cluster can be
# resynchronized with pg_rewind to catch up with the new "standby" cluster.
# This test can be run with either a local data folder or a remote
# connection as source.

# Exit on error
set -e

mkdir -p "regress_log"
log_path="regress_log/pg_rewind_log_"`date +%F-%s`
: ${MAKE=make}

# Guard against parallel make issues (see comments in pg_regress.c)
unset MAKEFLAGS
unset MAKELEVEL

# Check at least that the option given is suited
if [ "$1" = '--remote' ]; then
	echo "Running tests with libpq connection as source" >>$log_path 2>&1
	TEST_SUITE="remote"
elif [ "$1" = '--local' ]; then
	echo "Running tests with local data folder as source" >>$log_path 2>&1
	TEST_SUITE="local"
else
	echo "Option $1 is not valid"
	exit 1
fi

# Set listen_addresses desirably
testhost=`uname -s`
case $testhost in
	MINGW*) LISTEN_ADDRESSES="localhost" ;;
	*)      LISTEN_ADDRESSES="" ;;
esac

# Adjust these paths for your environment
TESTROOT=$PWD/tmp_check
TEST_MASTER=$TESTROOT/data_master
TEST_STANDBY=$TESTROOT/data_standby

# Create the root folder for test data
mkdir -p $TESTROOT

# Clear out any environment vars that might cause libpq to connect to
# the wrong postmaster (cf pg_regress.c)
#
# Some shells, such as NetBSD's, return non-zero from unset if the variable
# is already unset. Since we are operating under 'set -e', this causes the
# script to fail. To guard against this, set them all to an empty string first.
PGDATABASE="";        unset PGDATABASE
PGUSER="";            unset PGUSER
PGSERVICE="";         unset PGSERVICE
PGSSLMODE=""          unset PGSSLMODE
PGREQUIRESSL="";      unset PGREQUIRESSL
PGCONNECT_TIMEOUT=""; unset PGCONNECT_TIMEOUT
PGHOST=""             unset PGHOST
PGHOSTADDR="";        unset PGHOSTADDR

# Define non conflicting ports for both nodes, this could be a bit
# smarter with for example dynamic port recognition using psql but
# this will make it for now.
PG_VERSION_NUM=90401
PORT_MASTER=`expr $PG_VERSION_NUM % 16384 + 49152`
PORT_STANDBY=`expr $PORT_MASTER + 1`

# Initialize master, data checksums are mandatory
rm -rf $TEST_MASTER
initdb -D $TEST_MASTER >>$log_path 2>&1

# Custom parameters for master's postgresql.conf
cat >> $TEST_MASTER/postgresql.conf <<EOF
wal_level = hot_standby
max_wal_senders = 2
wal_keep_segments = 20
checkpoint_segments = 50
shared_buffers = 1MB
wal_log_hints = on
log_line_prefix = 'M  %m %p '
hot_standby = on
autovacuum = off
max_connections = 50
listen_addresses = '$LISTEN_ADDRESSES'
port = $PORT_MASTER
EOF

# Accept replication connections on master
cat >> $TEST_MASTER/pg_hba.conf <<EOF
local replication all trust
host replication all 127.0.0.1/32 trust
host replication all ::1/128 trust
EOF

pg_ctl -w -D $TEST_MASTER start >>$log_path 2>&1

# 1. Do some inserts in master
psql --no-psqlrc -p $PORT_MASTER -c "CREATE TABLE tbl1 (d text);" postgres >>$log_path 2>&1
psql --no-psqlrc -p $PORT_MASTER -c "INSERT INTO tbl1 VALUES ('in master');" postgres >>$log_path 2>&1
psql --no-psqlrc -p $PORT_MASTER -c "CHECKPOINT; " postgres >>$log_path 2>&1

# Set up standby with necessary parameter
rm -rf $TEST_STANDBY

# Base backup is taken with xlog files included
pg_basebackup -D $TEST_STANDBY -p $PORT_MASTER -x >>$log_path 2>&1
echo "port = $PORT_STANDBY" >> $TEST_STANDBY/postgresql.conf

cat > $TEST_STANDBY/recovery.conf <<EOF
primary_conninfo='port=$PORT_MASTER'
standby_mode=on
recovery_target_timeline='latest'
EOF

# Start standby
pg_ctl -w -D $TEST_STANDBY start >>$log_path 2>&1

# Insert additional data on master and be sure that the standby has
# caught up.
psql --no-psqlrc -p $PORT_MASTER \
	 -c "INSERT INTO tbl1 values ('in master, before promotion');" \
	 postgres >>$log_path 2>&1
sleep 1

# Now promote slave and insert some new data on master, this will put
# the master out-of-sync with the standby.
pg_ctl -w -D $TEST_STANDBY promote >>$log_path 2>&1
sleep 1
psql --no-psqlrc -p $PORT_MASTER \
	 -c "INSERT INTO tbl1 VALUES ('in master, after promotion');" \
	 postgres >>$log_path 2>&1

# And complete it with some data on the standby, now both node have
# completely different data.
psql --no-psqlrc -p $PORT_STANDBY \
	 -c "INSERT INTO tbl1 VALUES ('in standby, after promotion');" \
	 postgres >>$log_path 2>&1

# Stop the master and be ready to perform the rewind
pg_ctl -w -D $TEST_MASTER stop -m fast >>$log_path 2>&1

# At this point, the rewind processing is ready to run.
# We now have a very simple scenario with a few diverged WAL record.
# The real testing begins really now with a bifurcation of the possible
# scenarios that pg_rewind supports.
sleep 1

# Keep a temporary postgresql.conf for master node or it would be
# overwritten during the rewind.
cp $TEST_MASTER/postgresql.conf $TESTROOT/master-postgresql.conf.tmp

# Now launch the test suite
if [ $TEST_SUITE == "local" ]; then
	# Do rewind using a local pgdata as source
	./pg_rewind \
		--source-pgdata=$TEST_STANDBY \
		--target-pgdata=$TEST_MASTER >>$log_path 2>&1
elif [ $TEST_SUITE == "remote" ]; then
	# Do rewind using a remote connection as source
	./pg_rewind \
		--source-server="port=$PORT_STANDBY dbname=postgres" \
		--target-pgdata=$TEST_MASTER >>$log_path 2>&1
else
	# Cannot come here normally
	echo "Incorrect test suite specified"
	exit 1
fi

# Now move back postgresql.conf with old settings
mv $TESTROOT/master-postgresql.conf.tmp $TEST_MASTER/postgresql.conf

# Restart the master to check that rewind went correctly
pg_ctl -w -D $TEST_MASTER start >>$log_path 2>&1

# Compare results generated by querying master
psql --no-psqlrc -p $PORT_MASTER -c "SELECT * from tbl1" postgres

# Stop remaining servers
pg_ctl stop -D $TEST_MASTER -m fast -w >>$log_path 2>&1
pg_ctl stop -D $TEST_STANDBY -m fast -w >>$log_path 2>&1

# Remove test data
rm -rf $TEST_ROOT
