#!/bin/bash
#
# pg_rewind.sh
#
# Test driver for pg_rewind. This test script initdb's and configures a
# cluster and creates a table with some data in it. Then, it makes a
# standby of it with pg_basebackup, and promotes the standby.
#
# The result is two clusters, so that the old "master" cluster can be
# resynchronized with pg_rewind to catch up with the new "standby" cluster.
# This test can be run with either a local data folder or a remote
# connection as source.
#
# Before running this script, the calling script should've included
# config_test.sh, and defined four functions to define the test case:
#
#  before_master   - runs after initializing the master, before starting it
#  before_standby  - runs after starting the master, before creating the
#                    standby
#  standby_following_master - runs after standby has been created and started
#  after_promotion - runs after standby has been promoted, but old master is
#                    still running
#  after_rewind    - runs after pg_rewind and after restarting the rewound
#                    old master
#
# In those functions, the test script can use $MASTER_PSQL and $STANDBY_PSQL
# to run psql against the master and standby servers, to cause the servers
# to diverge.

# Helper function to wait for an instance to catch up based on a status
# query. The status query should be written so as it returns true ('t')
# when matching the catchup query provided by the caller.  Note that
# this cannot output logs except when failing as this makes the results
# differ and the regression tests fail.
function poll_query_until
{
	node_port=$1
	query=$2

	MAX_ATTEMPTS=100
	attempts=1
	while [ $attempts -lt $MAX_ATTEMPTS ]; do
		# We don't want -a/--echo-all here as it falsifies results.
		status=$(psql --no-psqlrc -p $node_port -At -c "$query")
		if [ "$status" = "t" ]; then
			break
		fi
		sleep 1
		let attempts=attempts+1
	done

	# Nothing more can be done, so make sure that the test fails with
	# a proper diff.
	if [ $attempts == 100 ]; then
		echo "Maximum number of attempts reached, failing."
		exit 1
	fi
}

# Initialize master, data checksums are mandatory
rm -rf $TEST_MASTER
initdb -N -A trust --data-checksums -D $TEST_MASTER >>$log_path

# Custom parameters for master's postgresql.conf
cat >> $TEST_MASTER/postgresql.conf <<EOF
wal_level = hot_standby
max_wal_senders = 2
wal_keep_segments = 20
checkpoint_segments = 50
shared_buffers = 1MB
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

#### Now run the test-specific parts to initialize the master before setting
echo "Master initialized."
before_master

pg_ctl -w -D $TEST_MASTER start >>$log_path 2>&1

# up standby
echo "Master running."
before_standby

# Set up standby with necessary parameter
rm -rf $TEST_STANDBY

# Base backup is taken with xlog files included
pg_basebackup -D $TEST_STANDBY -p $PORT_MASTER -x >>$log_path 2>&1
echo "port = $PORT_STANDBY" >> $TEST_STANDBY/postgresql.conf

cat > $TEST_STANDBY/recovery.conf <<EOF
primary_conninfo='port=$PORT_MASTER application_name=$STANDBY_APPLICATION_NAME'
standby_mode=on
recovery_target_timeline='latest'
EOF

# Start standby
pg_ctl -w -D $TEST_STANDBY start >>$log_path 2>&1

#### Now run the test-specific parts to run after standby has been started
# up standby
echo "Standby initialized and running."
standby_following_master

# Make sure that the standby has caught up to the latest point of the
# primary before moving on.
CATCHUP_QUERY="SELECT pg_catalog.pg_current_xlog_location() = flush_location FROM pg_catalog.pg_stat_replication WHERE application_name='$STANDBY_APPLICATION_NAME';"
poll_query_until $PORT_MASTER "$CATCHUP_QUERY"

# Now promote slave and insert some new data on master, this will put
# the master out-of-sync with the standby.
pg_ctl -w -D $TEST_STANDBY promote >>$log_path 2>&1

# Make sure that the standby has finished recovery before moving on.
poll_query_until $PORT_STANDBY "SELECT NOT pg_catalog.pg_is_in_recovery()"

#### Now run the test-specific parts to run after promotion
echo "Standby promoted."
after_promotion

# Stop the master and be ready to perform the rewind
pg_ctl -w -D $TEST_MASTER stop -m fast >>$log_path 2>&1

# For a local test, source node need to be stopped as well.
if [ $TEST_SUITE == "local" ]; then
	pg_ctl -w -D $TEST_STANDBY stop -m fast >>$log_path 2>&1
fi

# At this point, the rewind processing is ready to run.
# We now have a very simple scenario with a few diverged WAL record.
# The real testing begins really now with a bifurcation of the possible
# scenarios that pg_rewind supports.

# Keep a temporary postgresql.conf for master node or it would be
# overwritten during the rewind.
cp $TEST_MASTER/postgresql.conf $TESTROOT/master-postgresql.conf.tmp

# Now run pg_rewind
echo "Running pg_rewind..."
echo "Running pg_rewind..." >> $log_path
if [ $TEST_SUITE == "local" ]; then
	# Do rewind using a local pgdata as source
	pg_rewind \
		--verbose \
		--source-pgdata=$TEST_STANDBY \
		--target-pgdata=$TEST_MASTER >>$log_path 2>&1
elif [ $TEST_SUITE == "remote" ]; then
	# Do rewind using a remote connection as source
	pg_rewind \
		--verbose \
		--source-server="port=$PORT_STANDBY dbname=postgres" \
		--target-pgdata=$TEST_MASTER >>$log_path 2>&1
else
	# Cannot come here normally
	echo "Incorrect test suite specified"
	exit 1
fi

# After rewind is done, restart the source node in local mode.
if [ $TEST_SUITE == "local" ]; then
	pg_ctl -w -D $TEST_STANDBY start >>$log_path 2>&1
fi

# Now move back postgresql.conf with old settings
mv $TESTROOT/master-postgresql.conf.tmp $TEST_MASTER/postgresql.conf

# Plug-in rewound node to the now-promoted standby node
cat > $TEST_MASTER/recovery.conf <<EOF
primary_conninfo='port=$PORT_STANDBY'
standby_mode=on
recovery_target_timeline='latest'
EOF

# Restart the master to check that rewind went correctly
pg_ctl -w -D $TEST_MASTER start >>$log_path 2>&1

#### Now run the test-specific parts to check the result
echo "Old master restarted after rewind."
after_rewind

# Stop remaining servers
pg_ctl stop -D $TEST_MASTER -m fast -w >>$log_path 2>&1
pg_ctl stop -D $TEST_STANDBY -m fast -w >>$log_path 2>&1
