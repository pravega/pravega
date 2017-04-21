#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

# Waits for a bookie ensemble and then deploys Pravega.

. /opt/pravega/pravega-release/scripts/setup_env.sh
CURR_ENSEMBLE=0

until [ $ENSEMBLE_SIZE -lt $CURR_ENSEMBLE ]
do 
 echo "Waiting for BK ensemble to be complete"
 sleep 50
CURR_ENSEMBLE=`/opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL stat /pravega/bookkeeper/ledgers/available | grep numChild | sed -e 's/numChildren =//g'`
echo "Ensemble is $CURR_ENSEMBLE expected to be $ENSEMBLE_SIZE"
done

/opt/pravega/pravega-release/scripts/start_pravega.sh
