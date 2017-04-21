#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

# This is vagrant specific script which deploys namenode and ZK on the master node

. /opt/pravega/pravega-release/scripts/setup_env.sh
/opt/pravega/pravega-release/scripts/start_namenode.sh 
/opt/pravega/pravega-release/scripts/start_zk.sh 
