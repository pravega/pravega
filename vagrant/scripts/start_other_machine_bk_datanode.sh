#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

#Starts bookie and datanode on non master nodes

/opt/pravega/pravega-release/scripts/start_datanode.sh
/opt/pravega/pravega-release/scripts/start_bookie.sh 
sleep 30

