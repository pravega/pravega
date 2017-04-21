#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

# Starts Pravega host and controller

echo "Starting Pravega"
. /opt/pravega/pravega-release/scripts/setup_env.sh
/opt/pravega/pravega-release/service/host/bin/host > /tmp/host-`hostname`.log &
/opt/pravega/pravega-release/controller/server/bin/server > /tmp/controller-`hostname`.log &
