#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

# Starts a Pravega deployment for Vagrant. nohup is needed as vagrant deploys machine one by one
# and we can not deploy Pravega before the bookie ensemble is ready.

nohup /opt/pravega/pravega-release/scripts/start_pravega_wait_quorum.sh 0<&- &> /tmp/nohup_pravega.log &



