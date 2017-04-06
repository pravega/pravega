#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

#Script to start the ZK

/opt/pravega/pravega-release/zk/zookeeper-3.5.1-alpha/bin/zkServer.sh start > /tmp/zklogs.txt
