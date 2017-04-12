#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

#Add coomon overridable configs for the whole deployment here.

export ENSEMBLE_SIZE=2
export ZK_URL=controlnode:2181
export PRAVEGA_PATH="pravega"
export BK_CLUSTER_NAME="bookkeeper"
export BK_LEDGERS_PATH="/${PRAVEGA_PATH}/${BK_CLUSTER_NAME}/ledgers"
export DL_NS_PATH="/pravega/segmentstore/containers"

export JAVA_OPTS="$JAVA_OPTS -Ddlog.hostname=controlnode -Dpravegaservice.zkURL=controlnode:2181 -DpravegaService.controllerUri=tcp://controlnode:9090 -Dhdfs.hdfsUrl=controlnode:9000 -Dpravegaservice.listeningIPAddress=`hostname` -DbkcEnsembleSize=$ENSEMBLE_SIZE"
