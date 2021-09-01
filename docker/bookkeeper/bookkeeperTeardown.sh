#!/bin/bash

set -ex

HOST=`hostname -s`
PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
PRAVEGA_CLUSTER_NAME=${PRAVEGA_CLUSTER_NAME:-"pravega-cluster"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}
BK_CLUSTER_ROOT_PATH=/${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME}
BK_zkServers=$(echo "${ZK_URL:-127.0.0.1:2181}" | sed -r 's/;/,/g')

if [[ $HOST =~ (.*)-([0-9]+)$ ]]; then
  NAME=${BASH_REMATCH[1]}
  ORD=${BASH_REMATCH[2]}
else
  echo Failed to parse name and ordinal of Pod
  exit 1
fi

REPLICAS=`zk-shell --run-once "get ${BK_CLUSTER_ROOT_PATH}/conf" ${BK_zkServers} | cut -d'=' -f 2`

if [[ -n "$REPLICAS" && "$REPLICAS" -le "$ORD" ]]; then
  /opt/bookkeeper/bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie
fi