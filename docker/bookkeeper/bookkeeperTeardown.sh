#!/bin/bash
#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Exit the script in case of an error
set -ex

HOST=`hostname -s`
PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
PRAVEGA_CLUSTER_NAME=${PRAVEGA_CLUSTER_NAME:-"pravega-cluster"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}
BK_CLUSTER_ROOT_PATH=/${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME}
BK_zkServers=$(echo "${ZK_URL:-127.0.0.1:2181}" | sed -r 's/;/,/g')

# Extract pod ordinal value from its hostname
if [[ $HOST =~ (.*)-([0-9]+)$ ]]; then
  ORD=${BASH_REMATCH[2]}
else
  echo "Failed to parse ordinal value of the pod"
  exit 1
fi

# ClusterSize is set by the bookkeeper operator
CLUSTERSIZE=`zk-shell --run-once "get ${BK_CLUSTER_ROOT_PATH}/conf" ${BK_zkServers} | cut -d'=' -f 2`

if [[ -n "$CLUSTERSIZE" && "$CLUSTERSIZE" -le "$ORD" ]]; then
  # If ClusterSize <= Ordinal number, this instance is being permanantly removed
  # Therefore formatting any pre-existent data and deleting all cookies
  # created by this bookie instance to avoid potential conflicts
  /opt/bookkeeper/bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie
fi