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

PORT0=${PORT0:-$bookiePort}
PORT0=${PORT0:-3181}
ZK_URL=${ZK_URL:-127.0.0.1:2181}
USE_MOUNT=${USE_MOUNT:-0}
PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
PRAVEGA_CLUSTER_NAME=${PRAVEGA_CLUSTER_NAME:-"pravega-cluster"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}
BK_AUTORECOVERY=${BK_AUTORECOVERY:-"false"}

BK_LEDGERS_PATH="/${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME}/ledgers"

if [ $USE_MOUNT -eq 0 ]; then
    BK_DIR="/bk"
else
    BK_DIR=$MESOS_SANDBOX
fi

echo "bookie service port0 is $PORT0 "
echo "ZK_URL is $ZK_URL"
echo "BK_DIR is $BK_DIR"
echo "BK_LEDGERS_PATH is $BK_LEDGERS_PATH"

sed -i 's/3181/'$PORT0'/' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i "s/localhost:2181/${ZK_URL}/" /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|journalDirectory=/tmp/bk-txn|journalDirectory='${BK_DIR}'/journal|' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|ledgerDirectories=/tmp/bk-data|ledgerDirectories='${BK_DIR}'/ledgers|' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|indexDirectories=/tmp/data/bk/ledgers|indexDirectories='${BK_DIR}'/index|' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|# zkLedgersRootPath=/ledgers|zkLedgersRootPath='${BK_LEDGERS_PATH}'|' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf

sed -i '/autoRecoveryDaemonEnabled/d' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
echo autoRecoveryDaemonEnabled=${BK_AUTORECOVERY} >> /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
#sed -i '/journalAdaptiveGroupWrites/d' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
#echo journalAdaptiveGroupWrites=false >> /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf

echo "wait for zookeeper"
until /opt/zk/zookeeper-3.5.1-alpha/bin/zkCli.sh -server $ZK_URL ls /; do sleep 2; done

echo "create the zk root"
/opt/zk/zookeeper-3.5.1-alpha/bin/zkCli.sh -server $ZK_URL create /${PRAVEGA_PATH}
/opt/zk/zookeeper-3.5.1-alpha/bin/zkCli.sh -server $ZK_URL create /${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}
/opt/zk/zookeeper-3.5.1-alpha/bin/zkCli.sh -server $ZK_URL create /${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME}

echo "format the bookie"
# format bookie
BOOKIE_CONF=/opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf /opt/bk_all/bookkeeper-server-4.4.0/bin/bookkeeper shell metaformat -nonInteractive

echo "start a new bookie"
# start bookie,
export SERVICE_PORT=$PORT0
nohup /opt/bk_all/bookkeeper-server-4.4.0//bin/bookkeeper bookie --conf  /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf 0<&- &> /tmp/nohup.log &
sleep 5
