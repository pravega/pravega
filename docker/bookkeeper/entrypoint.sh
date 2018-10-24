#!/bin/bash
#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

set -eo pipefail

ZK_HOME=/opt/zookeeper
BK_HOME=/opt/bookkeeper

PORT0=${PORT0:-$bookiePort}
PORT0=${PORT0:-3181}
ZK_URL=${ZK_URL:-127.0.0.1:2181}
USE_MOUNT=${USE_MOUNT:-0}
PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
PRAVEGA_CLUSTER_NAME=${PRAVEGA_CLUSTER_NAME:-"pravega-cluster"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}
BK_AUTORECOVERY=${BK_AUTORECOVERY:-"false"}
BK_useHostNameAsBookieID=${BK_useHostNameAsBookieID:-"false"}

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

sed -i 's/3181/'$PORT0'/' ${BK_HOME}/conf/bk_server.conf
sed -i "s/localhost:2181/${ZK_URL}/" ${BK_HOME}/conf/bk_server.conf
sed -i 's|journalDirectory=/tmp/bk-txn|journalDirectory='${BK_DIR}'/journal|' ${BK_HOME}/conf/bk_server.conf
sed -i 's|ledgerDirectories=/tmp/bk-data|ledgerDirectories='${BK_DIR}'/ledgers|' ${BK_HOME}/conf/bk_server.conf
sed -i 's|indexDirectories=/tmp/data/bk/ledgers|indexDirectories='${BK_DIR}'/index|' ${BK_HOME}/conf/bk_server.conf
sed -i 's|# zkLedgersRootPath=/ledgers|zkLedgersRootPath='${BK_LEDGERS_PATH}'|' ${BK_HOME}/conf/bk_server.conf

sed -i '/autoRecoveryDaemonEnabled/d' ${BK_HOME}/conf/bk_server.conf
echo autoRecoveryDaemonEnabled=${BK_AUTORECOVERY} >> ${BK_HOME}/conf/bk_server.conf
echo useHostNameAsBookieID=${BK_useHostNameAsBookieID} >> ${BK_HOME}/conf/bk_server.conf

echo "
tlsProvider=OpenSSL
# key store
tlsKeyStoreType=JKS
tlsKeyStore=/var/private/tls/bookie.keystore.jks
tlsKeyStorePasswordPath=/var/private/tls/bookie.keystore.passwd
# trust store
tlsTrustStoreType=JKS
tlsTrustStore=/var/private/tls/bookie.truststore.jks
tlsTrustStorePasswordPath=/var/private/tls/bookie.truststore.passwd" >> ${BK_HOME}/conf/bk_server.conf

echo "wait for zookeeper"
until ${ZK_HOME}/bin/zkCli.sh -server $ZK_URL ls /; do sleep 2; done

echo "create the zk root"
# Silence exit codes with "|| :" as the commands can safely fail with a "Node already exists" error
${ZK_HOME}/bin/zkCli.sh -server $ZK_URL create /${PRAVEGA_PATH} || :
${ZK_HOME}/bin/zkCli.sh -server $ZK_URL create /${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME} || :
${ZK_HOME}/bin/zkCli.sh -server $ZK_URL create /${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME} || :

echo "format the bookie"
# Silence exit codes with "|| :" as the command can safely fail if another instance has already formatted the bookie
BOOKIE_CONF=${BK_HOME}/conf/bk_server.conf ${BK_HOME}/bin/bookkeeper shell metaformat -nonInteractive || :

echo "start a new bookie"
SERVICE_PORT=$PORT0 ${BK_HOME}/bin/bookkeeper bookie --conf ${BK_HOME}/conf/bk_server.conf
