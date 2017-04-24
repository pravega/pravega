#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

PORT0=${PORT0:-$bookiePort}
PORT0=${PORT0:-3181}
ZK_URL=${ZK_URL:-127.0.0.1:2181}
USE_MOUNT=${USE_MOUNT:-0}
PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}

BK_LEDGERS_PATH="/${PRAVEGA_PATH}/${BK_CLUSTER_NAME}/ledgers"

if [ $USE_MOUNT -eq 0 ]; then
    BK_DIR="/bk"
else
    BK_DIR=$MESOS_SANDBOX
fi

echo "bookie service port0 is $PORT0 "
echo "ZK_URL is $ZK_URL"
echo "BK_LEDGERS_PATH is $BK_LEDGERS_PATH"
echo "BK_DIR is $BK_DIR"

sed -i 's/3181/'$PORT0'/' /opt/dl_all/bookkeeper-server-4.4.0/conf/bookie.conf
sed -i "s/localhost:2181/${ZK_URL}/" /opt/dl_all/bookkeeper-server-4.4.0/conf/bookie.conf
sed -i 's|journalDirectory=/tmp/data/bk/journal|journalDirectory='${BK_DIR}'/journal|' /opt/dl_all/bookkeeper-server-4.4.0/conf/bookie.conf
sed -i 's|ledgerDirectories=/tmp/data/bk/ledgers|ledgerDirectories='${BK_DIR}'/ledgers|' /opt/dl_all/bookkeeper-server-4.4.0/conf/bookie.conf
sed -i 's|indexDirectories=/tmp/data/bk/ledgers|indexDirectories='${BK_DIR}'/index|' /opt/dl_all/bookkeeper-server-4.4.0/conf/bookie.conf
sed -i 's|zkLedgersRootPath=/messaging/bookkeeper/ledgers|zkLedgersRootPath='${BK_LEDGERS_PATH}'|' /opt/dl_all/bookkeeper-server-4.4.0/conf/bookie.conf

echo "format the bookie"
# format bookie
echo "Y" | BOOKIE_CONF=/opt/dl_all/distributedlog-service/conf/bookie.conf /opt/dl_all/bookkeeper-server-4.4.0/bin/bookkeeper bkshell bookieformat

echo "start a new bookie"
# start bookie,
SERVICE_PORT=$PORT0 /opt/dl_all/distributedlog-service/bin/dlog org.apache.bookkeeper.proto.BookieServer --conf /opt/dl_all/bookkeeper-server-4.4.0/conf/bookie.conf

