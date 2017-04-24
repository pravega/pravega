#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

PORT0=${PORT0:-$bookiePort}
PORT0=${PORT0:-3181}
ZK_URL=${ZK_URL:-127.0.0.1:2181}
USE_MOUNT=${USE_MOUNT:-0}

if [ $USE_MOUNT -eq 0 ]; then
    BK_DIR="/bk"
else
    BK_DIR=$MESOS_SANDBOX
fi

echo "bookie service port0 is $PORT0 "
echo "ZK_URL is $ZK_URL"
echo "BK_DIR is $BK_DIR"

sed -i 's/3181/'$PORT0'/' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i "s/localhost:2181/${ZK_URL}/" /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|journalDirectory=/tmp/bk-txn|journalDirectory='${BK_DIR}'/journal|' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|ledgerDirectories=/tmp/bk-data|ledgerDirectories='${BK_DIR}'/ledgers|' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|indexDirectories=/tmp/data/bk/ledgers|indexDirectories='${BK_DIR}'/index|' /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf

echo "format the bookie"
# format bookie
BOOKIE_CONF=/opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf /opt/bk_all/bookkeeper-server-4.4.0/bin/bookkeeper shell metaformat -nonInteractive
BOOKIE_CONF=/opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf /opt/bk_all/bookkeeper-server-4.4.0/bin/bookkeeper shell bookieformat

echo "start a new bookie"
# start bookie,
SERVICE_PORT=$PORT0 /opt/bk_all/bookkeeper-server-4.4.0//bin/bookkeeper bookie --conf  /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf
