#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

PORT0=${PORT0:-3181}
ZK_URL=${ZK_URL:-127.0.0.1:2181}
USE_MOUNT=${USE_MOUNT:-0}
PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}

BK_LEDGERS_PATH="/${PRAVEGA_PATH}/${BK_CLUSTER_NAME}/ledgers"
DL_NS_PATH="/pravega/segmentstore/containers"

if [ $USE_MOUNT -eq 0 ]; then
    BK_DIR="/bk"
else
    BK_DIR=$MESOS_SANDBOX
fi

echo "bookie service port0 is $PORT0 "
echo "ZK_URL is $ZK_URL"
echo "BK_LEDGERS_PATH is $BK_LEDGERS_PATH"
echo "BK_DIR is $BK_DIR"

cp /home/ubuntu/dl_all/conf/bookie.conf.template /home/ubuntu/dl_all/conf/bookie.conf

sed -i 's/3181/'$PORT0'/' /home/ubuntu/dl_all/conf/bookie.conf
sed -i "s/localhost:2181/${ZK_URL}/" /home/ubuntu/dl_all/conf/bookie.conf
sed -i 's|journalDirectory=/tmp/data/bk/journal|journalDirectory=/mnt/journal|' /home/ubuntu/dl_all/conf/bookie.conf
sed -i 's|ledgerDirectories=/tmp/data/bk/ledgers|ledgerDirectories=/mnt/ledgers|' /home/ubuntu/dl_all/conf/bookie.conf
sed -i 's|indexDirectories=/tmp/data/bk/ledgers|indexDirectories='${BK_DIR}'/index|' /home/ubuntu/dl_all/conf/bookie.conf
sed -i 's|zkLedgersRootPath=/messaging/bookkeeper/ledgers|zkLedgersRootPath='${BK_LEDGERS_PATH}'|' /home/ubuntu/dl_all/conf/bookie.conf

#Re-create all the needed metadata dir in zk is OK, if they exisited before.
/home/ubuntu/dl_all/bin/dlog zkshell $ZK_URL create /${PRAVEGA_PATH} ''
/home/ubuntu/dl_all/bin/dlog zkshell $ZK_URL create /${PRAVEGA_PATH}/${BK_CLUSTER_NAME} ''
/home/ubuntu/dl_all/bin/dlog zkshell $ZK_URL create ${BK_LEDGERS_PATH} ''

echo "Create dl namespace here: distributedlog://${ZK_URL}${DL_NS_PATH}"
/home/ubuntu/dl_all/bin/dlog admin bind -dlzr $ZK_URL -dlzw $ZK_URL -s $ZK_URL -bkzr $ZK_URL -l ${BK_LEDGERS_PATH} -i false -r true -c distributedlog://${ZK_URL}${DL_NS_PATH}

#Format bookie metadata in zookeeper, the command should be run only once, because this command will clear all the bookies metadata in zk.
retString=`/home/ubuntu/dl_all/bin/dlog zkshell $ZK_URL stat ${BK_LEDGERS_PATH}/available/readonly 2>&1`
echo $retString | grep "not exist"
if [ $? -eq 0 ]; then
    # create ephemeral zk node bkInitLock
    retString=`/home/ubuntu/dl_all/bin/dlog zkshell $ZK_URL create -e /${PRAVEGA_PATH}/${BK_CLUSTER_NAME}/bkInitLock 2>&1`
    echo $retString | grep "Created"
    if [ $? -eq 0 ]; then 
        # bkInitLock created success, this is the first bookie creating
        echo "Bookkeeper metadata not be formated before, do the format."
        BOOKIE_CONF=/home/ubuntu/dl_all/conf/bookie.conf /home/ubuntu/dl_all/bin/dlog bkshell metaformat -f -n
        /home/ubuntu/dl_all/bin/dlog zkshell $ZK_URL delete  /${PRAVEGA_PATH}/${BK_CLUSTER_NAME}/bkInitLock
    else 
        # Wait other bookie do the format
        a=0
        while [ $a -lt 10 ]
        do
            sleep 10
            (( a++ ))
            retString=`/home/ubuntu/dl_all/bin/dlog zkshell $ZK_URL stat ${BK_LEDGERS_PATH}/available/readonly 2>&1`
            echo $retString | grep "not exist"
            if [ $? -eq 0 ]; then
                echo "wait $a * 10 seconds, still not formated"
                continue
            else
                echo "wait $a * 10 seconds, bookkeeper formated"
                break       
            fi
            
            echo "Waited 100 seconds for bookkeeper metaformat, something wrong, please check" 
            exit
        done    
    fi
else
    echo "Bookkeeper metadata be formated before, no need format"
fi

echo "format the bookie"
# format bookie
echo "Y" | BOOKIE_CONF=/home/ubuntu/dl_all/conf/bookie.conf /home/ubuntu/dl_all/bin/dlog bkshell bookieformat

echo "start a new bookie"
# start bookie,
export SERVICE_PORT=$PORT0
nohup /home/ubuntu/dl_all/bin/dlog org.apache.bookkeeper.proto.BookieServer --conf /home/ubuntu/dl_all/conf/bookie.conf 0<&- &> /tmp/nohup.log &
sleep 5
