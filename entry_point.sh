#!/bin/bash
PORT0=${PORT0:-$bookiePort}
PORT0=${PORT0:-3181}
ZK_URL=${ZK_URL:-127.0.0.1:2181}

PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}

BK_LEDGERS_PATH="/${PRAVEGA_PATH}/${BK_CLUSTER_NAME}/ledgers"
DL_NS_PATH="/messaging/distributedlog/mynamespace"

echo "bookie service port0 is $PORT0 "
echo "ZK_URL is $ZK_URL"
echo "BK_LEDGERS_PATH is $BK_LEDGERS_PATH"

cp /opt/dl_all/distributedlog-service/conf/bookie.conf.template /opt/dl_all/distributedlog-service/conf/bookie.conf

sed -i 's/3181/'$PORT0'/' /opt/dl_all/distributedlog-service/conf/bookie.conf
sed -i "s/localhost:2181/${ZK_URL}/" /opt/dl_all/distributedlog-service/conf/bookie.conf
sed -i 's|journalDirectory=/tmp/data/bk/journal|journalDirectory=/bk/journal|' /opt/dl_all/distributedlog-service/conf/bookie.conf
sed -i 's|ledgerDirectories=/tmp/data/bk/ledgers|ledgerDirectories=/bk/ledgers|' /opt/dl_all/distributedlog-service/conf/bookie.conf
sed -i 's|indexDirectories=/tmp/data/bk/ledgers|indexDirectories=/bk/index|' /opt/dl_all/distributedlog-service/conf/bookie.conf
sed -i 's|zkLedgersRootPath=/messaging/bookkeeper/ledgers|zkLedgersRootPath='${BK_LEDGERS_PATH}'|' /opt/dl_all/distributedlog-service/conf/bookie.conf

#Re-create all the needed metadata dir in zk is OK, if they exisited before.
/opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL create /${PRAVEGA_PATH} ''
/opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL create /${PRAVEGA_PATH}/${BK_CLUSTER_NAME} ''
/opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL create ${BK_LEDGERS_PATH} ''

echo "Create dl namespace here: distributedlog://${ZK_URL}${DL_NS_PATH}"
/opt/dl_all/distributedlog-service/bin/dlog admin bind -dlzr $ZK_URL -dlzw $ZK_URL -s $ZK_URL -bkzr $ZK_URL -l ${BK_LEDGERS_PATH} -i false -r true -c distributedlog://${ZK_URL}${DL_NS_PATH}

#Format bookie metadata in zookeeper, the command should be run only once, because this command will clear all the bookies metadata in zk.
retString=`/opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL stat ${BK_LEDGERS_PATH}/available/readonly 2>&1`
echo $retString | grep "not exist"
if [ $? -eq 0 ]; then
    # create ephemeral zk node bkInitLock
    retString=`/opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL create -e /${PRAVEGA_PATH}/${BK_CLUSTER_NAME}/bkInitLock 2>&1`
    echo $retString | grep "Created"
    if [ $? -eq 0 ]; then 
        # bkInitLock created success, this is the first bookie creating
        echo "Bookkeeper metadata not be formated before, do the format."
        BOOKIE_CONF=/opt/dl_all/distributedlog-service/conf/bookie.conf /opt/dl_all/distributedlog-service/bin/dlog bkshell metaformat -f -n
        /opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL delete  /${PRAVEGA_PATH}/${BK_CLUSTER_NAME}/bkInitLock
    else 
        # Wait other bookie do the format
        a=0
        while [ $a -lt 10 ]
        do
            sleep 10
            (( a++ ))
            retString=`/opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL stat ${BK_LEDGERS_PATH}/available/readonly 2>&1`
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
echo "Y" | BOOKIE_CONF=/opt/dl_all/distributedlog-service/conf/bookie.conf /opt/dl_all/distributedlog-service/bin/dlog bkshell bookieformat

echo "start a new bookie"
# start bookie,
SERVICE_PORT=$PORT0 /opt/dl_all/distributedlog-service/bin/dlog org.apache.bookkeeper.proto.BookieServer --conf /opt/dl_all/distributedlog-service/conf/bookie.conf

