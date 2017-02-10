#!/bin/bash
PORT0=${PORT0:-$bookiePort}
ZK_URL=${ZK_URL:-127.0.0.1:2181}

echo "bookie service port0 is $PORT0 "
echo "ZK_URL is $ZK_URL"

cp /opt/dl_all/distributedlog-service/conf/bookie.conf.template /opt/dl_all/distributedlog-service/conf/bookie.conf

sed -i 's/3181/'$PORT0'/' /opt/dl_all/distributedlog-service/conf/bookie.conf
sed -i "s/localhost:2181/${ZK_URL}/" /opt/dl_all/distributedlog-service/conf/bookie.conf
sed -i 's|journalDirectory=/tmp/data/bk/journal|journalDirectory=/bk/journal|' /opt/dl_all/distributedlog-service/conf/bookie.conf
sed -i 's|ledgerDirectories=/tmp/data/bk/ledgers|ledgerDirectories=/bk/ledgers|' /opt/dl_all/distributedlog-service/conf/bookie.conf
sed -i 's|indexDirectories=/tmp/data/bk/ledgers|indexDirectories=/bk/index|' /opt/dl_all/distributedlog-service/conf/bookie.conf

#Format bookie metadata in zookeeper, the command should be run only once, because this command will clear all the data in zk.
#Re-create all the needed metadata dir in zk is OK, if they exisited before.
FIRST_BOOKIE=0
retString=`/opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL stat /messaging/bookkeeper/ledgers/available/readonly 2>&1`
echo $retString | grep "not exist"
if [ $? -eq 0 ]; then
    echo "bookkeeper metadata not be formated before, do the format. zk dir: /messaging/bookkeeper/ledgers"

    /opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL create /messaging ''
    /opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL create /messaging/bookkeeper ''
    /opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL create /messaging/bookkeeper/ledgers ''
    BOOKIE_CONF=/opt/dl_all/distributedlog-service/conf/bookie.conf /opt/dl_all/distributedlog-service/bin/dlog bkshell metaformat -f -n
    FIRST_BOOKIE=1
else
    echo "bookkeeper metadata be formated before, no need format"
fi

# sleep a moment, if there is a race above, we wait all metaformat is done
sleep 5

echo "format the bookie"
# format bookie
echo "Y" | BOOKIE_CONF=/opt/dl_all/distributedlog-service/conf/bookie.conf /opt/dl_all/distributedlog-service/bin/dlog bkshell bookieformat

if [ $FIRST_BOOKIE -eq 1 ]; then
    echo "this is the first Bookie, could create dl namespace here"
    /opt/dl_all/distributedlog-service/bin/dlog admin bind -dlzr $ZK_URL -dlzw $ZK_URL -s $ZK_URL -bkzr $ZK_URL -l /messaging/bookkeeper/ledgers -i false -r true -c distributedlog://$ZK_URL/messaging/distributedlog/mynamespace
else
    echo "this is not the first Bookie"
fi

echo "start a new bookie"
# start bookie,
SERVICE_PORT=$PORT0 /opt/dl_all/distributedlog-service/bin/dlog org.apache.bookkeeper.proto.BookieServer --conf /opt/dl_all/distributedlog-service/conf/bookie.conf

