#!/bin/bash
grep -iq 'zk {' /opt/controller/server/conf/application.conf
if [ "$?" == "1" ]
then 
     echo "ZK config not found"
     exit
else
     sed -i 's/url = "localhost:2181"/url = "'$zkCluster'"/' /opt/controller/server/conf/application.conf
fi

/opt/controller/server/bin/server
