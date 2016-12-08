#!/bin/bash
sed -i 's/'localhost:2181'/'$zkCluster'/'  /opt/controller/server/conf/application.conf
/opt/controller/server/bin/server
