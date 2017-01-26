#!/bin/bash
ZK_URL=${ZK_URL:-127.0.0.1:2181}
sed -i 's/3181/'$bookiePort'/' /opt/bookkeeper/conf/bk_server.conf
sed -i "s/localhost:2181/${ZK_URL}/" /opt/bookkeeper/conf/bk_server.conf
sed -i 's/#allowLoopback=false/allowLoopback=true/' /opt/bookkeeper/conf/bk_server.conf
./bin/bookkeeper shell metaformat  -nonInteractive -force  
./bin/bookkeeper shell bookieformat -nonInteractive -force && ./bin/bookkeeper  bookie 


