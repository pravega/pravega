#!/bin/bash
sed -i 's/3181/'$bookiePort'/' /opt/bookkeeper/conf/bk_server.conf
sed -i 's/localhost:2181/127.0.0.1:2181/' /opt/bookkeeper/conf/bk_server.conf
sed -i 's/#allowLoopback=false/allowLoopback=true/' /opt/bookkeeper/conf/bk_server.conf
./bin/bookkeeper shell metaformat  -nonInteractive -force  
./bin/bookkeeper shell bookieformat -nonInteractive -force && ./bin/bookkeeper  bookie 


