export DL_HOME=/opt/pravega/pravega-release/dl/distributedlog-service/
SERVICE_PORT=3181 /opt/pravega/pravega-release/dl/distributedlog-service/bin/dlog-daemon.sh start bookie --conf ${DL_HOME}/distributedlog-service/conf/bookie.conf
