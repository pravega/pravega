
sleep 200
echo "Starting Pravega"
. /opt/pravega/pravega-release/scripts/setup_env.sh
/opt/pravega/pravega-release/service/host/bin/host > /tmp/host-`hostname`.log &
/opt/pravega/pravega-release/controller/server/bin/server > /tmp/controller-`hostname`.log &
