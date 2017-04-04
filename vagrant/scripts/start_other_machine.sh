/opt/pravega/pravega-release/scripts/start_datanode.sh &
/opt/pravega/pravega-release/scripts/start_bookie.sh &
sleep 20
/opt/pravega/pravega-release/scripts/start_pravega.sh &
