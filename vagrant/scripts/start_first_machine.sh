/opt/pravega/pravega-release/scripts/start_namenode.sh &
/opt/pravega/pravega-release/scripts/start_zk.sh &
/opt/pravega/pravega-release/scripts/start_other_machine.sh &
sleep 10
echo "Waiting for all the processes for stop"
wait
