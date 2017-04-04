. /opt/pravega/pravega-release/scripts/setup_env.sh
until [ `/opt/dl_all/distributedlog-service/bin/dlog zkshell $ZK_URL stat /pravega/bookkeeper/ledgers/available | grep numChild | sed -e 's/numChildren =//g'` -ge $ENSEMBLE_SIZE ]
do 
 echo "Waiting for BK ensemble to be complete"
 sleep 50
done

/opt/pravega/pravega-release/scripts/start_pravega.sh
