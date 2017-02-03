set -x
#sshgc 10.249.250.154 "mount /dev/sdaa1  /mnt/journal" 
#sshgc 10.249.250.155 "mount /dev/sdaa1  /mnt/journal" 
#sshgc 10.249.250.156 "mount /dev/sdaa1  /mnt/journal" 
#sshgc 10.249.250.157 "mount /dev/sdaa1 /mnt/journal" 
#sshgc 10.249.250.158 "mount /dev/sdaa1 /mnt/journal" 
#sshgc 10.249.250.159 "mount /dev/sdaa1 /mnt/journal"

#for ip in 10.249.250.154 10.249.250.155 10.249.250.156 10.249.250.157 10.249.250.158
# do 
#echo $ip
#ssh $ip docker -H unix:///var/run/docker.sock run -e HOST=$ip -e PORT=3181 -e PORT_3181=3181 -e PORTS=3181 -e ZK=master.mesos:2181 -e PORT0=3181 -e LIBPROCESS_IP=$ip -v /mnt/bkj/:/bk/journal/ -v /mnt/bki/:/bk/index/ -v /mnt/bkl/:/bk/ledgers/ --net host -e DLOG_EXTRA_OPTS=-Xms512m -d  arvindkandhare/bookkeeper
#done

export PUBLIC_ZOOKEEPER_ADDRESSES=master.mesos
~/distributedlog-service/bin/dlog admin bind -dlzr $PUBLIC_ZOOKEEPER_ADDRESSES -dlzw $PUBLIC_ZOOKEEPER_ADDRESSES -s $PUBLIC_ZOOKEEPER_ADDRESSES -bkzr $PUBLIC_ZOOKEEPER_ADDRESSES        -l /messaging/bookkeeper/ledgers -i false -r true -c distributedlog://$PUBLIC_ZOOKEEPER_ADDRESSES/messaging/distributedlog/mynamespace

ssh 10.249.250.153 "naucli marathon group add PravegaGroup.json"
