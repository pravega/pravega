set -x

ssh 10.249.250.153 "naucli marathon group remove pravega"

for ip in 10.249.250.154 10.249.250.155 10.249.250.156 10.249.250.157 10.249.250.158 10.249.250.159 
do
#ssh $ip "mkdir /mnt/bkj /mnt/bki /mnt/bkl;
#	mount /dev/sdaa1 /mnt/bkj;
#	mount /dev/sdca1 /mnt/bki;
#	mount /dev/sdd1 /mnt/bkl;"
ssh $ip "rm -r /mnt/bki/* /mnt/bkj/* /mnt/bkl/*;df"
ssh $ip "docker ps| grep entry | cut -d' ' -f 1 - | xargs docker stop | xargs docker rm"
#ssh $ip "docker rmi arvindkandhare/pravega_host"
	
done

hdfs dfs -rm -r /Scope
cat << EOF | ~/distributedlog-service/bin/dlog zkshell 10.249.250.151
deleteall /cluster
deleteall /messaging
deleteall /streams
EOF
