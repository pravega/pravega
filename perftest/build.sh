
set -x

./gradlew distTar distZip 
 docker build -f Dockerfile_Streaming -t arvindkandhare/pravega_host . 
 docker build -f Dockerfile_Controller -t arvindkandhare/pravega_controller . 

docker save -o ~/host.tgz arvindkandhare/pravega_host
docker save -o ~/controller.tgz arvindkandhare/pravega_controller

for ip in 10.249.250.154 10.249.250.155 10.249.250.156 10.249.250.157 10.249.250.158 10.249.250.159 
do
#docker ps -a | awk " { print \$1; }"| xargs docker rm
df
scp ~/host.tgz root@$ip:/root/
scp ~/controller.tgz root@$ip:/root/
ssh $ip "docker load -i host.tgz"
ssh $ip "docker load -i controller.tgz"

done
