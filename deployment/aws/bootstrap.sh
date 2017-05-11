#!/bin/bash

# Prepare installer
cd ../../ && ./gradlew distTar && cd deployment/aws && mv ../../build/distributions/pravega-0.1.0-SNAPSHOT.tgz installer/data/
cp ../../config/config.properties installer/data/
cp ../../docker/bookkeeper/entrypoint.sh install-bk-temp.sh

# Modify bookkeeper launch script to run in background
sed '$ d' install-bk-temp.sh > install-bk.sh && rm install-bk-temp.sh
echo "export SERVICE_PORT=\$PORT0" >> install-bk.sh
echo "nohup /opt/bk_all/bookkeeper-server-4.4.0//bin/bookkeeper bookie --conf  /opt/bk_all/bookkeeper-server-4.4.0/conf/bk_server.conf 0<&- &> /tmp/nohup.log &" >> install-bk.sh
echo "sleep 5" >> install-bk.sh
mkdir -p installer/roles/install-bk/files && mv install-bk.sh installer/roles/install-bk/files/

# Fill in config templates
cp installer/hosts-template installer/hosts
public_ips=$1
emr_endpoint=$2
IFS=',' read -r -a public_ip_array <<< "$public_ips"
sed "s/ZKNODE/${public_ip_array[0]}/g;s/NAMENODE/$emr_endpoint/g;s/CONTROLLERNODE/${public_ip_array[0]}/g" installer/entry_point_template.yml > installer/entry_point.yml
count=${#public_ip_array[@]}
for (( i=0; i<$count; i++ ));
do
   sed "s/N$i/${public_ip_array[$i]}/g" installer/hosts > installer/hosts-temp 
   mv installer/hosts-temp installer/hosts
done
region=$3
if [ "$region" == "us-east-1" ]; then
   sed "s/HIGH_PERFORMANCE_BUTTON/false/g" installer/data/variable_template.yml > installer/data/variable.yml
else
   sed "s/HIGH_PERFORMANCE_BUTTON/true/g" installer/data/variable_template.yml > installer/data/variable.yml
fi
