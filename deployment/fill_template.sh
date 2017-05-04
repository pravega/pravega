#!/bin/bash
set -x
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
