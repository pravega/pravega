#!/bin/bash
#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
aws_access_key=${1:-null}
aws_secret_key=${2:-null}
aws_region=${3:-null}
aws_key_name=${4:-aws-key-pair}
cred_path=${5:-null}
config_path=${6:-null}
pravega_org=${7:-pravega/pravega}
pravega_branch=${8:-master}
travis_commit=${9:-null}
cd aws/
sed -i 's/,/ /g' public_dns.txt
var=`cat public_dns.txt`
sudo chmod 400 $cred_path
for i in $var; do
        ssh -o StrictHostKeyChecking=no -i $cred_path root@$i "bash -s" -- < ./logCollectionScript.sh $aws_access_key $aws_secret_key $pravega_branch $travis_commit
done

TF_LOG=INFO terraform destroy -force -var aws_access_key=$aws_access_key \
 -var aws_secret_key=$aws_secret_key \
  -var aws_region=$aws_region  \
  -var aws_key_name=$aws_key_name \
-var cred_path=$cred_path \
 -var config_path=$config_path \
  -var pravega_org=$pravega_org  \
  -var pravega_branch=$pravega_branch
