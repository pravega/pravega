#!/bin/bash
#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
aws_access_key=${1:-null}
aws_secret_key=${2:-null}
aws_region=${3:-null}
aws_key_name=${4:-aws-key-pair}
cred_path=${5:-null}
config_path=${6:-null}
pravega_org=${7:-pravega/pravega}
pravega_branch=${8:-master}
cd aws/
TF_LOG=INFO terraform init
TF_LOG=INFO terraform apply -auto-approve -var aws_access_key=$aws_access_key \
 -var aws_secret_key=$aws_secret_key \
  -var aws_region=$aws_region  \
  -var aws_key_name=$aws_key_name \
-var cred_path=$cred_path \
 -var config_path=$config_path \
  -var pravega_org=$pravega_org  \
  -var pravega_branch=$pravega_branch
touch public_dns.txt
master_public_dns=`terraform output master_public_dns`
echo $master_public_dns >> $config_path/public_dns.txt
slave_public_dns=`terraform output slave_public_dns`
echo $slave_public_dns >> $config_path/public_dns.txt
