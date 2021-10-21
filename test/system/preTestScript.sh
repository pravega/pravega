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

#The script is only for jarvis environments and jarvis client should be installed.
CLUSTER_NAME=${1:-null}
echo "Cluster Name is $CLUSTER_NAME"
MASTER=${2:-null}
echo "Master IP of jarvis cluster is $MASTER"
NUM_SLAVES=${3:-null}
DOCKER_VERSION=`docker version --format '{{.Server.APIVersion}}'`
echo "Docker API version is $DOCKER_VERSION"
DOCKER_API_MIN_VERSION=1.37
st=`echo "${DOCKER_VERSION} < ${DOCKER_API_MIN_VERSION}" | bc`
if [ 1 -eq $st ];
then
   exit
fi
if [ $CLUSTER_NAME != null ]; then
    jarvis save $CLUSTER_NAME
    if [ $MASTER != null ]; then
      #master
      jarvis ssh $CLUSTER_NAME "bash -s" -- < ./Change_Docker_Config_Master.sh $MASTER
      echo "Copying token to host machine"
      jarvis scp master-1:/home/nautilus/token.sh .

      #slaves
      for i in `seq 1 $NUM_SLAVES`
      do
      echo "ssh into slave-$i"
      jarvis ssh slave-$i "bash -s" -- < ./Change_Docker_Config_Slave.sh
      echo "slave-$i joining as a worker node"
      jarvis ssh slave-$i "bash -s" -- < ./token.sh
      done
    fi
fi
