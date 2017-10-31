#!/bin/bash
#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

CLUSTER_NAME=${1:-null}
echo "Cluster Name is $CLUSTER_NAME"
MASTER=${2:-null}
echo "Master IP of jarvis cluster is $MASTER"
NUM_SLAVES=${3:-null}
DOCKER_VERSION=`docker version --format '{{.Server.APIVersion}}'`
echo "Docker API version is $DOCKER_VERSION"

TOKEN=null
if [ $DOCKER_VERSION < 1.2.5 ]; then
exit
fi
if [ $CLUSTER_NAME != null ]; then
    jarvis save $CLUSTER_NAME
    if [ $MASTER != null ]; then
      #master
      jarvis ssh $CLUSTER_NAME "bash -s" -- < ./Change_Docker_Config_Master.sh $MASTER
      jarvis scp master-1:/home/nautilus/token.sh .

      #slaves
      jarvis ssh slave-1 "bash -s" -- < ./Change_Docker_Config_Slave.sh
      jarvis ssh slave-1 "bash -s" -- < ./token.sh

      jarvis ssh slave-2 "bash -s" -- < ./Change_Docker_Config_Slave.sh
      jarvis ssh slave-2 "bash -s" -- < ./token.sh

      jarvis ssh slave-3 "bash -s" -- < ./Change_Docker_Config_Slave.sh
      jarvis ssh slave-3 "bash -s" -- < ./token.sh

    fi
fi
