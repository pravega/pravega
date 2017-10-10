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
MASTER_IP=${2:-127.0.0.1}
echo "IP for local docker execution is $MASTER_IP"
MASTER_1=${3:-null}
echo "Master IP of jarvis cluster is $MASTER_1"

if [ $CLUSTER_NAME = null ]; then
  docker swarm init --advertise-addr 127.0.0.1
else
  jarvis save $CLUSTER_NAME
    if [ $MASTER_1 != null ]; then
      #master
      jarvis ssh master-1 "bash -s" -- < ./Change_Docker_Config.sh
      jarvis ssh master-1 'docker swarm init --advertise-addr $MASTER_1; TOKEN = `docker swarm join-token worker`; echo $TOKEN; exit'

      #slave-1
      jarvis ssh slave-1 "bash -s" -- < ./Change_Docker_Config.sh
      jarvis ssh slave-1 'sed -i 's/"live-restore": true/"live-restore": false/' /etc/docker/daemon.json;
      cat /etc/docker/daemon.json; sudo service docker restart; echo $TOKEN; exit'

      #slave-2
      jarvis ssh slave-2 "bash -s" -- < ./Change_Docker_Config.sh
      jarvis ssh slave-2 'sed -i 'echo $TOKEN; exit'

      #slave-3
      jarvis ssh slave-3 "bash -s" -- < ./Change_Docker_Config.sh
      jarvis ssh slave-3 'sed -i 'echo $TOKEN; exit'

      #slave-4
      jarvis ssh slave-4 "bash -s" -- < ./Change_Docker_Config.sh
      jarvis ssh slave-4 'sed -i 'echo $TOKEN; exit'

      #slave-5
     jarvis ssh slave-5 "bash -s" -- < ./Change_Docker_Config.sh
     jarvis ssh slave-5 'sed -i 'echo $TOKEN; exit'
    fi
fi
