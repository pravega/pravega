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
CLUSTER_NAME=${CLUSTER_NAME:-0}
MASTER_IP=${masterIP:-127.0.0.1}
MASTER_1=${MASTER_1:-0}

if [ $CLUSTER_NAME -eq 0 ]; then
  docker swarm init --advertise-addr $MASTER_IP
else
  jarvis save $CLUSTER_NAME
    if [ $MASTER_1 -ne 0 ]; then
      jarvis ssh master-1
      sed -i 's|"live-restore": true |"live-restore": false' /etc/docker/daemon.json
      docker swarm init --advertise-addr $MASTER_1
      TOKEN = `$(docker swarm join-token worker)`
      exit
      jarvis ssh slave-1
      sed -i 's|"live-restore": true |"live-restore": false' /etc/docker/daemon.json
      echo $TOKEN
      exit
      jarvis ssh slave-2
      sed -i 's|"live-restore": true |"live-restore": false' /etc/docker/daemon.json
      echo $TOKEN
      exit
      jarvis ssh slave-3
      sed -i 's|"live-restore": true |"live-restore": false' /etc/docker/daemon.json
      decho $TOKEN
      exit
      jarvis ssh slave-4
      sed -i 's|"live-restore": true |"live-restore": false' /etc/docker/daemon.json
      echo $TOKEN
      exit
      jarvis ssh slave-5
      sed -i 's|"live-restore": true |"live-restore": false' /etc/docker/daemon.json
      echo $TOKEN
      exit
    fi
fi
