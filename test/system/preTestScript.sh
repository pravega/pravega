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
CLUSTER_NAME = $(CLUSTER_NAME:-null}
MASTER_IP=$(masterIP:-127.0.0.1}
MASTER_1=$(MASTER_1:-null)
SLAVE_1=$(SLAVE_1:-null)
SLAVE_2=$(SLAVE_2:-null)
SLAVE_3=$(SLAVE_3:-null)
SLAVE_4=$(SLAVE_4:-null)
SLAVE_5=$(SLAVE_5:-null)

if($CLUSTER_NAME -eq null); then
  docker swarm init --advertise-addr $MASTER_IP
else
  jarvis save $CLUSTER_NAME
    if($MASTER_1 -ne null); then
      jarvis ssh master-1
      sed -i 's|live-restore": true | "live-restore": false' /etc/docker/daemon.json
      docker swarm init --advertise-addr $MASTER_1
      TOKEN = $(docker swarm join-token worker)
      exit
    fi
    if($SLAVE_1 -ne null); then
      jarvis ssh slave-1
      sed -i 's|live-restore": true | "live-restore": false' /etc/docker/daemon.json
      docker swarm join
      exit
    fi
    if($SLAVE_2 -ne null); then
      jarvis ssh slave-1
      sed -i 's|live-restore": true | "live-restore": false' /etc/docker/daemon.json
      docker swarm join
      exit
    fi
    if($SLAVE_3 -ne null); then
      jarvis ssh slave-1
      sed -i 's|live-restore": true | "live-restore": false' /etc/docker/daemon.json
      docker swarm join
      exit
    fi
    if($SLAVE_3 -ne null); then
      jarvis ssh slave-1
      sed -i 's|live-restore": true | "live-restore": false' /etc/docker/daemon.json
      docker swarm join
      exit
    fi
    if($SLAVE_5 -ne null); then
      jarvis ssh slave-1
      sed -i 's|live-restore": true | "live-restore": false' /etc/docker/daemon.json
      docker swarm join
      exit
    fi
fi
