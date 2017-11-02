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
set -e
MASTER=${1:-null}
sed -i '1i {"hosts":["tcp://0.0.0.0:2375","unix:///var/run/docker.sock"]}' /etc/docker/daemon.json
sed -i 's/"live-restore": true,/"live-restore": false,/' /etc/docker/daemon.json
cat /etc/docker/daemon.json
service docker stop
service docker start
docker swarm init --advertise-addr $MASTER
docker network create -d overlay --attachable docker-network
docker swarm join-token worker > token.sh && mv token.sh /home/nautilus
exit