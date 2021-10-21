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
set -e
MASTER=${1:-null}
sed -i '1i {"hosts":["tcp://0.0.0.0:2375","unix:///var/run/docker.sock"],"insecure-registries": ["0.0.0.0/0"], "live-restore": false, "log-opts":{"max-size": "25m", "max-file": "2"}, "mtu": 1450 }' /etc/docker/daemon.json
cat /etc/docker/daemon.json
service docker stop
service docker start
docker swarm init --advertise-addr $MASTER
docker network create -d overlay --attachable docker-network
docker swarm join-token worker > token.sh
exit
