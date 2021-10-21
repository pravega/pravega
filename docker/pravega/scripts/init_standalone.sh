#!/bin/sh
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

init_standalone() {
    add_system_property "pravegaservice.service.published.host.nameOrIp" "${HOST_IP}"
    add_system_property "pravegaservice.service.listener.host.nameOrIp" "0.0.0.0"
    add_system_property "singlenode.zk.port" "2181"
    add_system_property "singlenode.controller.rpc.port" "9090"
    add_system_property "singlenode.segmentStore.port" "12345"
    echo "JAVA_OPTS=${JAVA_OPTS}"
}
