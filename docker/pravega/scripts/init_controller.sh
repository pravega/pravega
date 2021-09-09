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

init_controller() {
    [ ! -z "$HOSTNAME" ] && add_system_property "controller.metrics.prefix" "${HOSTNAME}"
    add_system_property "controller.zk.connect.uri" "${ZK_URL}"
    add_system_property "controller.server.store.host.type" "Zookeeper"
    [ -d "$INFLUX_DB_SECRET_MOUNT_PATH" ] \
    && [ -f "$INFLUX_DB_SECRET_MOUNT_PATH"/username ] && [ -f  "$INFLUX_DB_SECRET_MOUNT_PATH"/password ] \
    && add_system_property "metrics.influxDB.connect.credentials.username"  "$(cat "$INFLUX_DB_SECRET_MOUNT_PATH"/username)" \
    && add_system_property "metrics.influxDB.connect.credentials.pwd" "$(cat "$INFLUX_DB_SECRET_MOUNT_PATH"/password)"
    echo "JAVA_OPTS=${JAVA_OPTS}"

}
