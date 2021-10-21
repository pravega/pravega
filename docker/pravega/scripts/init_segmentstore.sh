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

init_segmentstore() {
    [ ! -z "$PUBLISHED_ADDRESS" ] && add_system_property "pravegaservice.service.published.host.nameOrIp" "${PUBLISHED_ADDRESS}"
    [ ! -z "$PUBLISHED_ADDRESS" ] && add_system_property "pravegaservice.service.listener.host.nameOrIp" "0.0.0.0"
    [ ! -z "$PUBLISHED_PORT" ] && add_system_property "pravegaservice.service.published.port" "${PUBLISHED_PORT}"
    [ ! -z "$HOSTNAME" ] && add_system_property "metrics.prefix" "${HOSTNAME}"
    add_system_property "pravegaservice.zk.connect.uri" "${ZK_URL}"
    add_system_property "autoScale.controller.connect.uri" "${CONTROLLER_URL}"
    add_system_property "bookkeeper.zk.connect.uri" "${BK_ZK_URL:-${ZK_URL}}"
    add_system_property "pravegaservice.storageThreadPool.size" "${STORAGE_THREAD_POOL_SIZE}"
    [ -d "$INFLUX_DB_SECRET_MOUNT_PATH" ] \
    && [ -f "$INFLUX_DB_SECRET_MOUNT_PATH"/username ] && [ -f  "$INFLUX_DB_SECRET_MOUNT_PATH"/password ] \
    && add_system_property "metrics.influxDB.connect.credentials.username"  "$(cat "$INFLUX_DB_SECRET_MOUNT_PATH"/username)" \
    && add_system_property "metrics.influxDB.connect.credentials.pwd" "$(cat "$INFLUX_DB_SECRET_MOUNT_PATH"/password)"
    echo "JAVA_OPTS=${JAVA_OPTS}"
}
