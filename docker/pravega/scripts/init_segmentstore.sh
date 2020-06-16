#!/bin/sh
#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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
    echo "JAVA_OPTS=${JAVA_OPTS}"
}
