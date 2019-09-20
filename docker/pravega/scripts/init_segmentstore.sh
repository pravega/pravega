#!/bin/sh
#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

init_segmentstore() {
    [ ! -z "$PUBLISHED_ADDRESS" ] && add_system_property "pravegaservice.publishedIPAddress" "${PUBLISHED_ADDRESS}"
    [ ! -z "$PUBLISHED_ADDRESS" ] && add_system_property "pravegaservice.listeningIPAddress" "0.0.0.0"
    [ ! -z "$PUBLISHED_PORT" ] && add_system_property "pravegaservice.publishedPort" "${PUBLISHED_PORT}"
    [ ! -z "$HOSTNAME" ] && add_system_property "metrics.metricsPrefix" "${HOSTNAME}"
    add_system_property "pravegaservice.zkURL" "${ZK_URL}"
    add_system_property "autoScale.controllerUri" "${CONTROLLER_URL}"
    add_system_property "bookkeeper.zkAddress" "${BK_ZK_URL:-${ZK_URL}}"
    add_system_property "pravegaservice.storageThreadPoolSize" "${STORAGE_THREAD_POOL_SIZE}"
    echo "JAVA_OPTS=${JAVA_OPTS}"
}
