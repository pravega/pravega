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

init_controller() {
    [ ! -z "$HOSTNAME" ] && add_system_property "controller.metrics.metricsPrefix" "${HOSTNAME}"
    add_system_property "controller.zk.url" "${ZK_URL}"
    add_system_property "controller.server.store.host.type" "Zookeeper"
    echo "JAVA_OPTS=${JAVA_OPTS}"
}
