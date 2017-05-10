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

dir=$( cd "$( dirname "$0" )" && pwd )
# Adds a system property if the value is not empty
add_system_property() {
    local name=$1
    local value=$2

    if [ -n "${value}" ]; then
        export JAVA_OPTS="${JAVA_OPTS} -D${name}=${value}"
    fi
}

configure_controller() {
    add_system_property "config.controller.server.zk.url" "${ZK_URL}"
    add_system_property "config.controller.server.store.host.type" "Zookeeper"
    echo "JAVA_OPTS=${JAVA_OPTS}"
}

configure_segmentstore() {
    add_system_property "pravegaservice.zkURL" "${ZK_URL}"
    add_system_property "autoScale.controllerUri" "${CONTROLLER_URL}"
    add_system_property "hdfs.hdfsUrl" "${HDFS_URL}"
    add_system_property "hdfs.hdfsRoot" "${HDFS_ROOT}"
    add_system_property "hdfs.replication" "${HDFS_REPLICATION}"
    add_system_property "bookkeeper.zkAddress" "${BK_ZK_URL:-${ZK_URL}}"
    echo "JAVA_OPTS=${JAVA_OPTS}"
}

configure_standalone() {
    add_system_property "pravegaservice.publishedIPAddress" "${HOST_IP}"
    add_system_property "pravegaservice.listeningIPAddress" "0.0.0.0"
    add_system_property "singlenode.zkPort" "2181"
    add_system_property "singlenode.controllerPort" "9090"
    add_system_property "singlenode.segmentstorePort" "12345"
    echo "JAVA_OPTS=${JAVA_OPTS}"
}

if [ ${WAIT_FOR} ];then
    ${dir}/wait_for
fi

case $1 in
controller)
    configure_controller
    exec /opt/pravega/bin/pravega-controller
    ;;
segmentstore)
    configure_segmentstore
    exec /opt/pravega/bin/pravega-segmentstore
    ;;
standalone)
    configure_standalone
    exec /opt/pravega/bin/pravega-standalone
    ;;
*)
    echo "Usage: $0 (controller|segmentstore|standalone)"
    exit 1
    ;;
esac

