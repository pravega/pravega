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

set -eo pipefail

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
    [ ! -z "$HOSTNAME" ] && add_system_property "config.controller.metricmetricsPrefix" "${HOSTNAME}"
    add_system_property "config.controller.server.zk.url" "${ZK_URL}"
    add_system_property "config.controller.server.store.host.type" "Zookeeper"
    echo "JAVA_OPTS=${JAVA_OPTS}"
}

configure_tier2() {
    add_system_property "pravegaservice.storageImplementation" "${TIER2_STORAGE}"

    case "${TIER2_STORAGE}" in
    FILESYSTEM)
    echo "Checking whether NFS mounting is required"
    if [ "${MOUNT_IN_CONTAINER}"  = "true" ]; then
        while [ -z ${NFS_SERVER} ]
        do
            echo "NFS_SERVER not set. Looping till the container is restarted with NFS_SERVER set."
            sleep 60
        done

        NFS_MOUNT=${NFS_MOUNT:-"/fs/"}
        echo "Mounting the NFS share"
        mkdir -p ${NFS_MOUNT}
        while [ true ]
        do
            mount -t nfs ${NFS_SERVER} ${NFS_MOUNT} -o nolock && break
            echo "Mount failed. Retrying after 5 sec ..."
            sleep 5
        done
    fi
    add_system_property "filesystem.root" "${NFS_MOUNT}"
    ;;

    HDFS)
    add_system_property "hdfs.hdfsUrl" "${HDFS_URL}"
    add_system_property "hdfs.hdfsRoot" "${HDFS_ROOT}"
    add_system_property "hdfs.replication" "${HDFS_REPLICATION}"
    ;;
    EXTENDEDS3)
    EXTENDEDS3_ROOT=${EXTENDEDS3_ROOT:-"/"}

    # Determine whether there is any variable missing
    if [ -z ${EXTENDEDS3_ACCESS_KEY_ID} ]
    then
        echo "EXTENDEDS3_ACCESS_KEY_ID is missing."
    fi

    if [ -z ${EXTENDEDS3_SECRET_KEY} ]
    then
        echo "EXTENDEDS3_SECRET_KEY is missing."
    fi

    if [ -z ${EXTENDEDS3_URI} ]
    then
        echo "EXTENDEDS3_URI is missing."
    fi

    if [ -z ${EXTENDEDS3_BUCKET} ]
    then
        echo "EXTENDEDS3_BUCKET is missing."
    fi

    # Loop until all variables are set
    while [ -z ${EXTENDEDS3_ACCESS_KEY_ID} ] ||
          [ -z ${EXTENDEDS3_SECRET_KEY} ] ||
          [ -z ${EXTENDEDS3_URI} ] ||
          [ -z ${EXTENDEDS3_BUCKET} ]
    do
        echo "Looping till the container is restarted with all these variables set."
        sleep 60
    done
    add_system_property "extendeds3.root" "${EXTENDEDS3_ROOT}"
    add_system_property "extendeds3.accessKey" "${EXTENDEDS3_ACCESS_KEY_ID}"
    add_system_property "extendeds3.secretKey" "${EXTENDEDS3_SECRET_KEY}"
    add_system_property "extendeds3.url" "${EXTENDEDS3_URI}"
    add_system_property "extendeds3.bucket" "${EXTENDEDS3_BUCKET}"
    add_system_property "extendeds3.namespace" "${EXTENDEDS3_NAMESPACE}"
    ;;
    esac
}
configure_segmentstore() {
    [ ! -z "$HOSTNAME" ] && add_system_property "metrics.metricsPrefix" "${HOSTNAME}"
    add_system_property "pravegaservice.zkURL" "${ZK_URL}"
    add_system_property "autoScale.controllerUri" "${CONTROLLER_URL}"
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
    configure_tier2
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
