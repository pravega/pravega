#!/bin/sh

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
    add_system_property "dlog.hostname" "${ZK_URL%:*}"
    add_system_property "dlog.port" "${ZK_URL#*:}"
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

