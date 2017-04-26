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
}

configure_segmentstore() {
    add_system_property "pravegaservice.zkUrl" "${ZK_URL}"
    add_system_property "autoScale.controllerUri" "${CONTROLLER_URL}"
    add_system_property "hdfs.hdfsUrl" "${HDFS_URL}"
    add_system_property "hdfs.hdfsRoot" "${HDFS_ROOT}"
    add_system_property "hdfs.replication" "${HDFS_REPLICATION}"
    add_system_property "dlog.hostname" "${ZK_URL%:*}"
    add_system_property "dlog.port" "${ZK_URL#*:}"
}

configure_singlenode() {
    add_system_property "pravegaservice.publishedIPAddress" "${HOST_IP}"
    add_system_property "pravegaservice.listeningIPAddress" "0.0.0.0"
    add_system_property "log.dir" "/opt/pravega/logs"
    add_system_property "logback.configurationFile" "/opt/pravega/conf/logback.xml"
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
singlenode)
    configure_singlenode
    exec /opt/pravega/bin/pravega-singlenode 2181 9090 12345
    ;;
*)
    echo "Usage: $0 (controller|segmentstore|singlenode)"
    exit 1
    ;;
esac

