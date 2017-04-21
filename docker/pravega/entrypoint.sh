#!/bin/sh

case $1 in
controller)
    exec /opt/pravega/bin/pravega-controller
    ;;
segmentstore)
    exec /opt/pravega/bin/pravega-segmentstore
    ;;
singlenode)
    export JAVA_OPTS="-Dlog.dir=/opt/pravega/logs -Dlogback.configurationFile=/opt/pravega/conf/logback.xml"
    exec /opt/pravega/bin/pravega-singlenode
    ;;
*)
    echo "Usage: $0 (controller|segmentstore|singlenode)"
    exit 1
    ;;
esac

