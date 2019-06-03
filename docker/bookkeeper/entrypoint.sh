#!/bin/bash
#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
set -e

BOOKIE_PORT=${bookiePort:-${BOOKIE_PORT}}
BOOKIE_PORT=${BOOKIE_PORT:-3181}
BK_zkServers=$(echo "${ZK_URL:-127.0.0.1:2181}" | sed -r 's/;/,/g')
ZK_URL=$(echo "${ZK_URL:-127.0.0.1:2181}" | sed -r 's/,/;/g')
PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
PRAVEGA_CLUSTER_NAME=${PRAVEGA_CLUSTER_NAME:-"pravega-cluster"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}
BK_LEDGERS_PATH="/${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME}/ledgers"
BK_DIR="/bk"

export BOOKIE_PORT=${BOOKIE_PORT}
export BK_zkServers=${BK_zkServers}
export BK_metadataServiceUri=zk://${ZK_URL}${BK_LEDGERS_PATH}
export BK_journalDirectory=${BK_DIR}/journal
export BK_ledgerDirectories=${BK_DIR}/ledgers
export BK_indexDirectories=${BK_DIR}/index
export BK_CLUSTER_ROOT_PATH=/${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME}

export BK_tlsProvider=OpenSSL
export BK_tlsKeyStoreType=JKS
export BK_tlsKeyStore=/var/private/tls/bookie.keystore.jks
export BK_tlsKeyStorePasswordPath=/var/private/tls/bookie.keystore.passwd
export BK_tlsTrustStoreType=JKS
export BK_tlsTrustStore=/var/private/tls/bookie.truststore.jks
export BK_tlsTrustStorePasswordPath=/var/private/tls/bookie.truststore.passwd

echo "wait for zookeeper"
until zk-shell --run-once "ls /" ${BK_zkServers}; do sleep 5; done

# We need to update the metadata endpoint and Bookie ID before attempting to delete the cookie
sed -i "s|.*metadataServiceUri=.*\$|metadataServiceUri=${BK_metadataServiceUri}|" /opt/bookkeeper/conf/bk_server.conf
if [ ! -z "$BK_useHostNameAsBookieID" ]; then
  sed -i "s|.*useHostNameAsBookieID=.*\$|useHostNameAsBookieID=${BK_useHostNameAsBookieID}|" ${BK_HOME}/conf/bk_server.conf
fi

if [ `find $BK_journalDirectory $BK_ledgerDirectories $BK_indexDirectories -type f 2> /dev/null | wc -l` -gt 0 ]; then
  # The container already contains data in BK directories. This is probably because
  # the container has been restarted; or, if running on Kubernetes, it has probably been
  # updated or evacuated without losing its persistent volumes.
  echo "data available in bookkeeper directories; not formatting the bookie"
else
  # The container does not contain any BK data, it is probably a new
  # bookie. We will format any pre-existent data and metadata before starting
  # the bookie to avoid potential conflicts.
  echo "format bookie data and metadata"
  /opt/bookkeeper/bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie
fi

echo "start bookie"
/opt/bookkeeper/scripts/entrypoint.sh bookie
