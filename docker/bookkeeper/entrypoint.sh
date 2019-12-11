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
# This file contains source code copied from the Apache BookKeeper project (http://bookkeeper.apache.org).
# Specifically, it contains source code copied from file `init_bookie.sh`, revision `3bfecd4`:
# https://github.com/apache/bookkeeper/blob/branch-4.7/docker/scripts/init_bookie.sh.
#

# Exit the script in case of an error
set -e

BOOKIE_PORT=${bookiePort:-${BOOKIE_PORT}}
BOOKIE_PORT=${BOOKIE_PORT:-3181}
BOOKIE_HTTP_PORT=${BOOKIE_HTTP_PORT:-9090}
BK_zkServers=$(echo "${ZK_URL:-127.0.0.1:2181}" | sed -r 's/;/,/g')
ZK_URL=$(echo "${ZK_URL:-127.0.0.1:2181}" | sed -r 's/,/;/g')
PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
PRAVEGA_CLUSTER_NAME=${PRAVEGA_CLUSTER_NAME:-"pravega-cluster"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}
BK_LEDGERS_PATH="/${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME}/ledgers"
BK_DIR="/bk"
BK_zkLedgersRootPath=${BK_LEDGERS_PATH}
BK_HOME=/opt/bookkeeper
BINDIR=${BK_HOME}/bin
BOOKKEEPER=${BINDIR}/bookkeeper
SCRIPTS_DIR=${BK_HOME}/scripts

export PATH=$PATH:/opt/bookkeeper/bin
export JAVA_HOME=/usr/lib/jvm/jre-1.8.0
export BK_zkLedgersRootPath=${BK_LEDGERS_PATH}
export BOOKIE_PORT=${BOOKIE_PORT}
export SERVICE_PORT=${BOOKIE_PORT}
export BK_bookiePort=${BK_bookiePort:-${BOOKIE_PORT}}
export BK_zkServers=${BK_zkServers}
export BK_metadataServiceUri=zk://${ZK_URL}${BK_LEDGERS_PATH}
export BK_journalDirectories=${BK_journalDirectories:-${BK_DIR}/journal}
export BK_ledgerDirectories=${BK_ledgerDirectories:-${BK_DIR}/ledgers}
export BK_indexDirectories=${BK_DIR}/index
export BK_CLUSTER_ROOT_PATH=/${PRAVEGA_PATH}/${PRAVEGA_CLUSTER_NAME}/${BK_CLUSTER_NAME}

export BK_tlsProvider=OpenSSL
export BK_tlsKeyStoreType=JKS
export BK_tlsKeyStore=/var/private/tls/bookie.keystore.jks
export BK_tlsKeyStorePasswordPath=/var/private/tls/bookie.keystore.passwd
export BK_tlsTrustStoreType=JKS
export BK_tlsTrustStore=/var/private/tls/bookie.truststore.jks
export BK_tlsTrustStorePasswordPath=/var/private/tls/bookie.truststore.passwd

# Override the `set -e` defined in the top of the file temporarily. Doing this is necessary as the Bookeeper
# `common.sh` script has logic that can return non-zero status.
set +e
source ${SCRIPTS_DIR}/common.sh
set -e

# Create directories for multiple ledgers and journals if specified.
create_bookie_dirs() {
  IFS=',' read -ra directories <<< $1
  for i in "${directories[@]}"
  do
      mkdir -p $i
      if [ "$(id -u)" = '0' ]; then
          chown -R "${BK_USER}:${BK_USER}" $i
      fi
  done
}

wait_for_zookeeper() {
    echo "Waiting for zookeeper"
    until zk-shell --run-once "ls /" ${BK_zkServers}; do sleep 5; done
}


create_zk_root() {
    if [ "x${BK_CLUSTER_ROOT_PATH}" != "x" ]; then
        echo "Creating the zk root dir '${BK_CLUSTER_ROOT_PATH}' at '${BK_zkServers}'"
        zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH} '' false false true" ${BK_zkServers}
    fi
}

configure_bk() {
    # We need to update the metadata endpoint and Bookie ID before attempting to delete the cookie
    sed -i "s|.*metadataServiceUri=.*\$|metadataServiceUri=${BK_metadataServiceUri}|" /opt/bookkeeper/conf/bk_server.conf
    if [ ! -z "$BK_useHostNameAsBookieID" ]; then
      sed -i "s|.*useHostNameAsBookieID=.*\$|useHostNameAsBookieID=${BK_useHostNameAsBookieID}|" ${BK_HOME}/conf/bk_server.conf
    fi
}

# Init the cluster if required znodes do not exist in Zookeeper.
function init_cluster() {
    set +e
    zk-shell --run-once "ls ${BK_zkLedgersRootPath}/available/readonly" ${BK_zkServers}
    if [ $? -eq 0 ]; then
        echo "Cluster metadata already exists"
    else
        echo "Sleeping for a random time"
        sleep $[ ( $RANDOM % 10 )  + 1 ]s

        # Create an ephemeral zk node `bkInitLock` for use as a lock.

        , initiator who this node, then do init; other initiators will wait.
        lock=`zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH}/bkInitLock1 '' true false false" ${BK_zkServers}`
        echo "Lock: $lock"

        if [ -z "$lock" ]; then
            echo "Bookkeeper znodes do not exist in Zookeeper. Initializing a new Bookeekeper cluster."
            /opt/bookkeeper/bin/bookkeeper shell initnewcluster
            if [ $? -eq 0 ]; then
                echo "initNewCluster operation succeeded"
            else
                echo "initNewCluster operation failed. Please check the reason."
                echo "Exit status of initnewcluster"
                echo $?
                sleep 1000
                exit
            fi
        else
            echo "Other containers may be initializing at the same time."
            tenSeconds=1
            while [ ${tenSeconds} -lt 1000 ]
            do
                sleep 10
                zk-shell --run-once "ls ${BK_zkLedgersRootPath}/available/readonly" ${BK_zkServers}
                if [ $? -eq 0 ]; then
                    echo "Waited $tenSeconds * 10 seconds. Successfully listed ''${BK_zkLedgersRootPath}/available/readonly'"
                    break
                else
                    echo "Waited $tenSeconds * 10 seconds. Continue waiting."
                    (( tenSeconds++ ))
                    continue
                fi
            done

            if [ ${tenSeconds} -eq 1000 ]; then
                echo "Waited 1000 seconds for bookkeeper cluster to initialize, but to no . Something is wrong, please check"
                exit
            fi
        fi
    fi
    set -e
}


format_bookie_data_and_metadata() {
    if [ `find $BK_journalDirectory $BK_ledgerDirectories $BK_indexDirectories -type f 2> /dev/null | wc -l` -gt 0 ]; then
      # The container already contains data in BK directories. This is probably because
      # the container has been restarted; or, if running on Kubernetes, it has probably been
      # updated or evacuated without losing its persistent volumes.
      echo "Data available in bookkeeper directories; not formatting the bookie"
    else
      # The container does not contain any BK data, it is probably a new
      # bookie. We will format any pre-existent data and metadata before starting
      # the bookie to avoid potential conflicts.
      echo "Formatting bookie data and metadata"
      /opt/bookkeeper/bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie || true
    fi
}

echo "Creating directories for journal and ledgers"
create_bookie_dirs "${BK_journalDirectories}"
create_bookie_dirs "${BK_ledgerDirectories}"

echo "Waiting for Zookeeper to come up"
wait_for_zookeeper

echo "Creating Zookeeper root"
create_zk_root

configure_bk

format_bookie_data_and_metadata

echo "Initializing Cluster"
init_cluster

echo "Starting the bookie"
/opt/bookkeeper/bin/bookkeeper bookie