#!/bin/bash
#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# set -e

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

source ${SCRIPTS_DIR}/common.sh

# To create directories for multiple ledgers and journals if specified
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
    echo "Done waiting for Zookeeper"
}


create_zk_root() {
    if [ "x${BK_CLUSTER_ROOT_PATH}" != "x" ]; then
        echo "Creating the zk root dir '${BK_CLUSTER_ROOT_PATH}' at '${BK_zkServers}'"
        zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH} '' false false true" ${BK_zkServers}
        echo "Done creating the zk root dir"
    fi
}

configure_bk() {
    # We need to update the metadata endpoint and Bookie ID before attempting to delete the cookie
    sed -i "s|.*metadataServiceUri=.*\$|metadataServiceUri=${BK_metadataServiceUri}|" /opt/bookkeeper/conf/bk_server.conf
    if [ ! -z "$BK_useHostNameAsBookieID" ]; then
      sed -i "s|.*useHostNameAsBookieID=.*\$|useHostNameAsBookieID=${BK_useHostNameAsBookieID}|" ${BK_HOME}/conf/bk_server.conf
    fi
}

# Init the cluster if required znodes not exist in Zookeeper.
# Use ephemeral zk node as lock to keep initialize atomic.
function init_cluster() {
    zk-shell --run-once "ls ${BK_zkLedgersRootPath}/available/readonly" ${BK_zkServers}
    if [ $? -eq 0 ]; then
        echo "Metadata of cluster already exists, no need format"
    else
        echo "Sleep for a random time"
        sleep $[ ( $RANDOM % 10 )  + 1 ]s
        # create ephemeral zk node bkInitLock, initiator who this node, then do init; other initiators will wait.
        lock=`zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH}/bkInitLock1 '' true false false" ${BK_zkServers}`
        echo "output of zk-shell create bkInitLock"
        echo $lock
        if [ -z "$lock" ]; then
            # bkInitLock created success, this is the successor to do znode init
            echo "Bookkeeper znodes not exist in Zookeeper, do the init to create them."
            /opt/bookkeeper/bin/bookkeeper shell initnewcluster
            if [ $? -eq 0 ]; then
                echo "Bookkeeper znodes init success."
            else
                echo "Exit status of initnewcluster"
                echo $?
                echo "Bookkeeper znodes init failed. please check the reason."
                sleep 1000
                exit
            fi
        else
            echo "Other docker instance is doing initialize at the same time, will wait in this instance."
            tenSeconds=1
            while [ ${tenSeconds} -lt 1000 ]
            do
                sleep 10
                zk-shell --run-once "ls ${BK_zkLedgersRootPath}/available/readonly" ${BK_zkServers}
                if [ $? -eq 0 ]; then
                    echo "Waited $tenSeconds * 10 seconds, bookkeeper inited"
                    break
                else
                    echo "Waited $tenSeconds * 10 seconds, still not init"
                    (( tenSeconds++ ))
                    continue
                fi
            done

            if [ ${tenSeconds} -eq 1000 ]; then
                echo "Waited 1000 seconds for bookkeeper cluster init, something wrong, please check"
                exit
            fi
        fi
    fi
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
      /opt/bookkeeper/bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie
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

echo "Starting bookie"
/opt/bookkeeper/bin/bookkeeper bookie