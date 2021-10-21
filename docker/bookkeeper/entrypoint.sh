#!/bin/bash
#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Note: This file contains source code copied from the Apache BookKeeper
#       project (http://bookkeeper.apache.org). Specifically, it contains
#       source code copied from file `init_bookie.sh`, revision `3bfecd4`:
#  github.com/apache/bookkeeper/blob/branch-4.7/docker/scripts/init_bookie.sh.
#

# Exit the script in case of an error
set -e

BOOKIE_PORT=${bookiePort:-${BOOKIE_PORT}}
BOOKIE_PORT=${BOOKIE_PORT:-3181}
BOOKIE_HTTP_PORT=${BOOKIE_HTTP_PORT:-8080}
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

# Create a Bookie ID if this is a newly added bookkeeper pod
# or read the Bookie ID if a cookie containing this value already exists
set_bookieid() {
  IFS=',' read -ra journal_directories <<< $BK_journalDirectories
  COOKIE="${journal_directories[0]}/current/VERSION"
  if [ `find ${COOKIE} | wc -l` -gt 0 ]; then
    # Reading the Bookie ID value from the existing cookie
    bkHost=`cat ${COOKIE} | grep bookieHost`
    IFS=" " read -ra id <<< $bkHost
    BK_bookieId=${id[1]:1:-1}
  else
    # Creating a new Bookie ID following the latest nomenclature
    BK_bookieId="`hostname -s`-${RANDOM}"
  fi
  echo "BookieID = $BK_bookieId"
  sed -i "s|.*bookieId=.*\$|bookieId=${BK_bookieId}|" ${BK_HOME}/conf/bk_server.conf
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

initialize_cluster() {
    set +e

    zk-shell --run-once "ls ${BK_zkLedgersRootPath}/available/readonly" ${BK_zkServers}
    if [ $? -eq 0 ]; then
        echo "Cluster metadata already exists"
    else
        # Create an ephemeral zk node `bkInitLock` for use as a lock.
        lock=`zk-shell --run-once "create ${BK_CLUSTER_ROOT_PATH}/bkInitLock '' true false false" ${BK_zkServers}`
        if [ -z "$lock" ]; then
            echo "Bookkeeper znodes do not exist in Zookeeper. Initializing a new Bookeekeper cluster."

            # Note that this `bookkeeper` shell script sets "exit on error" (`set -e`) and sources from
            # ${SCRIPTS_DIR}/common.sh. Make sure to invoke `fix_bk_ipv6_check()` before the control reaches here.
            /opt/bookkeeper/bin/bookkeeper shell initnewcluster
            if [ $? -eq 0 ]; then
                echo "initnewcluster operation succeeded"
            else
                echo "initnewcluster operation failed. Please check the reason."
                echo "Exit status of initnewcluster"
                echo $?
                exit
            fi
        else
            echo "Another instance might be initializing the cluster at the same time."
            tenSeconds=1
            while [ ${tenSeconds} -lt 20 ]
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

            if [ ${tenSeconds} -eq 20 ]; then
                echo "Waited ${tenSeconds} * 20 seconds for bookkeeper cluster to initialize, but to no avail. Something is wrong, please check."
                exit
            fi
        fi
    fi
    set -e
}

format_bookie_data_and_metadata() {
    IFS=',' read -ra journal_directories <<< $BK_journalDirectories
    IFS=',' read -ra ledger_directories <<< $BK_ledgerDirectories
    IFS=" " eval 'directory_names="${journal_directories[*]} ${ledger_directories[*]}"'
    if [ `find $directory_names $BK_indexDirectories -type f 2> /dev/null | wc -l` -gt 0 ]; then
      # The container already contains data in BK directories. Examples of when this can happen include:
      #    - A container was restarted, say, in a non-Kubernetes deployment.
      #    - A container running on Kubernetes was updated/evacuated, and
      #      it did not lose its persistent volumes.
      echo "Data available in bookkeeper directories; not formatting the bookie"
    else
      # The container does not contain any BK data, and it is likely a new
      # bookie. We will format any pre-existent data and metadata before starting
      # the bookie to avoid potential conflicts.
      echo "Formatting bookie data and metadata"
      /opt/bookkeeper/bin/bookkeeper shell bookieformat -nonInteractive -force -deleteCookie || true
    fi
}

# The reason for creating custom journal and ledger files here is to support
# multi ledger/journal scenarios for better write performance. It was found that
# performance can be increased by increasing write parallelism for those files.
#
# However, during those experiments it was also found that index dir has a very low write
# throughput, so using the default settings for it should not have any negative effect
# on performance. Therefore, we do not set the paths for index directories below.
echo "Creating directories for Bookkeeper journal and ledgers"
create_bookie_dirs "${BK_journalDirectories}"
create_bookie_dirs "${BK_ledgerDirectories}"

echo "Configuring the Bookie ID"
set_bookieid

echo "Sourcing ${SCRIPTS_DIR}/common.sh"
source ${SCRIPTS_DIR}/common.sh

echo "Waiting for Zookeeper to come up"
wait_for_zookeeper

echo "Creating Zookeeper root"
create_zk_root

echo "Configuring Bookkeeper"
configure_bk

echo "Formatting Bookie data and metadata, if needed"
format_bookie_data_and_metadata

echo "Initializing Cluster"
initialize_cluster

echo "Starting the bookie"
/opt/bookkeeper/bin/bookkeeper bookie
