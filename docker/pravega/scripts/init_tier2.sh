#!/bin/sh
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

init_tier2() {
    add_system_property "pravegaservice.storage.impl.name" "${TIER2_STORAGE}"

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
    add_system_property "hdfs.connect.uri" "${HDFS_URL}"
    add_system_property "hdfs.root" "${HDFS_ROOT}"
    add_system_property "hdfs.replication.factor" "${HDFS_REPLICATION}"
    ;;
    EXTENDEDS3)
    EXTENDEDS3_PREFIX=${EXTENDEDS3_PREFIX:-"/"}

    # Determine whether there is any variable missing
    if [ -z ${EXTENDEDS3_CONFIGURI} ]
    then
        echo "EXTENDEDS3_CONFIGURI is missing."
    fi

    if [ -z ${EXTENDEDS3_BUCKET} ]
    then
        echo "EXTENDEDS3_BUCKET is missing."
    fi

    if [ -z ${EXTENDEDS3_PREFIX} ]
    then
        echo "EXTENDEDS3_PREFIX is missing."
    fi

    # Loop until all variables are set
    while [ -z ${EXTENDEDS3_CONFIGURI} ] ||
          [ -z ${EXTENDEDS3_BUCKET} ]
    do
        echo "Looping till the container is restarted with all these variables set."
        sleep 60
    done
    add_system_property_ecs_config_uri "extendeds3.connect.config.uri" "${EXTENDEDS3_CONFIGURI}" "${EXTENDEDS3_ACCESS_KEY_ID}" "${EXTENDEDS3_SECRET_KEY}"
    add_system_property "extendeds3.bucket" "${EXTENDEDS3_BUCKET}"
    add_system_property "extendeds3.prefix" "${EXTENDEDS3_PREFIX}"
    add_certs_into_truststore
    ;;
    esac
}
