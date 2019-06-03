#!/bin/sh
#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

k8() {
    local namespace=$1
    local resource_type=$2
    local resource_name=$3
    local jsonpath=$4
    local bearer=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
    local cacert="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    if [ -z "$namespace" ]; then
        retval=$( curl --cacert ${cacert} -H "Authorization: Bearer ${bearer}" https://kubernetes.default.svc/api/v1/${resource_type}/${resource_name} 2> /dev/null | jq -rM "${jsonpath}" 2> /dev/null )
    else
        retval=$( curl --cacert ${cacert} -H "Authorization: Bearer ${bearer}" https://kubernetes.default.svc/api/v1/namespaces/${namespace}/${resource_type}/${resource_name} 2> /dev/null | jq -rM "${jsonpath}" 2> /dev/null )
    fi

    if [ "$retval" == "null" ]; then
        retval=""
    fi

    echo "$retval"
}

init_kubernetes() {
    if [ -d "/var/run/secrets/kubernetes.io" ] && [ ! -z "${K8_EXTERNAL_ACCESS}" ]; then
        echo "Running in a Kubernetes environment and managed by the Pravega Operator with external access enabled"

        if [ -z "${POD_NAMESPACE}" ]; then
          echo "POD_NAMESPACE variable not set. Exiting..."
          exit 1
        fi

        if [ -z "${POD_NAME}" ]; then
          echo "POD_NAME variable not set. Exiting..."
          exit 1
        fi

        local ns=${POD_NAMESPACE}
        local podname=${POD_NAME}
        export PUBLISHED_ADDRESS=""
        export PUBLISHED_PORT=""

        service_type=$( k8 "${ns}" "services" "${podname}" ".spec.type" )
        if [ "${service_type}" == "LoadBalancer" ]; then
            while [ -z ${PUBLISHED_ADDRESS} ] || [ -z ${PUBLISHED_PORT} ]
            do
                echo "Trying to obtain LoadBalancer external endpoint..."
                sleep 10
                export PUBLISHED_ADDRESS=$( k8 "${ns}" "services" "${podname}" ".status.loadBalancer.ingress[0].ip" )
                export PUBLISHED_PORT=$( k8 "${ns}" "services" "${podname}" ".spec.ports[].port" )
            done
        elif [ "${service_type}" == "NodePort" ]; then
            nodename=$( k8 "${ns}" "pods" "${podname}" ".spec.nodeName" )
            while [ -z ${PUBLISHED_ADDRESS} ] || [ -z ${PUBLISHED_PORT} ]
            do
                echo "Trying to obtain NodePort external endpoint..."
                sleep 10
                export PUBLISHED_ADDRESS=$( k8 "" "nodes" "${nodename}" ".status.addresses[] | select(.type == \"ExternalIP\") | .address" )
                export PUBLISHED_PORT=$( k8 "${ns}" "services" "${podname}" ".spec.ports[].nodePort" )
            done
        else
            echo "Unexpected service type ${service_type}. Exiting..."
            exit 1
        fi
        echo "Found external service endpoint: ${PUBLISHED_ADDRESS}:${PUBLISHED_PORT}"
    fi
}
