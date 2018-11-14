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

kubernetes_service_info() {
    local jsonpath=$1
    local namespace=$(cat /etc/podinfo/namespace)
    local podname=$(cat /etc/podinfo/podname)
    local bearer=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
    local cacert="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    retval=$( curl --cacert ${cacert} -H "Authorization: Bearer ${bearer}" https://kubernetes/api/v1/namespaces/${namespace}/services/${podname} 2> /dev/null | jq -rM "${jsonpath}" 2> /dev/null )
    echo "$retval"
}

init_kubernetes() {
    if [ -d "/var/run/secrets/kubernetes.io" ] && [ ! -z ${K8_EXTERNAL_ACCESS} ]; then
        echo "Running in a Kubernetes environment and managed by the Pravega Operator with external access enabled"
        echo "Trying to obtain the external service endpoint..."

        service_type=$( kubernetes_service_info ".spec.type" )
        if [ ${service_type} == "LoadBalancer" ]; then
            export PUBLISHED_ADDRESS=$( kubernetes_service_info ".status.loadBalancer.ingress[].ip" )
            export PUBLISHED_PORT=$( kubernetes_service_info ".spec.ports[].port" )
            while [ -z ${PUBLISHED_ADDRESS} ] || [ -z ${PUBLISHED_PORT} ]
            do
                echo "LoadBalancer external address and port could not be obtained. Waiting 30 seconds and trying again..."
                sleep 30
                export PUBLISHED_ADDRESS=$( kubernetes_service_info ".status.loadBalancer.ingress[].ip" )
                export PUBLISHED_PORT=$( kubernetes_service_info ".spec.ports[].port" )
            done
            echo "Found external service endpoint: ${PUBLISHED_ADDRESS}:${PUBLISHED_PORT}"
        else
            echo "Service type is ${service_type}. Skipping published endpoint configuration..."
        fi
    fi
}
