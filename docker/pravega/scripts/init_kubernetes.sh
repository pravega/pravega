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
        local service=$( k8 "${ns}" "services" "${podname}" .kind )

        if [[  "${service}" != "Service" ]];
        then 
            echo "Failed to get External Service. Exiting..."
            exit 1     
        fi

	export PUBLISHED_ADDRESS=$( k8 "${ns}" "services" "${podname}" ".metadata.annotations[\"external-dns.alpha.kubernetes.io/hostname\"]" )

        if [[ -n ${PUBLISHED_ADDRESS} && "${PUBLISHED_ADDRESS:${#PUBLISHED_ADDRESS}-1}" == "." ]];
        then
          export PUBLISHED_ADDRESS=${PUBLISHED_ADDRESS::-1}
        fi

        service_type=$( k8 "${ns}" "services" "${podname}" ".spec.type" )
        if [ "${service_type}" == "LoadBalancer" ]; then
            while [ -z ${PUBLISHED_ADDRESS} ] || [ -z ${PUBLISHED_PORT} ]
            do
                if [ -z ${PUBLISHED_ADDRESS} ]; then
		        echo "Trying to obtain LoadBalancer external endpoint..."
		        sleep 10
                	export PUBLISHED_ADDRESS=$( k8 "${ns}" "services" "${podname}" ".status.loadBalancer.ingress[0].ip" )
                	if [ -z "${PUBLISHED_ADDRESS}" ]; then
                    		export PUBLISHED_ADDRESS=$( k8 "${ns}" "services" "${podname}" ".status.loadBalancer.ingress[0].hostname" )
                	fi
		fi
                export PUBLISHED_PORT=$( k8 "${ns}" "services" "${podname}" ".spec.ports[0].port" )
            done
        elif [ "${service_type}" == "NodePort" ]; then
            nodename=$( k8 "${ns}" "pods" "${podname}" ".spec.nodeName" )
            while [ -z ${PUBLISHED_ADDRESS} ] || [ -z ${PUBLISHED_PORT} ]
            do
                if [ -z ${PUBLISHED_ADDRESS} ]; then
		  echo "Trying to obtain NodePort external endpoint..."
                  sleep 10
                  export PUBLISHED_ADDRESS=$( k8 "" "nodes" "${nodename}" ".status.addresses[] | select(.type == \"ExternalIP\") | .address" )
                  export PUBLISHED_PORT=$( k8 "${ns}" "services" "${podname}" ".spec.ports[0].nodePort" )
            	fi
	    done
        else
            echo "Unexpected service type ${service_type}. Exiting..."
            exit 1
        fi
        echo "Found external service endpoint: ${PUBLISHED_ADDRESS}:${PUBLISHED_PORT}"
    fi
}
