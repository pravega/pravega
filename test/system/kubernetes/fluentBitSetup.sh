#!/usr/bin/env bash
################################################################################
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
################################################################################

BASE=$(dirname $(readlink -f $BASH_SOURCE))

set -euo pipefail

PVC_NAME=pravega-log-sink
VOLUME_NAME=logs
HOST_LOGS=host-logs
MOUNT_PATH=/data
CONFIG_MAP_NAME="pravega-logrotate"
CONFIG_MAP_DATA=/etc/config
KEEP_PVC=false
NAMESPACE=${NAMESPACE:-"default"}
NAME=${NAME:-"pravega-fluent-bit"}
LOGS_DIR="pravega-logs-export"
TAR_NAME="${LOGS_DIR}.tar"
SKIP_FORCE_ROTATE=${SKIP_FORCE_ROTATE:-"false"}
ALPINE_IMAGE=${ALPINE_IMAGE:-"alpine:latest"}
SKIP_LOG_BUNDLE_COMPRESSION=${SKIP_LOG_BUNDLE_COMPRESSION:-"false"}
RETRIES=3

# Configurable flag parameters.
FLUENT_BIT_DEPLOYMENT=${FLUENT_BIT_DEPLOYMENT:-"$NAME"}
FLUENT_IMAGE_REPO=${FLUENT_IMAGE_REPO:-"fluent/fluent-bit"}
FLUENT_BIT_IMAGE_TAG=${FLUENT_BIT_IMAGE_TAG:-"1.7.1"}
FLUENT_BIT_STORAGE_CLASS=${FLUENT_BIT_STORAGE_CLASS:-"standard"}
FLUENT_BIT_PVC_CAPACITY=${FLUENT_BIT_PVC_CAPACITY:-50}
FLUENT_BIT_RECLAIM_TARGET_PERCENT=${FLUENT_BIT_RECLAIM_TARGET_PERCENT:-75}
FLUENT_BIT_RECLAIM_TRIGGER_PERCENT=${FLUENT_BIT_RECLAIM_TRIGGER_PERCENT:-95}
FLUENT_BIT_ROTATE_INTERVAL_SECONDS=${LOG_ROTATE_INTERVAL:-10}
FLUENT_BIT_HOST_LOGS_PATH=${FLUENT_BIT_HOST_LOGS_PATH:-""}
FLUENT_BIT_ROTATE_THRESHOLD_BYTES=${FLUENT_BIT_ROTATE_THRESHOLD_BYTES:-10000000}
FLUENT_BIT_EXPORT_PATH=${FLUENT_BIT_EXPORT_PATH:-"archive"}
LOG_ROTATE_CONF_PATH=${LOG_ROTATE_CONF_PATH:-$CONFIG_MAP_DATA/"logrotate.conf"}
LOG_EXT="gz"

logs_fetched=1
###################################
### Flags/Args Parsing
###################################

# Flags must be parsed before any of the heredocs are expanded into variables.
set +u
CMD=$1
set -u
shift

for i in "$@"; do
    case $i in
        # Options.
    -c=* | --pvc-capacity=*)
        FLUENT_BIT_PVC_CAPACITY="${i#*=}"
        ;;
    -r=* | --pvc-reclaim-percent=*)
        FLUENT_BIT_RECLAIM_TARGET_PERCENT="${i#*=}"
        ;;
    -t=* | --pvc-reclaim-trigger=*)
        FLUENT_BIT_RECLAIM_TRIGGER_PERCENT="${i#*=}"
        ;;
    -h=* | --host-path=*)
        FLUENT_BIT_HOST_LOGS_PATH="${i#*=}"
        ;;
    -s=* | --storageclass=*)
        FLUENT_BIT_STORAGE_CLASS="${i#*=}"
        ;;
    -i=* | --rotation-interval=*)
        FLUENT_BIT_ROTATE_INTERVAL_SECONDS="${i#*=}"
        ;;
    -n=* | --namespace=*)
        NAMESPACE="${i#*=}"
        ;;
    -p=* | --export-path=*)
        FLUENT_BIT_EXPORT_PATH="${i#*=}"
        ;;
    -m | --mount)
        FLUENT_BIT_EXPORT_MOUNT="true"
        ;;
    -f | --skip-force-rotate)
        SKIP_FORCE_ROTATE="true"
        ;;
    -b | --skip-bundle-compression)
        SKIP_LOG_BUNDLE_COMPRESSION="true"
        ;;
esac
done

kilobyte=1024
megabyte=$((kilobyte*1024))
gigabyte=$((megabyte*1024))

yellow() {
    local msg="$*"
    local yellow="\033[38;2;242;218;75m"
    local nocolor='\033[0m'
    echo -ne "${yellow}$msg${nocolor}"
}

green() {
    local msg="$*"
    local green="\033[38;2;191;216;95m"
    local nocolor='\033[0m'
    echo -ne "${green}$msg${nocolor}"
}

######################################
# Calculates the current utilization as a percentage of the fluent-bit PVC.
# Globals:
#   MOUNT_PATH
#   NAMESPACE
#   FLUENT_BIT_PVC_CAPACITY
# Arguments:
#   The pod name of the pravega-log pod
#   The log name to copy
# Outputs:
#   Writes the utilization to STDOUT
######################################
utilization() {
    local log_pod="$1"
    local size_kilo=$(kubectl exec -n=$NAMESPACE $log_pod -- du -s $MOUNT_PATH)
    local total=$(kubectl get pvc -n=$NAMESPACE $PVC_NAME -o custom-columns=:.status.capacity.storage --no-headers)
    total=${total%Gi}
    size_kilo=${size_kilo%$MOUNT_PATH}
    local size_gi=$(bc -l <<< "scale=3; $size_kilo/$megabyte")
    local percent=$(bc -l <<< "scale=3; ($size_kilo*$kilobyte)/($total*$gigabyte) * 100")
    echo "$MOUNT_PATH has a utilization of $percent% (${size_gi} Gi/${total} Gi)."
}

force_rotate() {
    if [ "$SKIP_FORCE_ROTATE" != "true" ]; then
        # Prematurely rotate the logs to receive the most up to date log set.
        kubectl exec "$pravega_log_pod" -n=$NAMESPACE -- /etc/config/watch.sh force
    fi
}

kubectl_cp() {
    local file=$1
    local output_file=$2
    if ! kubectl cp "$pravega_log_pod:$MOUNT_PATH/$file" "$output_file" > /dev/null 2>&1; then
        return 1
    fi

    return 0
}

################################
# Performs a copy of a single file from a pod with a retry mechanism.
# Globals:
#   TAR_NAME
# Arguments:
#   The pod name of the pravega-log pod
#   The log name to copy
######################################
cp_log() {
    local log_pod=$1
    local file=$2
    file=${file#./}
    # The leading ./ must be parsed out in the src file as well as the ':' from the dest.
    local output_file="$(echo $file | sed 's/:/-/g')"
    local tries=0
    printf "%.80s (%s/%s)\n" "$output_file" "$(yellow $logs_fetched)" "$(green $total)"
    # If we needed to create a directory for this collection, make sure to clean it up.
    while ! kubectl_cp "$file" "$output_file"; do
        : $((tries+=1))
        if [ "$tries" -ge "$RETRIES" ]; then
            echo "Failed downloading log $output_file." >> "$FLUENT_BIT_EXPORT_PATH/debug.log"
            break
        fi
    done
}

######################################
# Fetches a list of filenames from a remote pod.
# Globals:
#   TAR_NAME
# Arguments:
#   The output directory.
#   A space delimited list file paths that exist on the remote log pod.
# Outputs:
#   Set of log files downloaded to $FLUENT_BIT_EXPORT_PATH.
######################################
cp_remote_logs() {
    local remote_log_files=$@
    if [ -z "$remote_log_files" ]; then
        echo "No remote files given to collect."
    fi
    remote_log_files=($remote_log_files)

    # Clean any previous instances of collected logs.
    rm -rf "$TAR_NAME"{.gz,} "$LOGS_DIR"{.zip,}
    # Temporary directory to hold the log files.
    local logs_dir=$LOGS_DIR
    mkdir "$logs_dir" && cd "$logs_dir"

    local total=${#remote_log_files[@]}
    for file in "${remote_log_files[@]}"; do
        cp_log "$pravega_log_pod" "$file" &
        ((logs_fetched+=1))
        if [ $((logs_fetched % 10)) = '0' ]; then
            wait
        fi
    done
    wait
    # Return from $logs_dir.
    cd ../
    # Validate log collection -- compare number of fetched logs to number of given logs.
    local actual_logs="$(find $logs_dir -type f)"
    local actual_log_count="$(echo "$actual_logs" | wc -l)"
    local expected_log_count="$total"

    if [ "$expected_log_count" != "$actual_log_count" ]; then
        echo -e "\nFound mismatch between expected # of logs ($expected_log_count) and actual ($actual_log_count)."
        for log in "${remote_log_files[@]}"; do
            log="$(echo $log | sed -e 's/:/-/g' -e 's/\.\///g')"
            if ! echo "$actual_logs" | grep "$log" > /dev/null; then
                echo -e "Missing '$log'."
            fi
        done
    else
        echo ""
        echo "Successfully downloaded a total of $actual_log_count log files."
    fi

    if [ "$SKIP_LOG_BUNDLE_COMPRESSION" != "true" ]; then
      if command -v zip; then
        zip -r "$logs_dir.zip" "$logs_dir" > /dev/null
      else
        tar --remove-files -zcf "$TAR_NAME.gz" "$logs_dir"
      fi
      rm -rf "$logs_dir"
    fi

    logs_fetched=1
}

######################################
# Fetches the set of logs belonging to a list of *active* pods.
# Globals:
#   SKIP_FORCE_ROTATE
# Arguments:
#   The output path to store the logs.
#   A space delimited list of pairs (namespace pod) used to gather the logs from.
# Outputs:
#   Set of log files downloaded.
######################################
fetch_active_logs() {
    local output=$1; shift
    if [ ! -d "$output" ]; then
        echo "$output directory does not exist!"
        exit 1
    fi

    pravega_log_pod=$(kubectl get pods -n=$NAMESPACE -l "app=$FLUENT_BIT_DEPLOYMENT" -o custom-columns=:.metadata.name --no-headers)
    force_rotate

    log_files=()
    local pods=${@:-""}
    local logs=$(kubectl exec $pravega_log_pod -- find . -mindepth 2 -name '*.gz')
    # For all logs that exist on the PVC, find the logs belonging to the inputted pods.
    while read -r namespace pod; do
        tag=${pod##*-}
        if echo "$logs" | grep "$namespace/${pod%-$tag}/$tag" > /dev/null; then
            matches=$(echo "$logs" | grep "$namespace/${pod%-$tag}/$tag")
            for match in $matches; do
                log_files+=("$match")
            done;
        fi
    done <<< $pods
    pushd "$output" > /dev/null 2>&1
    cp_remote_logs "${log_files[@]}"
    popd > /dev/null 2>&1
}

######################################
# Unlike 'fetch_active_logs', this function pulls all persisted logs from the provided
# namespace. If not namespace it provided, it will fetch all logs from all namespaces.
# Globals:
#   SKIP_FORCE_ROTATE
# Arguments:
#   The output path to store the logs.
# Outputs:
#   Set of log files downloaded.
######################################
fetch_stored_logs() {
    local output=$1; shift
    local namespace=${1:-''}
    if [ ! -d "$output" ]; then
        echo "$output directory does not exist!"
        exit 1
    fi
    pravega_log_pod=$(kubectl get pods -n=$NAMESPACE -l "app=$FLUENT_BIT_DEPLOYMENT" -o custom-columns=:.metadata.name --no-headers)
    force_rotate

    local logs=''
    if [ -z "$namespace" ]; then
        logs=$(kubectl exec $pravega_log_pod -- find . -mindepth 2 -name '*.gz' -type f)
    else 
        logs=$(kubectl exec $pravega_log_pod -- find "$namespace" -mindepth 1 -name '*.gz' -type f)
    fi

    pushd "$output" > /dev/null 2>&1
    cp_remote_logs $logs
    popd > /dev/null 2>&1
}

#################################
# Fluent Bit Configuration
#################################

# See information about tag expansion: https://docs.fluentbit.io/manual/pipeline/filters/kubernetes
FLUENT_BIT_INPUTS=$(cat << EOF
[INPUT]
    Name tail
    Path /var/log/containers/*.log
    Parser docker
    Tag kube.*
    Mem_Buf_Limit 5MB
    Skip_Long_Lines Off
EOF
)

# Regex field of 'trim_newline' must be grouped (?<group>).
FLUENT_BIT_PARSERS=$(cat << EOF
[PARSER]
    Name docker_no_time
    Format json
    Time_Keep Off
    Time_Key time
    Time_Format %Y-%m-%dT%H:%M:%S.%L

[PARSER]
    Name trim_newline
    Format regex
    Regex (?<log>[^\\\n]*)
EOF
)

FLUENT_BIT_FILTERS=$(cat << EOF
[FILTER]
    Name             kubernetes
    Match            kube.*
    Kube_URL         https://kubernetes.default.svc:443
    Merge_Log        On

[FILTER]
    Name record_modifier
    Match *
    Whitelist_key log

[FILTER]
    Name parser
    Match *
    Key_Name log
    Parser trim_newline
EOF
)

FLUENT_BIT_OUTPUTS=$(cat << EOF
[OUTPUT]
    Name file
    Match *
    Path $MOUNT_PATH
    Format template
    Template {log}
EOF
)

FLUENT_BIT_SERVICE=$(cat << EOF
[SERVICE]
    Flush 1
    Daemon off
    Log_Level Info
    Parsers_File parsers.conf
    Parsers_File custom_parsers.conf
    HTTP_Server On
    HTTP_Listen 0.0.0.0
    HTTP_Port {{ .Values.service.port }}
EOF
)

#################################
# Log Rotation Configuration
#################################

# See logrotate manpage for more information.
LOG_ROTATE_CONF=$(cat << EOF
$MOUNT_PATH/*.log {
    su root root
    compress
    copytruncate
    size $FLUENT_BIT_ROTATE_THRESHOLD_BYTES
    rotate -1
    dateext
    dateformat -%s
}
EOF
)

######################################
# Generates the script used to watch, cleanup and normalize the rotated logs produced by log-rotate.
# Globals:
#   FLUENT_BIT_PVC_CAPACITY
#   FLUENT_BIT_RECLAIM_TRIGGER_PERCENT
#   FLUENT_BIT_RECLAIM_TARGET_PERCENT
#   MOUNT_PATH
#   LOG_EXT
#   FLUENT_BIT_ROTATE_INTERVAL_SECONDS
# Arguments
#   None
# Outputs
#   Compresses all files exceeding FLUENT_BIT_ROTATE_THRESHOLD_BYTES.
#   Removes oldest compressed logs once PVC capacity reachized FLUENT_BIT_RECLAIM_TRIGGER_PERCENT utilization.
#   * Removes files until FLUENT_BIT_RECLAIM_TARGET_PERCENT% of the PVC has been reclaimed.
# Note:
#   Escape all the non-configurable variables to avoid unintended command substitutions or variables expansions.
######################################

apply_logrotate_configmap() {
    tab='    '
    local log_rotate_watch=$(cat $BASE/fluentBitRotater.sh)
    # Apply required indentation by prepending two tabs.
    log_rotate_watch=$(echo "$log_rotate_watch" | sed "s/^/$tab$tab/")
    log_rotate_conf=$(echo "$LOG_ROTATE_CONF" | sed "s/^/$tab$tab/")
    cat << EOF | kubectl apply -n=$NAMESPACE --wait -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: $CONFIG_MAP_NAME
  namespace: $NAMESPACE
data:
    watch.sh: |
$log_rotate_watch
    logrotate.conf: |
$log_rotate_conf
EOF
}

# Mount the above configmap to provide the logrotate conf and watch/naming functionality.
# Also mount the $PVC_NAME PVC to provide an entry point into the logs.
apply_logrotate_deployment() {
    cat << EOF | kubectl apply -n=$NAMESPACE --wait -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $FLUENT_BIT_DEPLOYMENT
  labels:
    app: $FLUENT_BIT_DEPLOYMENT
spec:
  replicas: 1
  selector:
    matchLabels:
      app: $FLUENT_BIT_DEPLOYMENT
  template:
    metadata:
      labels:
        app: $FLUENT_BIT_DEPLOYMENT
    spec:
      containers:
      - name: rotate
        image: $ALPINE_IMAGE
        workingDir: $MOUNT_PATH
        command: [ '/bin/ash', '-c' ]
        args:
          - apk add logrotate;
            apk add bash;
            apk add findutils;
            apk add grep;
            $CONFIG_MAP_DATA/watch.sh install
        volumeMounts:
        - name: pravega-logs
          mountPath: $MOUNT_PATH
        - name: logrotate
          mountPath: $CONFIG_MAP_DATA
      volumes:
      - name: logrotate
        configMap:
          name: $CONFIG_MAP_NAME
          defaultMode: 0700
      - name: pravega-logs
        persistentVolumeClaim:
          claimName: $PVC_NAME
EOF
}

####################################
#### Main Process
####################################

# Kubernetes produces a log file for each pod (/var/log/containers/*). Each line
# of stdout generated by the application is appended to it's respective log file,
# formatted based on the configured 'logging driver' (json-file by default).
#
#   {"log":"...\n","stream":"...","time":"..."}
#
# The fluent-bit transformation process is as follows:
#
# 0. The Kubernetes filter default was removed, preventing various metadata being attached to the event.
# 1. The 'docker_no_time' parser drops the time key and converts it into the following message:
#       {"log": "...\n", "stream":"..."}
# 2. The 'parser' filter applies the 'trim_newline' parser, trimming the newline and overrides the log key.
#       {"log": "...", "stream": "..."}
#    This was done to compensate for the newline that is appended to each line in the output plugin.
# 3. Finally the file output plugin uses the '{log}' template to only write back the contents of the log key,
#    effectively avoiding all the extra metadata that was added through this pipeline.
#       "..."

install() {

    if helm list | grep $NAME > /dev/null; then
        echo "Detected existing installation. Exiting."
        exit 0
    fi
    helm repo add fluent https://fluent.github.io/helm-charts > /dev/null
    # The claim used to persist the logs. Required for all installations.
    cat << EOF | kubectl apply --wait -n=$NAMESPACE -f -
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: $PVC_NAME
spec:
  storageClassName: $FLUENT_BIT_STORAGE_CLASS
  accessModes:
    - ReadWriteMany
  resources:
     requests:
       storage: ${FLUENT_BIT_PVC_CAPACITY}Gi
EOF

    args=(
        --set config.service="$FLUENT_BIT_SERVICE"
        --set config.outputs="$FLUENT_BIT_OUTPUTS"
        --set config.filters="$FLUENT_BIT_FILTERS"
        --set config.inputs="$FLUENT_BIT_INPUTS"
        --set config.customParsers="$FLUENT_BIT_PARSERS"
        --set extraVolumeMounts[0].name=$VOLUME_NAME
        --set extraVolumeMounts[0].mountPath=$MOUNT_PATH
        --set extraVolumes[0].name=$VOLUME_NAME
        --set extraVolumes[0].persistentVolumeClaim.claimName=$PVC_NAME
    )

    # In the case where container logs are not stored/forwarded to the default
    # directory (/var/lib/docker/containers), mounting the location where they are is required.
    if [ ! -z "$FLUENT_BIT_HOST_LOGS_PATH" ]; then
        args+=(--set extraVolumeMounts[1].name=$HOST_LOGS)
        args+=(--set extraVolumeMounts[1].mountPath=$FLUENT_BIT_HOST_LOGS_PATH)
        args+=(--set extraVolumeMounts[1].readOnly=true)
        args+=(--set extraVolumes[1].name=$HOST_LOGS)
        args+=(--set extraVolumes[1].hostPath.path=$FLUENT_BIT_HOST_LOGS_PATH)
    fi

    helm install $NAME fluent/fluent-bit "${args[@]}" \
        --set image.tag=$FLUENT_BIT_IMAGE_TAG \
        -n=$NAMESPACE

    apply_logrotate_configmap
    apply_logrotate_deployment
}

uninstall() {
    set -e
    local response=$(helm delete $NAME -n=$NAMESPACE)
    local return_status=$?
    # If 'helm delete' fails, do not force an error response if it contains 'not found'.
    if [ $return_status -eq 1 ] && [[ ! $response =~ "not found" ]]; then
        echo $response
        exit 1
    fi
    kubectl delete pod -l app=$FLUENT_BIT_DEPLOYMENT -n=$NAMESPACE --ignore-not-found --wait
    kubectl delete deployment $FLUENT_BIT_DEPLOYMENT -n=$NAMESPACE --ignore-not-found --wait
    kubectl delete configmap $CONFIG_MAP_NAME -n=$NAMESPACE --ignore-not-found --wait
    if [ "$KEEP_PVC" = false ]; then
        kubectl delete pvc $PVC_NAME -n=$NAMESPACE --ignore-not-found=true --wait
    fi
    set +e
    echo "Uninstallation successful."

    return 0
}

info() {
    echo -e "Usage: $0 [CMD] [OPTION...]>"
    echo -e ""
    echo -e "install: Deploys a configured fluent-bit deployment for some PravegaCluster."
    echo -e "\t-h=*|--host-path=*:           Creates a new mount point to provide access to the logs on the host node. default: ''"
    echo -e "\t                              Specify this when the logs on the host node are not stored at: /var/lib/docker/container/..."
    echo -e "\t-s=*|--storageclass=*:        The storageclass used to provision the PVC. default: standard"
    echo -e "\t-c=*|--pvc-capacity=*:        The size of the PVC (in GiB) to provision. default: 50"
    echo -e "\t-r=*|--pvc-reclaim-percent=*: The percent of space on the PVC to reclaim upon a reclaimation attempt. default: 25"
    echo -e "\t-t=*|--pvc-reclaim-trigger=*: The percent utilization upon which to trigger a reclaimation. default: 95"
    echo -e "\t-i=*|--rotation-interval=*:   The interval (in seconds) at which to run the rotation. default: 10"
    echo -e ""
    echo -e "uninstall: Removes any existing Pravega fluent-bit deployment."
    echo -e ""
    echo -e "fetch-all-logs: Copies all logs currently persisted in the PVC."
    echo -e "fetch-namespace-logs Copies all logs emitted by pods in the specified namespace currently persisted in the PVC."
    echo -e "fetch-system-test-logs: Copies log files from all system test pods in a given namespace."
    echo -e "fetch-pod-logs: Copies log from active pods currently running in the given namespac."
    echo -e "\t-n=*|--namespace=*:          The namespace of the PravegaCluster/Pods. default: default"
    echo -e "\t-p=*|--export-path=*:        The path to save the logs to. default: /tmp/pravega-logs"
    echo -e ""
    echo -e "help: Displays this message."
}

case $CMD in
    install)
        install
        ;;
    uninstall)
        uninstall
        ;;
    fetch-namespace-logs)
        dest="$FLUENT_BIT_EXPORT_PATH"
        namespace="${1:-$NAMESPACE}"
        fetch_stored_logs "$dest" "$namespace"
        ;;
    fetch-all-logs)
        dest="$FLUENT_BIT_EXPORT_PATH"
        fetch_stored_logs "$dest"
        ;;
    fetch-pod-logs)
        dest="$FLUENT_BIT_EXPORT_PATH"
        pods=$(kubectl get pods -n=$NAMESPACE -o custom-columns=:.metadata.namespace,:.metadata.name --no-headers)
        fetch_active_logs "$dest" "$pods"
        ;;
    fetch-system-test-logs)
        dest="$FLUENT_BIT_EXPORT_PATH"
        pods=$(kubectl get pods -l 'app=pravega-system-tests' -n=$NAMESPACE -o custom-columns=:.metadata.namespace,:.metadata.name --no-headers)
        fetch_active_logs "$dest" "$pods"
        ;;
    help|--help)
        info
        ;;
    *)
        set +u
        echo -e "'$CMD' is an invalid command.\n"
        info
        set -u
        exit
        ;;
esac
