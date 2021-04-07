#!/usr/bin/env bash
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

set -euo pipefail

TAR_NAME="pravega-logs-export.tar"
CONFIG_MAP_NAME="pravega-logrotate"
PVC_NAME=pravega-log-sink
VOLUME_NAME=logs
HOST_LOGS=host-logs
MOUNT_PATH=/data
CONFIG_MAP_DATA=/etc/config
KEEP_PVC=false
LOG_EXT="gz"

# Configurable flag parameters.
FLUENT_BIT_DEPLOYMENT=${FLUENT_BIT_DEPLOYMENT:-"pravega-logs"}
FLUENT_IMAGE_REPO=${FLUENT_IMAGE_REPO:-"fluent/fluent-bit"}
FLUENT_BIT_IMAGE_TAG=${FLUENT_BIT_IMAGE_TAG:-"latest"}
FLUENT_BIT_STORAGE_CLASS=${FLUENT_BIT_STORAGE_CLASS:-"standard"}
FLUENT_BIT_PVC_CAPACITY=${FLUENT_BIT_PVC_CAPACITY:-50}
FLUENT_BIT_RECLAIM_TARGET_PERCENT=${FLUENT_BIT_RECLAIM_TARGET_PERCENT:-75}
FLUENT_BIT_RECLAIM_TRIGGER_PERCENT=${FLUENT_BIT_RECLAIM_TRIGGER_PERCENT:-95}
FLUENT_BIT_ROTATE_INTERVAL_SECONDS=${LOG_ROTATE_INTERVAL:-10}
FLUENT_BIT_HOST_LOGS_PATH=${FLUENT_BIT_HOST_LOGS_PATH:-""}
FLUENT_BIT_ROTATE_THRESHOLD_BYTES=${FLUENT_BIT_ROTATE_THRESHOLD_BYTES:-10000000}
FLUENT_BIT_EXPORT_PATH=${FLUENT_BIT_EXPORT_PATH:-"/tmp/pravega.logs"}
LOG_ROTATE_CONF_PATH=${LOG_ROTATE_CONF_PATH:-$CONFIG_MAP_DATA/"logrotate.conf"}
NAMESPACE=${NAMESPACE:-"default"}
NAME=${NAME:-"pravega-fluent-bit"}

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
esac
done

logs_fetched=1
cp_log() {
    local log_pod=$1
    local file=$2
    file=${file#./}
    # The leading ./ must be parsed out in the src file as well as the ':' from the dest.
    local output_file="$(echo $file | sed 's/:/-/g')"
    # If we needed to create a directory for this collection, make sure to clean it up.
    kubectl cp "$pravega_log_pod:$MOUNT_PATH/$file" "$output_file" -n=$NAMESPACE > /dev/null
    echo "$file ($logs_fetched/$total)"
    tar -rf "$TAR_NAME" "$output_file" 2> /dev/null
    rm "$output_file"
}
######################################
# Fetches the gathered logs from the $FLUENT_BIT_DEPLOYMENT pod.
# Globals:
#   MOUNT_PATH
#   NAMESPACE
# Arguments:
#   The list of pod names to gather the logs from.
# Outputs:
#   Set of log files downloaded to $FLUENT_BIT_EXPORT_PATH.
# Note:
# 'fetch_logs' requires the log files are named according to the
# '<namespace>.<pod-name>.<container-name>-<container-id>.log' convention, which is done by default
# via the tag expansion in the [INPUT] stanza.
######################################
fetch_fluent_logs() {
    local output=$1; shift
    local total=0
    if [ ! -d "$output" ]; then
        echo "$output directory does not exist!"
        exit 1
    fi
    pravega_log_pod=$(kubectl get pods -n=$NAMESPACE -l "app=$FLUENT_BIT_DEPLOYMENT" -o custom-columns=:.metadata.name --no-headers)
    # Prematurely rotate the logs to receive the most up to date log set.
    kubectl exec $pravega_log_pod -n=$NAMESPACE -- /etc/config/watch.sh force

    pods=$@
    log_files=()
    logs=$(kubectl exec $pravega_log_pod -- find . -mindepth 2 -name '*.gz')
    # For all logs that exist on the PVC, find the logs belonging to the inputted pods.
    for pod in $pods; do
        tag=${pod##*-}
        if echo "$logs" | grep "$NAMESPACE/${pod%-$tag}/$tag" > /dev/null; then
            matches=$(echo "$logs" | grep "$NAMESPACE/${pod%-$tag}/$tag")
            for match in $matches; do
                log_files+=("$match")
                ((total+=1))
            done;
        fi
    done

    pushd "$output" > /dev/null
    # Query logs from pravega-log-pod and archive them.
    rm -rf "$TAR_NAME"{.gz,}
    tar -cf "$TAR_NAME" --files-from=/dev/null
    for file in "${log_files[@]}"; do
        cp_log "$pravega_log_pod" "$file" &
        ((logs_fetched+=1))
    done
    wait

    rm -rf "$NAMESPACE"
    gzip "$TAR_NAME"
    popd > /dev/null
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
    Name          rewrite_tag
    Match         kube.*
    Rule          \$log .* \$kubernetes['namespace_name'].\$kubernetes['pod_name'].\$kubernetes['container_name'].\$kubernetes['docker_id'].log false
    Emitter_Name  re_emitted

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
    compress
    copytruncate
    size $FLUENT_BIT_ROTATE_THRESHOLD_BYTES
    rotate 1000
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
LOG_ROTATE_WATCH=$(cat << EOF
#!/usr/bin/env bash

set -e

kilobyte=1024
megabyte=$((1024**2))
gigabyte=$((1024**3))

used_kib() {
    du -s $MOUNT_PATH | cut -f 1
}

# Brings down the current PVC utilization to FLUENT_BIT_RECLAIM_TARGET_PERCENT by deleting the oldest compressed log files.
# Makes the assumption that rate of deletion will be never be lower than rate of accumulation.
reclaim() {
    # This must be a list of file paths in sorted order.
    total_kib=\$(($FLUENT_BIT_PVC_CAPACITY * megabyte))
    threshold_kib=\$(((total_kib * $FLUENT_BIT_RECLAIM_TRIGGER_PERCENT)/100))
    if [ "\$(used_kib)" -lt "\$threshold_kib" ]; then
        return 0
    fi
    target_kib=\$(((total_kib * $FLUENT_BIT_RECLAIM_TARGET_PERCENT)/100))
    # Search all log folders recursively and return results from oldest to youngest.
    local compressed=\$(find . -mindepth 2 -regextype posix-extended -regex '.*/*.$LOG_EXT\$' -printf "%T@ %P\n")
    compressed="\$(echo "\$compressed" | sort -n | grep -oE '[^ ]*$')"

    local reclaimed=0
    for file in \$compressed; do
        if [ "\$(used_kib)" -gt "\$target_kib" ]; then
            echo "Removing - \$file"
            bytes=\$(stat "\$file" -c=%s | sed 's/=//g')
            ((reclaimed+=bytes))
            rm "\$file"
        else
            break
        fi
    done
    end_kib=\$(used_kib)
    if [ "\$reclaimed" -gt 0 ]; then
        utilization=\$(((end_kib * 100)/total_kib))
        echo "Reclaimed a total of \$((reclaimed/gigabyte))GiB (\$((reclaimed/megabyte))MiB). Total(MiB): \$((total_kib/kilobyte)) , Used: \$((end_kib/kilobyte)) (\${utilization}%)"
    fi
    set +x
}

# This function assumes the '-%s' dateformat will be applied. It transforms any files in the '$MOUNT_PATH'
# directory in the '<logname>.log-<epoch>.gz' format to '<logname>-<utc>.log.gz'.
epoch_to_utc() {
  suffix=".log"
  name=\$1
  match=\$(echo "\$name" | grep -oE "\-[0-9]+\.$LOG_EXT\$")
  if [ \$? -eq 0 ]; then
    epoch="\$(echo \$match | grep -oE '[0-9]+')"
    utc=\$(date --utc +%FT%TZ -d "@\$epoch")
    original=\${name%\$match}
    dest="\${original%\$suffix}-\$utc\$suffix.$LOG_EXT"
    mv "\$name" "\$dest"
  fi
}

# Catch any 'orphans' -- those .log files that are from an old/restarted container and no longer
# will receive any new appends.
orphans() {
    count=0
    for file in \$@; do
        # File may have been moved.
        if [ ! -f "\$file" ]; then
            continue
        fi
        local prefix=\${file%.*.log}
        # Files with a shared prefix. Redirect errors in case no files exist.
        set +e
        local shared=\$(stat "\$prefix"*.log -t -c="%Y,%n" | sed 's/=//g' | sort -n)
        set -e
        local previous=''
        # shared will contain a list of files with the same prefix sorted (by time) in ascending order.
        # The most recent file with said prefix is not compressed, in case it is actively being written to.
        for current in \$shared; do
            if [ -n "\$previous" ]; then
                : \$((count+=1))
                local epoch=\${previous%%,*}
                local name=\${previous#*,}
                # Compress and redirect to file as if it was compressed by logrotate.
                echo "Compressing \$name -> \$name-\$epoch.gz"
                gzip -c \$name > "\$name-\$epoch.gz"
                rm \$name
            fi
            previous="\$current"
        done
    done
    if [ \$count -gt 0 ]; then
        echo "Found \$count inactive logs -- compressing."
    fi
}

# Will move files in the format produced by logrotate (and 'orphans') (<logname>.log-<epoch>.gz) and arranges a log file
# (where <logname> can be expanded into <namespace>.<pod_name-tag>.<container_name>.<container_id>)
# in this structure:
#
#   namespace/
#   └── pod_name/
#       └── <tag-container_name>-<substr(container_id, 0, 7)>.log-<epoch>.gz
#
# Todo: Return list of files moved to use for 'epoch_to_utc'.
redirect() {
    for src in \$@; do
        local path=\${src%.log*}
        local ext=\${src#\$path.}
        local split=\$(echo \$path | sed 's/\./ /g')
        read namespace pod container id <<< "\$split"
        local tag=\${pod##*-}
        local pod=\${pod%-\$tag}
        local dest="\$namespace/\$pod/\$tag-\$container-\$(printf "%.8s" "\$id").\$ext"
        mkdir -p "\$(dirname "\$dest")"
        mv "\$src" "\$dest"
    done
}

rotate() {
    (
        flock 200 || exit 1
        if [  "$PWD" != "$MOUNT_PATH" ]; then
            cd "$MOUNT_PATH"
        fi
        opts=\$1
        start_time=\$(date +%s%3N)
        if [ "\$opts" = "--force" ]; then
            echo "Received --force logrotate option."
        fi
        logrotate $LOG_ROTATE_CONF_PATH \$opts
        local files=\$(find . -maxdepth 1 -regextype posix-extended -regex '.*/*.log\$' -printf "%T@ %P\n" | sort -n | grep -oE '[^ ]*$')
        orphans "\${files[@]}"
        # Orphans can create newly compressed files, so we must update state.
        files=\$(find . -maxdepth 1 -regextype posix-extended -regex '.*/*.$LOG_EXT\$' -printf "%T@ %P\n")
        files=\$(echo "\$files" | sort -n | grep -oE '[^ ]*$')
        redirect "\${files[@]}"
        ## Rotated but not renamed (i.e. the files that were *just* rotated).
        rotated=\$(find . -mindepth 2 -regextype posix-extended -regex '.*/*[0-9]{10}.$LOG_EXT\$' -printf "%p\n")
        for name in \$rotated; do
            epoch_to_utc "\$name"
        done;
        # Attempt to reclaim any old log files that have been 'redirected' (those not just rotated).
        reclaim
        end_time=\$(date +%s%3N)
        if [ -n "\$rotated" ]; then
            echo "Rotation cycle completed in \$((end_time-start_time)) milliseconds."
        fi
    ) 200> /var/lock/rotate.lock
}

watch() {
    # Permissions of containing directory changed to please logrotate.
    chmod o-wx .
    mkdir -p $MOUNT_PATH

    while true; do
        rotate
        sleep $FLUENT_BIT_ROTATE_INTERVAL_SECONDS
    done
}

cmd=\$1
case \$cmd in
    nop)
        :
        ;;
    force)
        rotate '--force'
        ;;
    *)
        watch
        ;;
esac

EOF
)

# Writes the variable to a file and executes a NOP, ensuring that its syntax is valid before
# deploying it to a Kubernetes cluster.
validate_watcher() {
    tmp="/tmp/watcher.sh"
    echo "$LOG_ROTATE_WATCH" > $tmp
    chmod +x $tmp
    cat $tmp
    if ! $tmp nop; then
        echo "Error validating the fluent-bit-watcher."
    else
        rm $tmp
    fi
}

apply_logrotate_configmap() {
    tab='    '
    # Apply required indentation by prepending two tabs.
    log_rotate_watch=$(echo "$LOG_ROTATE_WATCH" | sed "s/^/$tab$tab/")
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
      - name: alpine
        image: alpine
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

    if helm list | grep $NAME; then
      echo "Detected existing installation. Exiting."
      exit 0
    fi

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
    kubectl delete deployment $FLUENT_BIT_DEPLOYMENT -n=$NAMESPACE --ignore-not-found
    kubectl delete configmap $CONFIG_MAP_NAME -n=$NAMESPACE --ignore-not-found
    if [ "$KEEP_PVC" = false ]; then
        kubectl delete pvc $PVC_NAME -n=$NAMESPACE --ignore-not-found=true
    fi
    set +e
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
    echo -e "fetch-logs: Copies log files produced by the PravegaCluster (on a given namespace) to a local directory."
    echo -e "fetch-all-logs: Copies log files from *all* pods in a given namespace."
    echo -e "fetch-system-test-logs: Copies log files from *all* system test pods in a given namespace."
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
    fetch-logs)
        dest="$FLUENT_BIT_EXPORT_PATH"
        pods=$(kubectl get pods -n=$NAMESPACE -o custom-columns=:.metadata.name --no-headers)
        fetch_fluent_logs "$dest" "$pods"
        ;;
    validate)
        validate_watcher
        ;;
    info)
        info
        ;;
    help|--help)
        help
        ;;
    *)
        set +u
        echo -e "'$(error $CMD)' is an invalid command.\n"
        help
        set -u
        exit
        ;;
esac
