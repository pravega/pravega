#!/usr/bin/env bash

# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

set -euo pipefail

# Kubernetes State
DEPLOYMENT_NAME="pravega-logs"
CONFIG_MAP_NAME="pravega-logrotate"
PVC_NAME=pravega-log-sink
VOLUME_NAME=logs

NAMESPACE=${NAMESPACE:-"default"}
HOST_LOGS=host-logs
MOUNT_PATH=/data
CONFIG_MAP_DATA=/etc/config
KEEP_PVC=false
NAME=${NAME:-"pravega-fluent-bit"}

# Configurable flag parameters.
FLUENT_IMAGE_REPO=${FLUENT_IMAGE_REPO:-"fluent/fluent-bit"}
FLUENT_BIT_IMAGE_TAG=${FLUENT_BIT_IMAGE_TAG:-"latest"}
FLUENT_BIT_STORAGE_CLASS=${FLUENT_BIT_STORAGE_CLASS:-"standard"}
FLUENT_BIT_PVC_CAPACITY=${FLUENT_BIT_PVC_CAPACITY:-50}
FLUENT_BIT_RECLAIM_PERCENT=${FLUENT_BIT_RECLAIM_PERCENT:-25}
FLUENT_BIT_RECLAIM_TRIGGER_PERCENT=${FLUENT_BIT_RECLAIM_TRIGGER_PERCENT:-95}
FLUENT_BIT_ROTATE_INTERVAL_SECONDS=${LOG_ROTATE_INTERVAL:-10}
# The location on the underlying node where the container logs are stored.
FLUENT_BIT_HOST_LOGS_PATH=${FLUENT_BIT_HOST_LOGS_PATH:-""}
FLUENT_BIT_ROTATE_THRESHOLD_BYTES=${FLUENT_BIT_ROTATE_THRESHOLD_BYTES:-10000000}
FLUENT_BIT_EXPORT_PATH=${FLUENT_BIT_EXPORT_PATH:-"/tmp/pravega-logs"}
LOG_ROTATE_OLD_DIR=${LOG_ROTATE_OLD_DIR:-"."}
LOG_ROTATE_CONF_PATH=$CONFIG_MAP_DATA/"logrotate.conf"
LOG_EXT="gz"

####################################
#### Flags/Args Parsing
####################################

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
        FLUENT_BIT_RECLAIM_PERCENT="${i#*=}"
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
    *)
        echo -e "\n${i%=*} is an invalid option, or is not provided a required value.\n"
        usage
        exit
        ;;
esac
done

# 'fetch_logs' requires the log files are named according to the '<pod-name>_<namespace>_<container-name>-<container-id>.log'
# convention, which is done by default via the tag expansion in the [INPUT] stanza.
fetch_logs() {
    if [ ! -d $FLUENT_BIT_EXPORT_PATH ]; then
        mkdir -p $FLUENT_BIT_EXPORT_PATH
    fi
    pods=$@
    pravega_log_pod=$(kubectl get pods -l "app=$DEPLOYMENT_NAME" -o custom-columns=:.metadata.name --no-headers)
    logs=$(kubectl exec $pravega_log_pod -- ls "$MOUNT_PATH")
    # For now, assume that each log will have a prefix of 'kube.var.log.containers'.
    for pod in ${pods[@]}; do
        matches=$(echo "$logs" | grep "${pod}_$NAMESPACE")
        for match in ${matches[@]}; do
            kubectl cp $pravega_log_pod:$MOUNT_PATH/$match $FLUENT_BIT_EXPORT_PATH/$match -n=$NAMESPACE
        done;
    done
}

# Fetches logs from the pods of the BookKeeperCluster, PravegaCluster and ZooKeeperCluster.
fetch_pravega_logs() {
    pods=$(kubectl get pods -l "app in (zookeeper,bookkeeper-cluster,pravega-cluster)" -n=$NAMESPACE -o custom-columns=:.metadata.name --no-headers)
    fetch_logs "$pods"
}

# Fetches logs of *all* pods in the given namespace.
fetch_all_logs() {
    pods=$(kubectl get pods -n="$NAMESPACE" -o custom-columns=:.metadata.name --no-headers)
    fetch_logs "$pods"
}

# Fetches logs of all system test pods in the given namespace.
fetch_system_test_logs() {
  pods=$(kubectl get pods -n="$NAMESPACE" -l app='pravega-system-tests' -o custom-columns=:.metadata.name --no-headers)
  fetch_logs "$pods"
}

#################################
# Fluent Bit Configuration
#################################

# See information about tag expansion: https://docs.fluentbit.io/manual/pipeline/filters/kubernetes
FLUENT_BIT_INPUTS=$(cat << EOF
[INPUT]
    Name tail
    Path /var/log/containers/*.log
    Parser docker_no_time
    Tag kube.*
    Skip_Long_Lines Off
    Mem_Buf_Limit 100MB

EOF
)

# Two levels of escaping are required: one for the assignment to this variable,
# and another during expansion in the --set flag.
#
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

LOG_ROTATE_CONF=$(cat << EOF
"$MOUNT_PATH/*.log" {
    compress
    copytruncate
    size $FLUENT_BIT_ROTATE_THRESHOLD_BYTES
    rotate 1000
    dateext
    dateformat -%s
}
EOF
)

# Escape all the non-configurable variables to avoid unintended command substitutions or variables expansions.
LOG_ROTATE_WATCH=$(cat << EOF
#!/bin/ash

mebibyte=$((1024**2))

used_kib() {
    echo \$(du -s $LOG_ROTATE_OLD_DIR | cut -f 1)
}

# Makes the assumption that rate of deletion will be never be lower than rate of accumulation.
# * Currently only compressed files are deleted. Should we also delete uncompressed (in the case of
#   many small files that are smaller than threshold)?
reclaim() {
    start_kib=\$(used_kib)
    total_kib=\$(($FLUENT_BIT_PVC_CAPACITY * \$mebibyte))
    threshold_kib=\$(((\$total_kib * $FLUENT_BIT_RECLAIM_TRIGGER_PERCENT)/100))
    if [ \$start_kib -lt \$threshold_kib ]; then
        return 0
    fi
    target_kib=\$((\$start_kib - (\$total_kib * $FLUENT_BIT_RECLAIM_PERCENT)/100))
    files=\$(ls -tr $LOG_ROTATE_OLD_DIR | grep .$LOG_EXT)
    for file in \$files; do
        if [ \$(used_kib) -gt \$target_kib ]; then
            echo "Removing $LOG_ROTATE_OLD_DIR/\$file"
            rm $LOG_ROTATE_OLD_DIR/\$file
        else
            break
        fi
    done
    end_kib=\$(used_kib)
    if [ \$start_kib -gt \$end_kib ]; then
        kib=\$((start_kib - end_kib))
        utilization=\$(((\$end_kib * 100)/\$total_kib))
        echo "Reclaimed a total of \$((\$kib/\$mebibyte))GiB (\${kib}KiB). Total: \$total_kib Used: \$end_kib (\${utilization}%)"
    fi
}

# This function assumes the '-%s' dateformat will be applied. It transforms any files in the '$LOG_ROTATE_OLD_DIR'
# directory in the '<logname>.log-<epoch>.gz' format to '<logname>-<utc>.log.gz'.
rename() {
  suffix=".log"
  name=\$1
  match=\$(echo \$name | grep -oE "\-[0-9]+\.$LOG_EXT\$")
  if [ \$? -eq 0 ]; then
    epoch="\$(echo \$match | grep -oE '[0-9]+')"
    utc=\$(date --utc +%FT%TZ -d "@\$epoch")
    original=\${name%\$match}
    echo "Renaming \$name -> \${original%\$suffix}-\$utc\$suffix.$LOG_EXT"
    mv \$name "\${original%\$suffix}-\$utc\$suffix.$LOG_EXT"
  fi
}

# 'orphans' handles the case where a log file was produced by an earlier container for a given pod that was unable to be
# compressed. This can occur if a container in a pod (or the pod itself) has been restarted and there are logs 'leftover'
# that will never receive anymore appends and should be compressed.
orphans() {
    count=0
    files=\$(stat $MOUNT_PATH/$LOG_ROTATE_OLD_DIR/*.log -t -c=%n  | sed 's/=//g')
    for file in \$files; do
        # File may have been moved.
        if [ ! -f "\$file" ]; then
            continue
        fi
        prefix=\${file%-*.log}
        # Files with a shared prefix.
        shared=\$(stat "\$prefix"*.log -t -c="%Y,%n" | sed 's/=//g' | sort)
        previous=''
        # shared will contain a list of files with the same prefix sorted (by time) in ascending order.
        # The most recent file with said prefix is not compressed, in case it is actively being written to.
        for current in \$shared; do
            if [ -n "\$previous" ]; then
                : \$((count+=1))
                epoch=\${previous%%,*}
                name=\${previous#*,}
                # Compress and redirect to file as if it was compressed by logrotate.
                echo "Compressing \$name -> \$name.log-\$epoch.gz"
                gzip -c \$name > "\$name.log-\$epoch.gz"
                rm \$name
                fi
            previous="\$current"
        done
    done
    if [ \$count -gt 0 ]; then
        echo "Found \$count inactive logs -- compressing."
    fi
}


watch() {
    # Permissions of containing directory changed to please logrotate.
    chmod o-wx .
    mkdir -p $MOUNT_PATH/$LOG_ROTATE_OLD_DIR

    while true; do
        start=\$(date +%s%3N)
        logrotate $LOG_ROTATE_CONF_PATH
        # Catch any 'orphans' -- those .log files that are from an old/restarted containers and no longer
        # will receive any new appends.
        orphans
        # Rotated but not renamed (i.e. the files that were *just* rotated).
        rotated=\$(stat $MOUNT_PATH/$LOG_ROTATE_OLD_DIR/*.$LOG_EXT -t -c=%n 2>/dev/null | sed  's/=//' | grep -E "\\-[0-9]+.$LOG_EXT\$")
        for name in \$rotated; do
            rename "\$name"
        done;
        reclaim
        end=\$(date +%s%3N)
        if [ -n "\$rotated" ]; then
            echo "Rotation cycle completed in \$((\$end-\$start)) milliseconds."
        fi
        sleep $FLUENT_BIT_ROTATE_INTERVAL_SECONDS
    done
}

cmd=\$1
case \$cmd in
    nop)
        :
        ;;
    *)
        watch
        ;;
esac

EOF
)

# Write the above script to a file to ensure its validity after expansion.
validate_watcher() {
    tmp="/tmp/watcher.sh"
    echo "$LOG_ROTATE_WATCH" > $tmp
    chmod +x $tmp
    if ! $tmp nop; then
        echo "Error validating the fluent-bit-watcher."
        exit 1
    fi
    rm $tmp
}

apply_logrotate_configmap() {
    tab='    '
    # Apply required indentation by prepending eight spaces.
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
  name: $DEPLOYMENT_NAME
  labels:
    app: $DEPLOYMENT_NAME
spec:
  replicas: 1
  selector:
    matchLabels:
      app: $DEPLOYMENT_NAME
  template:
    metadata:
      labels:
        app: $DEPLOYMENT_NAME
    spec:
      containers:
      - name: alpine
        image: alpine
        workingDir: $MOUNT_PATH
        command: [ '/bin/ash', '-c' ]
        args:
          - apk add logrotate;
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
        kubectl delete deployment $DEPLOYMENT_NAME -n=$NAMESPACE --ignore-not-found
        kubectl delete configmap $CONFIG_MAP_NAME -n=$NAMESPACE --ignore-not-found
        if [ "$KEEP_PVC" = false ]; then
            kubectl delete pvc $PVC_NAME -n=$NAMESPACE --ignore-not-found=true
        fi
        set +e
    }

usage() {
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
        if ! validate_watcher; then
            exit 1
        fi
        install
        ;;
    uninstall)
        uninstall
        ;;
    fetch-logs|fetch-pravega-logs)
        fetch_pravega_logs
        ;;
    fetch-system-test-logs)
        fetch_system_test_logs
        ;;
    fetch-all-logs)
        fetch_all_logs
        ;;
    help|--help)
        usage
        ;;
    *)
        set +u
        echo -e "'$CMD' is an invalid command.\n"
        usage
        set -u
        exit 1
        ;;
esac