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
######################################

set -eo pipefail

MOUNT_PATH=/data
CONFIG_MAP_DATA=/etc/config
CONFIG_MAP_NAME="pravega-logrotate"
LOG_ROTATE_CONF_PATH=${LOG_ROTATE_CONF_PATH:-$CONFIG_MAP_DATA/"logrotate.conf"}
LOG_EXT=${LOG_EXT:-"gz"}

FLUENT_BIT_PVC_CAPACITY=${FLUENT_BIT_PVC_CAPACITY:-50}
FLUENT_BIT_RECLAIM_TARGET_PERCENT=${FLUENT_BIT_RECLAIM_TARGET_PERCENT:-75}
FLUENT_BIT_RECLAIM_TRIGGER_PERCENT=${FLUENT_BIT_RECLAIM_TRIGGER_PERCENT:-95}
FLUENT_BIT_ROTATE_INTERVAL_SECONDS=${FLUENT_BIT_ROTATE_INTERVAL_SECONDS:-10}
LOG_LEVEL=${LOG_LEVEL:-'info'}

kilobyte=1024
megabyte=$((1024**2))
gigabyte=$((1024**3))

log() {
    echo -e "[\033[1;34m $(date --utc +%FT%TZ) \033[0m] $*"
}

debug() {
    if [ "$LOG_LEVEL" = 'debug' ]; then
        echo -e "[\033[1;35m $(date --utc +%FT%TZ) \033[0m] $*"
    fi
}

items() {
    local spaces=$(echo "$*" | head -n 1 | tr -cd ' \t' | wc -c)
    if [ -z "$*" ]; then
        echo "$spaces"
    else
        echo "$((spaces+1))"
    fi
}

used_kib() {
    du -s $MOUNT_PATH | cut -f 1
}

# Brings down the current PVC utilization to FLUENT_BIT_RECLAIM_TARGET_PERCENT by deleting the oldest compressed log files.
# Makes the assumption that rate of deletion will be never be lower than rate of accumulation.
reclaim() {
    # This must be a list of file paths in sorted order.
    local total_kib=$(($FLUENT_BIT_PVC_CAPACITY * megabyte))
    local threshold_kib=$(((total_kib * $FLUENT_BIT_RECLAIM_TRIGGER_PERCENT)/100))
    local used=$(used_kib)
    if [ "$used" -lt "$threshold_kib" ]; then
        debug "Used kilobytes ($used) less than the rotation threshold ($threshold_kib)"
        return 0
    fi
    local target_kib=$(((total_kib * $FLUENT_BIT_RECLAIM_TARGET_PERCENT)/100))
    log "Reclaiming oldest log files (Used: $total_kib, Threshold: $threshold_kib, Target: $target_kib)"
    # Search all log folders recursively and return results from oldest to youngest.
    local compressed=$(find . -mindepth 2 -regextype posix-extended -regex "\.\/.*\.$LOG_EXT$" -printf "%T@ %P\n")
    compressed="$(echo "$compressed" | sort -n | grep -oE '[^ ]*$')"

    local reclaimed=0
    for file in $compressed; do
        if [ "$(used_kib)" -gt "$target_kib" ]; then
            bytes=$(stat "$file" -c=%s | sed 's/=//g')
            ((reclaimed+=bytes))
            rm "$file"
        else
            break
        fi
    done
    local end_kib=$(used_kib)
    if [ "$reclaimed" -gt 0 ]; then
        utilization=$(((end_kib * 100)/total_kib))
        log "Reclaimed a total of $((reclaimed/gigabyte))GiB ($((reclaimed/megabyte))MiB). Total(MiB): $((total_kib/kilobyte)) , Used: $((end_kib/kilobyte)) (${utilization}%)"
    fi
    set +
}

# This function assumes the '-%s' dateformat will be applied. It transforms any files in the '$MOUNT_PATH'
# directory in the '<logname>.log-<epoch>.gz' format to '<logname>-<utc>.log.gz'.
# Furthermore the logrotate epoch is also transformed to a last modified epoch.
epoch_to_utc() {
  suffix=".log"
  name=$1
  match=$(echo "$name" | grep -oE "\-[0-9]+\.$LOG_EXT$")
  if [ $? -eq 0 ]; then
    epoch="$(stat $name -c %Y)"
    utc=$(date --utc +%FT%TZ -d "@$epoch")
    original=${name%$match}
    dest="${original%$suffix}-${utc}${suffix}.$LOG_EXT"
    mv "$name" "$dest"
  fi
}

# Catch any 'orphans' -- those .log files that are from an old/restarted container and no longer
# will receive any new appends. This does not work for restarted pods in a deployment because
# instead of just the container tag changing, a five character string is also appended to the pod name.
# So even if the container id is different and the log file is older, we can't differentiate between
# a restarted pod and simply a longer lived pod.
orphans() {
    local count=0
    for file in $@; do
        # File may have been moved.
        if [ ! -f "$file" ] || [ -z "$file" ]; then
            continue
        fi
        local prefix=${file%.*.log}
        # Files with a shared prefix. Redirect errors in case no files exist.
        # local shared=$(stat "$prefix"*.log -t -c="%Y,%n" || echo '')
        local shared=$(find . -maxdepth 1 -regextype posix-extended -regex "\.\/$prefix.[a-z0-9]{64}\.log$" -printf "%T@,%P\n" | sort -n)
        local previous=''
        # shared will contain a list of files with the same prefix sorted (by time) in ascending order.
        # The most recent file with said prefix is not compressed, in case it is actively being written to.
        for current in $shared; do
            if [ -n "$previous" ]; then
                : $((count+=1))
                local epoch=${previous%%,*}
                local name=${previous#*,}
                debug "Found stale log. (Current: $file, Stale: $previous)"
                # Compress and redirect to file as if it was compressed by logrotate.
                gzip -c "$name" > "$name-$epoch.gz"
                rm "$name"
            fi
            previous="$current"
        done
    done
    if [ $count -gt 0 ]; then
        log "Found $count inactive logs -- compressing."
    fi
    count=0
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
    for src in $@; do
        local ifs=$IFS
        IFS='_'
        local log=${src%.log*}
        local ext=${src#$log.}
        log=${log#kube.var.log.containers.}
        read pod namespace container <<< "$log"
        local container_id=${container##*-}
        local container_name=${container%-*}
        local tag=${pod##*-}
        pod=${pod%-$tag}
        local dest="$namespace/$pod/$tag-$container_name-$(printf "%.8s" "$container_id").$ext"
        IFS=$ifs
        mkdir -p "$(dirname "$dest")"
        mv "$src" "$dest"
    done
}

rotate() {
    debug "Attempting to acquire /var/lock/rotate.lock ..."
    (
        if ! flock 200; then
            return 1
        fi
        if [  "$PWD" != "$MOUNT_PATH" ]; then
            cd "$MOUNT_PATH"
        fi
        opts=${1:-""}
        start_time=$(date +%s%3N)
        if [ "$opts" = "--force" ]; then
            debug "Received --force logrotate option."
        fi
        if ! ls *.log > /dev/null 2>&1; then
            debug "No log files found -- skipping rotation."
            return 0
        fi
        debug "Running logrotate."
        if ! logrotate "$LOG_ROTATE_CONF_PATH" $opts; then
            return 2
        fi
        local files=$(find . -maxdepth 1 -regextype posix-extended -regex '.*\.log$' -printf "%T@ %P\n" | sort -n | grep -oE '[^ ]*$')
        debug "Found $(items $files) '.log' files."
        if [ -n "$files" ]; then
            log "Looking for stale log files."
        fi
        orphans "$files"
        # 'orphans' can create newly compressed files, so we must update state.
        files=$(find . -maxdepth 1 -regextype posix-extended -regex ".*\.$LOG_EXT$" -printf "%T@ %P\n")
        files=$(echo "$files" | sort -n | grep -oE '[^ ]*$')
        debug "Redirecting $(items $files) recently compressed '.$LOG_EXT' files."
        redirect "$files"
        ## Rotated but not renamed (i.e. the files that were *just* rotated).
        rotated=$(find . -mindepth 2 -regextype posix-extended -regex "\.\/.*[0-9]{10}\.$LOG_EXT$" -printf "%p\n")
        debug "Found $(items $rotated) recently rotated '.log' files."
        for name in $rotated; do
            epoch_to_utc "$name"
        done;
        # Attempt to reclaim any old log files that have been 'redirected' (those not just rotated).
        reclaim
        end_time=$(date +%s%3N)
        if [ -n "$rotated" ]; then
            log "Rotation cycle completed in $((end_time-start_time)) milliseconds."
        fi
        echo ""
    ) 200> /var/lock/rotate.lock 
}

watch() {
    mkdir -p $MOUNT_PATH

    while true; do
        local start_kilo=$(used_kib)
        sleep $FLUENT_BIT_ROTATE_INTERVAL_SECONDS
        local end_kilo=$(used_kib)
        local throughput=$(((end_kilo - start_kilo)/$FLUENT_BIT_ROTATE_INTERVAL_SECONDS))
        log "Receiving logs at an estimated rate of \033[1;32m$((throughput/kilobyte))\033[0m MBps / \033[1;32m$throughput\033[0m KBps. (Start: $start_kilo, End: $end_kilo, Duration: $FLUENT_BIT_ROTATE_INTERVAL_SECONDS)"
        rotate
        local status=$?
        if [ $status -eq 1 ]; then
            log "Rotation failed -- lock already held."
        elif [ $status -eq 2 ]; then
            log "Rotation failed -- logrotate returned non-zero code."
        fi
    done
}

cmd=${1:-""}

case $cmd in
    nop)
        :
        ;;
    force)
        rotate '--force'
        ;;
    rotate)
        rotate
        ;;
    *)
        watch
        ;;
esac