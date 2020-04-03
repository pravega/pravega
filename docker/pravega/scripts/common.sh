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

# Adds a system property if the value is not empty
add_system_property() {
    local name=$1
    local value=$2

    if [ -n "${value}" ]; then
        export JAVA_OPTS="${JAVA_OPTS} -D${name}=${value}"
    fi
}

# Add system property for ECS configUri with ECS credentials
add_system_property_ecs_config_uri() {
    local name=$1
    local configUri=$2
    local identity=$3
    local secret=$4

    if ! echo ${configUri} | grep -q "identity"; then
        configUri=${configUri}"%26identity="${identity}"%26secretKey="${secret}
    fi

    echo "${name}" "${configUri}"
    add_system_property "${name}" "${configUri}"
}
