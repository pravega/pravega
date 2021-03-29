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

set -eo pipefail

DIR=/opt/pravega
SCRIPTS_DIR=${DIR}/scripts

source ${SCRIPTS_DIR}/common.sh
source ${SCRIPTS_DIR}/init_controller.sh
source ${SCRIPTS_DIR}/init_segmentstore.sh
source ${SCRIPTS_DIR}/init_standalone.sh
source ${SCRIPTS_DIR}/init_tier2.sh
source ${SCRIPTS_DIR}/init_kubernetes.sh

if [ ${WAIT_FOR} ];then
    ${SCRIPTS_DIR}/wait_for
fi

case $1 in
controller)
    init_controller
    exec /opt/pravega/bin/pravega-controller
    ;;
segmentstore)
    init_kubernetes
    init_tier2
    init_segmentstore
    exec /opt/pravega/bin/pravega-segmentstore
    ;;
standalone)
    init_standalone
    exec /opt/pravega/bin/pravega-standalone
    ;;
*)
    echo "Usage: $0 (controller|segmentstore|standalone)"
    exit 1
    ;;
esac
