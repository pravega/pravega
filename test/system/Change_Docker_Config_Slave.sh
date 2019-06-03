#!/bin/bash
#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
set -e
sed -i 's/"live-restore": true,/"live-restore": false,/' /etc/docker/daemon.json
cat /etc/docker/daemon.json
service docker stop
service docker start
exit
