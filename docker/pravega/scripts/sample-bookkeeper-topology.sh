#!/usr/bin/env sh
#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

# In this exemple, we treat each host as a rack (for test-only purposes). This is being done by taking the last octet
# in the datanode's IP and prepending it with the word '/datacenter/rack-'.
echo $@ | xargs -n 1 | awk -F '.' '{print "/datacenter/rack-"$NF}'