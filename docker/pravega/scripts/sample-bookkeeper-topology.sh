#!/usr/bin/env sh
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

# In this exemple, we treat each host as a rack (for test-only purposes). This is being done by taking the last octet
# in the datanode's IP and prepending it with the word '/datacenter/rack-'.
echo $@ | xargs -n 1 | awk -F '.' '{print "/datacenter/rack-"$NF}'