#!/bin/bash
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
ACCESSKEY=${1:-null}
SECRETKEY=${2:-null}
RUNNAME=${3:-null}
COMMIT=${4:-null}
host="$(hostname)"

####copy all container logs in a given host.
containersResult="$(docker ps -a -q|xargs)"
echo "Containers in $host are ${containersResult}"
containers=(${containersResult/%$'\r'/})
for container in "${containers[@]}"
do
   echo "Executing commands for $container"
   mkdir -p logs/$container
   docker cp $container:/opt/pravega/logs/. logs/$container/
   if [ $? -ne 0 ]; then
     echo "$container is not a controller/pravega node"
     continue
   fi
   tar -zcvf logs_$container.tar.gz ./logs/$container/

done
index=1
find / -name logs |grep "/merged/opt/pravega/logs"| while read -r path ; do
    echo "Copying logs from $path $index"
    mkdir -p logs/$host-container-$index
    cp -r $path/. logs/$host-container-$index/
    index=$((index+1))
done
if [ -d logs ]; then
  tar -zcvf logs_$host.tar.gz logs/
else
  echo "No pravega containers running on $host"
fi
#### copy all test logs on the given mesos host.
find / -name 'server.log' -print0 | tar -czvf testLogsIn-$host.tar.gz --null -T -

echo "Uploading logs to s3 bucket:pravega-systemtests-logs"
for file in ./*.tar.gz; do  sudo AWS_ACCESS_KEY_ID=$ACCESSKEY AWS_SECRET_ACCESS_KEY=$SECRETKEY AWS_DEFAULT_REGION=us-east-2  aws s3 cp $file  s3://pravega-systemtests-logs/$RUNNAME/$COMMIT/; done
echo "Logs upload completed"
