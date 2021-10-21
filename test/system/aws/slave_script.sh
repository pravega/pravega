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
pravegaOrg=${1:-pravega}
branch=${2:-master}
sudo cp /home/ubuntu/.ssh/authorized_keys /root/.ssh/
sudo apt-get update
sudo apt-get -y install apt-transport-https  ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo apt-key fingerprint 0EBFCD88
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt-get update
apt-cache madison docker-ce
sudo apt-get -y install docker-ce=17.09.0~ce-0~ubuntu 
cd /tmp && git clone https://github.com/$pravegaOrg.git && cd pravega/
git checkout $branch
sudo add-apt-repository ppa:openjdk-r/ppa -y && sudo apt-get -y update && sudo apt-get install -y openjdk-8-jdk
sudo chmod 777 /var/run/docker.sock
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 && sudo ./gradlew --warn buildPravegaImage -PpravegaBaseTag=nautilus/pravega -PpravegaVersion=$branch  && sudo ./gradlew --warn buildBookkeeperImage -PbookkeeperBaseTag=nautilus/bookkeeper -PpravegaVersion=$branch
sudo apt -y install awscli
