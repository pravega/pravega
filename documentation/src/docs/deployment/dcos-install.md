<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Deploying on DC/OS

Prerequisities: DC/OS cli needs to be installed. To install the cli, follow the instructions here: https://docs.d2iq.com/mesosphere/dcos/1.13/cli/install/

Pravega can be run on DC/OS by leveraging Marathon.  
PravegaGroup.json defines the docker hub image locations and necessary application configration to start a simple Pravega cluster.

Download  [PravegaGroup.json](https://github.com/pravega/pravega/blob/master/PravegaGroup.json) to your DC/OS cluster.  For example:
```
wget https://github.com/pravega/pravega/blob/master/PravegaGroup.json
```
Add to Marathon using:
```
dcos marathon group add PravegaGroup.json
```
