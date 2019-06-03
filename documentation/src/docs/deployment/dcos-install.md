<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Deploying on DC/OS

Prerequisities: DC/OS cli needs to be installed. To install the cli, follow the instructions here: https://docs.mesosphere.com/1.8/usage/cli/install/

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
