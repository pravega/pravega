<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Marathon Deployment

`PravegaGroup.json` is sample marathon group, which can be used to quickly spin up a scalable DC/OS Pravega cluster.
It includes a single node ZooKeeper and HDFS, which are suitable for testing/development, but should be replaced
for production use.

## Deploying

```
curl -X POST http://master.mesos:8080/v2/groups \
     -H "Content-Type: application/json" \
     -d @PravegaGroup.json
```
