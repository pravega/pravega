<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# docker-compose

Docker compose can be used to quickly spin up a Pravega cluster to use for testing or development. Unlike 
`pravega-standalone`, a compose cluster will use a real standalone HDFS, Zookeeper, BookKeeper, and will run the 
Segment Store and the Controller in separate processes.

## Running

The IP or hostname must be provided as a `HOST_IP` environment variable. Otherwise, things should just work.

`HOST_IP=1.2.3.4 docker-compose up`

Clients can then connect to the Controller at `${HOST_IP}:9090`.
