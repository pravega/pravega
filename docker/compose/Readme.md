<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Containerized Deployment Using Docker Compose

Docker compose can be used to quickly spin up a Pravega cluster to use for testing or development. Unlike 
`pravega-standalone`, a Compose cluster will use a real standalone HDFS, Zookeeper and BookKeeper, and will also run
Segment Store and Controller in separate processes.

## Running

1. Ensure that you have Docker and Docker Compose installed.

2. Clone Pravega repository to fetch the code.

   ```bash
   git clone https://github.com/pravega/pravega.git
   ```

3. Navigate to the directory containing Docker Compose configuration `.yml` files.

   ```bash
   cd /path/to/pravega/docker/compose
   ```

4. Add HOST_IP as an environment variable with the value as the IP address of the host.

   ```bash
   export HOST_IP=<host-ip>
   ```

5. Now, run the following command to start a deployment comprising of multiple Docker containers, as specified in the
   `docker-compose.yml` file.

   ```bash
   docker-compose up -d
   ```

   If you want to use one of the other files in the directory, use the `-f` option to specify the file.

   ```bash
   docker-compose up -d -f docker-compose-nfs.yml
   ```

6. Verify that the deployment is up and running.

   ```bash
   docker-compose ps
   ```

Clients can then connect to the Controller at `<host-ip>:9090`.

