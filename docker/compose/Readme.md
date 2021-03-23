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
# Containerized Deployment Using Docker Compose

Docker Compose can be used to quickly spin up a Pravega cluster to use for testing or development. Unlike
`pravega-standalone`, a Compose cluster will use a real standalone HDFS, Zookeeper and BookKeeper, and will also run
Segment Store and Controller in separate processes.

## Running

1. Ensure that Docker and Docker Compose are installed in the host machine.

2. Clone Pravega repository to fetch the code.

   ```
   $ git clone https://github.com/pravega/pravega.git
   ```

3. Navigate to the directory containing Docker Compose configuration `.yml` files.

   ```
   $ cd /path/to/pravega/docker/compose
   ```

4. Add HOST_IP as an environment variable with the value as the IP address of the host.

   ```
   $ export HOST_IP=<HOST_IP>
   ```

5. Run the following command to start a deployment comprising of multiple Docker containers, as specified in the
   `docker-compose.yml` file.

   ```
   $ docker-compose up -d
   ```

   To use one of the other files in the directory, use the `-f` option to specify the file.

   ```
   $ docker-compose up -d -f docker-compose-nfs.yml
   ```

6. Verify that the deployment is up and running.

   ```
   $ docker-compose ps
   ```

Clients can then connect to the Controller at `<HOST_IP>:9090`.

