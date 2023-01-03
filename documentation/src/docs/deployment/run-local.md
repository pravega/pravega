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
# Running Pravega in Local Machine

As an alternative to running Pravega on a cluster of machines, you may run Pravega on a local/single machine. Running Pravega locally on a single host allows you to get started with Pravega quickly. Running Pravega locally is especially suitable for development and testing purposes.

You may run Pravega on local machine using either of these two options:

1. [Standalone mode](#standalone-mode) deployment: In this option, Pravega server runs in a single process and in-memory.
2. [Long Term Storage mode](#long-term-storage) deployment: In this option, Pravega server runs as a single instance on localhost or a virtual machine with streams persisted on Tier 2
3. Distributed mode [Docker Compose](#docker-compose-distributed-mode) deployment: In this option, Pravega components run on separate processes within the same host.

These options are explained in below subsections.

## Standalone Mode

In standalone mode, the Pravega server is accessible from clients through the `localhost` interface only. Controller REST APIs, however, are accessible from remote hosts/machines.

Security is off by default in Pravega. Please see [this](../security/securing-standalone-mode-cluster.md) document to find how to enable security in standalone mode.

**Note:** In order to remotely troubleshoot Java application, JDWP port is being used. The default JDWP port in Pravega(8050) can be overridden using the below command before starting Pravega in standalone mode. 0 to 65535 are allowed range of port numbers.

```
$ export JDWP_PORT=8888
```

You can launch a standalone mode server using the following options:

1. From [source code](#from-source-code)
2. From [installation package](#from-installation-package)
3. From [Docker image](#from-docker-image)

### From Source Code

Checkout the source code:

```
$ git clone https://github.com/pravega/pravega.git
$ cd pravega
```

Build the Pravega standalone mode distribution:

```
./gradlew startStandalone
```

### From Installation Package

Download the Pravega release from the [GitHub Releases](https://github.com/pravega/pravega/releases).

```
$ tar xfvz pravega-<version>.tgz
```
Download and extract either tarball or zip files. Follow the instructions provided for the tar files (same can be applied for zip file) to launch all the components of Pravega on your local machine.

Run Pravega Standalone:

```
$ pravega-<version>/bin/pravega-standalone
```


##  With Long Term Storage

When the streams are required to be persisted between server restarts, a Tier 2 storage is required by Pravega. For example, a mounted NFS share can be used as Tier 2 storage. Only the Segment Store requires its host to be configured with this storage. The Controller and Segment Store are run as different processes. These steps are described in [Manual Installation](manual-install.md).

### From Docker Image

The below command will download and run Pravega from the container image on docker hub.

**Note:** We must replace the `<ip>` with the IP of our machine to connect to Pravega from our local machine. Optionally we can replace `latest` with the version of Pravega as per the requirement.

```
docker run -it -e HOST_IP=<ip> -p 9090:9090 -p 12345:12345 pravega/pravega:latest standalone
```

## Docker Compose (Distributed Mode)

Unlike other options for running locally, the Docker Compose option runs a full deployment of Pravega
in distributed mode. It contains containers for running Zookeeper, Bookkeeper and HDFS. Hence Pravega operates as if it would in production. This is the easiest way to get started with the standalone option but requires additional resources.

1. Ensure that your host machine meets the following prerequisites:

   * It has Docker `1.12` or later installed.
   * It has Docker Compose installed.

2. Download the [docker-compose.yml](https://github.com/pravega/pravega/tree/master/docker/compose/docker-compose.yml) file from [Pravega](https://github.com/pravega/pravega) GitHub repository.

   ```
   $ wget https://raw.githubusercontent.com/pravega/pravega/master/docker/compose/docker-compose.yml
   ```

   Alternatively, clone the Pravega repository to fetch the code.

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

To access the Pravega Controller `REST` API, invoke it using a URL of the form `http://<HOST_IP>:10080/v1/scopes` (where
`/scopes` is one of the many endpoints that the API supports).
