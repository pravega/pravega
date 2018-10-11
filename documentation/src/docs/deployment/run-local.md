<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Running Pravega Locally

Running locally allows you to get started using pravega very quickly. Most of the options use the standalone mode which is suitable for most development and testing.

All of the options for running locally start the required prerequisites so you can get started right away.

## Standalone Mode

### From Source

First you need to have the Pravega source code checked out:

```
git clone https://github.com/pravega/pravega.git
cd pravega
```

This one command will download dependencies, compile pravega and start the standalone deployment.

```
./gradlew startStandalone
```

### From Installation Package

Download the Pravega release from the [github releases page](https://github.com/pravega/pravega/releases). The tarball and zip are identical. Instructions are provided using tar but the same steps work with the zip file.

```
tar xfvz pravega-0.1.0.tgz
```

Run pravega standalone:

```
pravega-0.1.0/bin/pravega-standalone
```

### From Docker Container

This will download and run Pravega from the container image on docker hub.

Note, you must replace the `<ip>` with the IP of your machine. This is so that you can connect to Pravega from your local system. Optionally you can replace `latest` with the version of Pravega your desire

```
docker run -it -e HOST_IP=<ip> -p 9090:9090 -p 12345:12345 pravega/pravega:latest standalone
```

## Docker Compose (Distributed Mode)

Unlike other options for running locally, the docker compose option runs a full Pravega install in distributed mode. It contains containers for running Zookeeper, Bookkeeper and HDFS so Pravega operates as if it would in production. This is as easy to get started with as the standalone option but requires additional resources available.

To use this you need to have Docker `1.12` or later.

Download the [docker-compose.yml](https://github.com/pravega/pravega/tree/master/docker/compose/docker-compose.yml) from github. For example:

```
wget https://raw.githubusercontent.com/pravega/pravega/master/docker/compose/docker-compose.yml
```

You need to set the IP address of your local machine as the value of HOST_IP in the following command. To run:

```
HOST_IP=1.2.3.4 docker-compose up
```

Clients can then connect to the controller at `${HOST_IP}:9090`.
