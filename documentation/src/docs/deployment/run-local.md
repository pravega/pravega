<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Running Pravega in Local Machine

Running in local machine, allows us to get started using pravega very quickly. Most of the options use the standalone mode which is suitable for most of the development and testing.

The prerequisites for running in local machine is described below.

## Standalone Mode

### From Source

First we need to have the Pravega source code checked out:

```
git clone https://github.com/pravega/pravega.git
cd pravega
```

The above command will download the dependencies. As a next step, compile pravega and start the standalone deployment.

```
./gradlew startStandalone
```

### From Installation Package

Download the Pravega latest release from the [github releases page](https://github.com/pravega/pravega/releases). The tarball or zip files can be used as they are identical. Instructions are provided for using the tar files, but the same can be used for the zip file also.

```
tar xfvz pravega-0.1.0.tgz
```

Run pravega standalone:

```
pravega-0.1.0/bin/pravega-standalone
```

### From Docker Container

The below command will download and run Pravega from the container image on docker hub.

**Note:** We must replace the `<ip>` with the IP of our machine to connect to Pravega from our local system. Optionally we can replace `latest` with the version of Pravega we desire.

```
docker run -it -e HOST_IP=<ip> -p 9090:9090 -p 12345:12345 pravega/pravega:latest standalone
```

## Docker Compose (Distributed Mode)

Unlike other options for running locally, the docker compose option runs a complete Pravega install in distributed mode. It contains containers for running Zookeeper, Bookkeeper and HDFS so Pravega operates as if it would in production. This is the simplest way to get started with as the standalone option but requires additional resources available.

To use this you need to have Docker `1.12` or later version.

Download the [docker-compose.yml](https://github.com/pravega/pravega/tree/master/docker/compose/docker-compose.yml) from github. For example:

```
wget https://github.com/pravega/pravega/tree/master/docker/compose/docker-compose.yml
```

You need to set the IP address of your local machine as the value of HOST_IP in the following command. To run:

```
HOST_IP=1.2.3.4 docker-compose up
```

Clients can then connect to the controller at `${HOST_IP}:9090`.

## Configuring standalone
The configuration properties for standalone can be specified in `config/standalone-config.properties`. 
These properties includes ports for zookeeper, segmentstore and controller.

They also contain other configurations related to security.

## Running standalone with encryption and authentication enabled
The configurations, `singlenode.enableTls` and `singlenode.enableauth` can be used to enable encryption and authentication respectively.
By default both these settings are disabled.
In case `enableTls` is set to true, the default certificates provided in the `conf` directory are used for setting up TLS.
These can be overridden by specifying in the properties file. 

