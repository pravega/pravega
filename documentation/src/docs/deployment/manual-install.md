<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Manual Installation

This page describes the prerequisites and installation steps to deploy Pravega in a multi-node production environment.

## Prerequisites

### HDFS

Setup an HDFS storage cluster running **HDFS version 2.7+**. HDFS is used as Tier 2 storage and must have
sufficient capacity to store contents of all streams. The storage cluster is recommended to be run
alongside Pravega on separate nodes.

### Java

Install the latest Java 8 from [java.oracle.com](http://java.oracle.com). Packages are available
for all major operating systems.

### Zookeeper

Pravega requires **Zookeeper 3.5.1-alpha+**. At least 3 Zookeeper nodes are recommended for a quorum. No special configuration is required for Zookeeper but it is recommended to use a dedicated cluster for Pravega.

This specific version of Zookeeper can be downloaded from Apache at [zookeeper-3.5.1-alpha.tar.gz](https://archive.apache.org/dist/zookeeper/zookeeper-3.5.1-alpha/zookeeper-3.5.1-alpha.tar.gz).

For installing Zookeeper see the [Getting Started Guide](http://zookeeper.apache.org/doc/r3.5.1-alpha/zookeeperStarted.html).

### Bookkeeper

Pravega requires **Bookkeeper 4.4.0+**. At least 3 Bookkeeper servers are recommended for a quorum.

This specific version of Bookkeeper can be downloaded from Apache at [bookkeeper-server-4.4.0-bin.tar.gz](https://archive.apache.org/dist/bookkeeper/bookkeeper-4.4.0//bookkeeper-server-4.4.0-bin.tar.gz).

For installing Bookkeeper see the [Getting Started Guide](http://bookkeeper.apache.org/docs/r4.4.0/bookkeeperStarted.html).
Some specific Pravega instructions are shown below. All sets are assumed to be run from the `bookkeeper-server-4.4.0` directory.

#### Bookkeeper Configuration

In the file `conf/bk_server.conf` file, the following configuration options should be implemented.

```
# Comma separated list of <zp-ip>:<port> for all ZK servers
zkServers=localhost:2181

# Alternatively specify a different path to the storage for /bk
journalDirectory=/bk/journal
ledgerDirectories=/bk/ledgers
indexDirectories=/bk/index

zkLedgersRootPath=/pravega/bookkeeper/ledgers
```

### Initializing Zookeeper paths

The following paths need to be created in Zookeeper. Open the `zookeeper-3.5.1-alpha` directory on the Zookeeper servers and run the following paths:

```
bin/zkCli.sh -server $ZK_URL create /pravega
bin/zkCli.sh -server $ZK_URL create /pravega/bookkeeper
```
Replace `<$ZK_URL>` with the IP address of the Zookeeper nodes.

### Running Bookkeeper

The bookie needs the following formatting before statring it.

```
bin/bookkeeper shell metaformat -nonInteractive
```

Start the bookie as mentioned below:

```
bin/bookkeeper bookie
```
### Running Bookkeeper with encryption enabled
Apache BookKeeper can be deployed with TLS enabled. Details can be found [here](https://bookkeeper.apache.org/docs/latest/security/tls/).

---
# Installing Pravega

For non-production systems, the containers can be used that are provided by the [docker](docker-swarm.md) installation to run non-production HDFS, Zookeeper or Bookkeeper.

The following two key components of Pravega needs to be run:
- Controller: The Control plane for Pravega. Installation requires at least one controlle.\(Two or more are recommended for HA\).
- Segment Store: The Storage node for Pravega. Installation requires at least one segment store.

Before we start, the latest Pravega release needs to be downloaded from the [github releases page](https://github.com/pravega/pravega/releases).

## Recommendations

_For a simple 3 node cluster, the following table depicts on layout of the services:_

|                       | Node 1 | Node 2 | Node 3 |
| --------------------- | ------ | ------ | ------ |
| Zookeeper             | X      | X      | X      |
| Bookkeeper            | X      | X      | X      |
| Pravega Controller    | X      | X      |        |
| Pravega Segment Store | X      | X      | X      |

## All Nodes

On each node, extract the distribution package to the desired directory as follows:

```
tar xfvz pravega-0.1.0.tgz
cd pravega-0.1.0
```

## Installation of the Controller

The controller can be run by using the following command. Replace `<zk-ip>` with the IP address of the Zookeeper nodes in the following command.

```
ZK_URL=<zk-ip>:2181 bin/pravega-controller
```

Instead specifying the `<zk-ip>` on every startup, we can edit the `conf/controller.conf` file and change the zk url as follows:

```
    zk {
      url = "<zk-ip>:2181"
...
    }
```

Then run the controller with the following command:

```
bin/pravega-controller
```

## Installation of the Segment Store

In the file `conf/config.properties`, make the following changes as mentioned:
Replace `<zk-ip>`, `<controller-ip>` and `<hdfs-ip>` with the IPs of the respective services.

```
pravegaservice.zkURL=<zk-ip>:2181
bookkeeper.zkAddress=<zk-ip>:2181
autoScale.controllerUri=tcp://<controller-ip>:9090

# Settings required for HDFS
hdfs.hdfsUrl=<hdfs-ip>:8020
```

After making the configuration changes, the segment store can be run using the following command:

```
bin/pravega-segmentstore
```
## Runing Pravega Controller and Segment Store with security enabled
Here are the details for configurations for [Pravega controller](../security/pravega-security-configurations.md#Pravega controller) and [Pravega Segmentstore](../security/pravega-security-configurations.md#Pravega Segmentstore).
These parameters can be changed to represent required security configurations.
