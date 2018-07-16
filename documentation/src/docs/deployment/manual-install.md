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
Some specific Pravega instructions are shown below. All sets assuming being run from the `bookkeeper-server-4.4.0` directory.

#### Bookkeeper Configuration

The following configuration options should be changed in the `conf/bk_server.conf` file.

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

The following paths need to be created in Zookeeper. From the `zookeeper-3.5.1-alpha` directory on the Zookeeper servers run:

```
bin/zkCli.sh -server $ZK_URL create /pravega
bin/zkCli.sh -server $ZK_URL create /pravega/bookkeeper
```
Replace `<$ZK_URL>` with the IP address of the Zookeeper nodes

### Running Bookkeeper

Before starting the bookie, it needs to be formatted:

```
bin/bookkeeper shell metaformat -nonInteractive
```

Start the bookie:

```
bin/bookkeeper bookie
```
### Running Bookkeeper with encryption enabled
Apache BookKeeper can be deployed with TLS enabled. Details can be found [here](https://bookkeeper.apache.org/docs/latest/security/tls/).

---
# Installing Pravega

For non-production systems, you can use the containers provided by the [docker](docker-swarm.md) installation to run non-production HDFS, Zookeeper or Bookkeeper.

There are two key components of Pravega that need to be run:
- Controller - Control plane for Pravega. Installation requires at least one controller. Two or more are recommended for HA.
- Segment Store - Storage node for Pravega. Installation requires at least one segment store.

Before you start, you need to download the latest Pravega release. You can find the latest Pravega release on the [github releases page](https://github.com/pravega/pravega/releases).

## Recommendations

If you are getting started with a simple 3 node cluster, you may want to layout your services like this:

|                       | Node 1 | Node 2 | Node 3 |
| --------------------- | ------ | ------ | ------ |
| Zookeeper             | X      | X      | X      |
| Bookkeeper            | X      | X      | X      |
| Pravega Controller    | X      | X      |        |
| Pravega Segment Store | X      | X      | X      |

## All Nodes

On each node extract the distribution package to your desired directory:

```
tar xfvz pravega-0.1.0.tgz
cd pravega-0.1.0
```

## Installing the Controller

The controller can simply be run using the following command. Replace `<zk-ip>` with the IP address of the Zookeeper nodes

```
ZK_URL=<zk-ip>:2181 bin/pravega-controller
```

Alternatively, instead of specifying this on startup each time, you can edit the `conf/controller.conf` file and change the zk url there:

```
    zk {
      url = "<zk-ip>:2181"
...
    }
```

Then you can run the controller with:

```
bin/pravega-controller
```

## Installing the Segment Store

Edit the `conf/config.properties` file. The following properies need to be changed. Replace `<zk-ip>`, `<controller-ip>` and `<hdfs-ip>` with the IPs of the respective services:

```
pravegaservice.zkURL=<zk-ip>:2181
bookkeeper.zkAddress=<zk-ip>:2181
autoScale.controllerUri=tcp://<controller-ip>:9090

# Settings required for HDFS
hdfs.hdfsUrl=<hdfs-ip>:8020
```

Once the configuration changes have been made you can start the segment store with:

```
bin/pravega-segmentstore
```
## Runing Pravega Controller and Segment Store with security enabled
Here are the details for configurations for [Pravega controller](../security/pravega-security-configurations.md#Pravega controller) and [Pravega Segmentstore](../security/pravega-security-configurations.md#Pravega Segmentstore).
These parameters can be changed to represent required security configurations.