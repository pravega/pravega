---
title: Manual Installation
---

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

This page describes the prerequisites and installation steps to deploy Pravega in a multi-node production environment.

## Prerequisites

### HDFS

Setup a HDFS storage cluster running **HDFS version 2.7+**. HDFS is used as Tier 2 Storage and must have
sufficient capacity to store the contents of all the streams. The storage cluster is recommended to be run
alongside Pravega on separate nodes.

### Filesystem

If it is easier to mount a NFS share, then FILESYSTEM can be used in place of HDFS. The following configuration options are necessary to configure the FILESYSTEM as Long Term Storage.
pravegaservice.storage.impl.name = FILESYSTEM
filesystem.root = /mnt/tier2
where /mnt/tier2 is replaced with your nfs share and FILESYSTEM is a keyword.

### Java

Install the latest Java 8 from [java.oracle.com](http://java.oracle.com). Packages are available
for all major operating systems.

### Zookeeper

Pravega requires **Zookeeper 3.6.1**. At least 3 Zookeeper nodes are recommended for a quorum. No special configuration is required for Zookeeper but it is recommended to use a dedicated cluster for Pravega.

This specific version of Zookeeper can be downloaded from Apache at [zookeeper-3.6.1](https://archive.apache.org/dist/zookeeper/zookeeper-3.6.1/apache-zookeeper-3.6.1.tar.gz).

For installing Zookeeper see the [Getting Started Guide](http://zookeeper.apache.org/doc/r3.6.1/zookeeperStarted.html).

### Bookkeeper

Pravega requires **Bookkeeper 4.11.1**. At least 3 Bookkeeper servers are recommended for a quorum.

This specific version of Bookkeeper can be downloaded from Apache at [bookkeeper-server-4.11.1-bin.tar.gz](https://archive.apache.org/dist/bookkeeper/bookkeeper-4.11.1/bookkeeper-server-4.11.1-bin.tar.gz).

For installing Bookkeeper see the [Getting Started Guide](http://bookkeeper.apache.org/docs/4.11.1/getting-started/installation).
Some specific Pravega instructions are shown below. All sets are assumed to be run from the `bookkeeper-server-4.11.1` directory.

#### Bookkeeper Configuration

In the file `conf/bk_server.conf`, the following configuration options should be implemented:

```
metadataServiceUri=zk://localhost:2181/pravega/bookkeeper/ledgers

# Alternatively specify a different path to the storage for /bk
journalDirectory=/bk/journal
ledgerDirectories=/bk/ledgers
indexDirectories=/bk/index
```

### Initializing Zookeeper paths

The following paths need to be created in Zookeeper. Open the `zookeeper-3.6.1` directory on the Zookeeper servers and run the following paths:

```
bin/zkCli.sh -server $ZK_URL create /pravega
bin/zkCli.sh -server $ZK_URL create /pravega/bookkeeper
```
Replace `<$ZK_URL>` with the IP address of the Zookeeper nodes.

### Running Bookkeeper

The bookie needs the following formatting before starting it:

```
bin/bookkeeper shell metaformat -nonInteractive
```

Start the bookie as mentioned below:

```
bin/bookkeeper bookie
```
### Running Bookkeeper with encryption enabled
Apache BookKeeper can be deployed with TLS enabled. Details can be found [here](https://bookkeeper.apache.org/docs/4.11.1/security/tls/).

---
# Installing Pravega

For non-production systems, the containers can be used that are provided by the [Docker](docker-swarm.md) installation to run non-production HDFS, Zookeeper or Bookkeeper.

The following two key components of Pravega needs to be run:

- **Controller**: The Control plane for Pravega. Installation requires at least one Controller. \(Two or more are recommended for HA\).

- **Segment Store**: The Storage node for Pravega. Installation requires at least one Segment Store.

Before we start, the latest Pravega release needs to be downloaded from the [GitHub Releases page](https://github.com/pravega/pravega/releases).

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
tar xfvz pravega-<version>.tgz
cd pravega-<version>
```

## Installation of the Controller

The controller can be run by using the following command. Replace `<zk-ip>` with the IP address of the Zookeeper nodes in the following command:

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
pravegaservice.zk.connect.uri=<zk-ip>:2181
bookkeeper.zk.connect.uri=<zk-ip>:2181
autoScale.controller.connect.uri=tcp://<controller-ip>:9090

# Settings required for HDFS
hdfs.connect.uri=<hdfs-ip>:8020
```

After making the configuration changes, the segment store can be run using the following command:

```
bin/pravega-segmentstore
```
## Running a Pravega Cluster with Security enabled

Steps for securing a distributed mode cluster can be found [here](../security/securing-distributed-mode-cluster.md).

For detailed information about security configuration parameters for [Controller](../security/pravega-security-configurations.md#pravega-controller))
and [Segment Store](../security/pravega-security-configurations.md#pravega-segment-store),
see [this]((../security/pravega-security-configurations.md) document.
