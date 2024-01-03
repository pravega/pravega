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
# Manual Installation

This page describes the prerequisites and installation steps to deploy Pravega in a multi-node production environment.

## Prerequisites

### Cloud Storage

Pravega supports Amazon S3, Azure Blob Storage, Google Cloud Storage, HDFS and mounted distributed filesystems for Long Term Storage.
With the S3 protocol being widely supported by 3rd party storage systems, Pravega can be run either alongside a private storage
cluster or by targeting public cloud resources.

### Filesystem

If it is easier to mount an NFS share, then `FILESYSTEM` can be used for Long Term Storage. The following configuration options are necessary to configure the `FILESYSTEM` as Long Term Storage.
```
pravegaservice.storage.impl.name = FILESYSTEM
filesystem.root = /mnt/tier2
```
where `/mnt/tier2` is replaced with your nfs share and `FILESYSTEM` is a keyword.

Other options for `pravegaservice.storage.impl.name` include `AZURE`, `EXTENDEDS3`, `GCP`, `HDFS`, and `S3`.

### Java

Install Java 11 or later. Packages are available for all major operating systems.

### Zookeeper

Pravega requires **Zookeeper 3.6.3**. At least 3 Zookeeper nodes are recommended for a quorum. No special configuration is required for Zookeeper but it is recommended to use a dedicated cluster for Pravega.

This specific version of Zookeeper can be downloaded from Apache at [apache-zookeeper-3.6.3.tar.gz](https://archive.apache.org/dist/zookeeper/zookeeper-3.6.3/apache-zookeeper-3.6.3.tar.gz).

For installing Zookeeper see the [Getting Started Guide](https://zookeeper.apache.org/doc/r3.6.3/zookeeperStarted.html).

### Bookkeeper

Pravega requires **Bookkeeper 4.14.1**. At least 3 Bookkeeper servers are recommended for a quorum.

This specific version of Bookkeeper can be downloaded from Apache at [bookkeeper-server-4.14.1-bin.tar.gz](https://archive.apache.org/dist/bookkeeper/bookkeeper-4.14.1/bookkeeper-server-4.14.1-bin.tar.gz).

For installing Bookkeeper see the [Getting Started Guide](https://bookkeeper.apache.org/docs/getting-started/installation/).
Some specific Pravega instructions are shown below. All sets are assumed to be run from the `bookkeeper-server-4.14.1` directory.

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

The following paths need to be created in Zookeeper. Open the `zookeeper-3.6.3` directory on the Zookeeper servers and run the following paths:

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
Apache BookKeeper can be deployed with TLS enabled. Details can be found [here](https://bookkeeper.apache.org/docs/security/tls/).

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
Replace `<zk-ip>` and `<controller-ip>` with the IPs of the respective services.

```
pravegaservice.zk.connect.uri=<zk-ip>:2181
bookkeeper.zk.connect.uri=<zk-ip>:2181
autoScale.controller.connect.uri=tcp://<controller-ip>:9090

# Settings required for your Long Term Storage provider
```

After making the configuration changes, the segment store can be run using the following command:

```
bin/pravega-segmentstore
```
## Running a Pravega Cluster with Security enabled

Steps for securing a distributed mode cluster can be found [here](../security/securing-distributed-mode-cluster.md).

For detailed information about security configuration parameters for [Controller](../security/pravega-security-configurations.md#controller-tls-configuration-parameters))
and [Segment Store](../security/pravega-security-configurations.md#segment-store-tls-configuration-parameters),
see [this]((../security/pravega-security-configurations.md) document.
