# Running Pravega

This guide describes the options for running Pravega for development, testing and in production.

## Pravega Modes

There are two modes for running Pravega.

- Standalone - Standalone mode is suitable for development and testing Pravega applications. It can either be run from the source code, from the distribution package or as a docker container.
- Distributed - Distributed mode runs each component separately on a single or multiple nodes. This is suitable for production in addition for development and testing. There are deployment options available including a manual installation, running in a docker swarm or DC/OS.

## Prerequisites

The following prerequisites are required for running in production. These are only required for running in distributed mode.

- Java 8 - Java 8 is required to be installed to run Pravega
- External HDFS 2.7 - HDFS is used as Tier 2 storage and must have sufficient capacity to store contents of all streams.
- Zookeeper 3.5.1-alpha - At least 3 Zookeeper servers recommended for a quorum. For installing Zookeeper see the [Getting Started Guide](http://zookeeper.apache.org/doc/r3.5.1-alpha/zookeeperStarted.html).
- Bookkeeper 4.4.0 - At least 3 Bookkeeper servers recommended for a quorum. For installing Bookkeeper see the [Getting Started Guide](http://bookkeeper.apache.org/docs/r4.4.0/bookkeeperStarted.html).

## Installation

There are multiple options provided for running Pravega in different environments. Most of these use the installation package from a Pravega release. You can find the latest Pravega release on the [github releases page](https://github.com/pravega/pravega/releases).

- [Local](run-local.md) - Running Pravega locally is suitable for development and testing.
    - Running from source
    - Local Standalone Mode
    - Docker Compose (Distributed Mode)
- Production - Multi-node installation suitable for running in production.
    - [Manual Installation](installation.md)
    - [Docker Swarm](docker-swarm.md)
    - [DC/OS](dcos-install.md)
    - Cloud - [AWS](aws-install.md)

