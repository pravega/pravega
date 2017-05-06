# Pravega Deployment Overview

This guide describes the options for running Pravega for development, testing and in production.

## Pravega Modes

There are two modes for running Pravega.

- Standalone - Standalone mode is suitable for development and testing Pravega applications. It can either be run from the source code, from the distribution package or as a docker container.
- Distributed - Distributed mode runs each component separately on a single or multiple nodes. This is suitable for production in addition for development and testing. There are deployment options available including a manual installation, running in a docker swarm or DC/OS.

## Prerequisites

The following prerequisites are required for running pravega in all modes.

- Java 8

The following prerequisites are required for running in production. These are only required for running in distributed mode.

- External HDFS 2.7
- Zookeeper 3.5.1-alpha
- Bookkeeper 4.4.0

For more details on the prerequisites and recommended configuration options for bookkeeper see the [Prerequisites Guide](prereqs.md).

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

