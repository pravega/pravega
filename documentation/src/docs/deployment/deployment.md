<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Deployment Overview

This guide describes the options for running Pravega for development, testing and in production.

## Pravega Modes

There are two modes for running Pravega.

- **Standalone:** Standalone mode is suitable for development and testing Pravega applications. It can either be run from the source code, from the distribution package or as a docker container. For more information please see [Running Pravega](http://pravega.io/docs/latest/deployment/run-local/#standalone-mode).

- **Distributed:** Distributed mode runs each component separately on a single or multiple nodes. This is suitable for production in addition for development and testing. The deployment options in this mode include a manual installation, running in Kubernetes or Docker Swarm or DC/OS.

## Prerequisites

The following prerequisites are required for running Pravega in all modes.

- Java 8

The following prerequisites are required for running in production. These are only required for running in distributed mode.

- External HDFS 2.7
- Zookeeper 3.5.4-beta
- Bookkeeper 4.7.3

For more details on the prerequisites and recommended configuration options for bookkeeper see the [Manual Install Guide](manual-install.md).

## Installation

There are multiple options provided for running Pravega in different environments. Most of these use the installation package from a Pravega release. You can find the latest Pravega release on the [GitHub Releases](https://github.com/pravega/pravega/releases).

- [Local](run-local.md) - Running Pravega locally is suitable for development and testing.
    - [Running from source](run-local.md#from-source)
    - Local Standalone Mode
    - Docker Compose (Distributed Mode)
- Production - Multi-node installation suitable for running in production.
    - [Manual Installation](manual-install.md)
    - [Kubernetes](kubernetes-install.md)
    - [Docker Swarm](docker-swarm.md)
    - [DC/OS](dcos-install.md)
    - Cloud - [AWS](aws-install.md)
