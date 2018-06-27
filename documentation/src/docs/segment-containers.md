<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Segment Containers in a Pravega Cluster

This document describes the high level design of how we are managing the lifecyle of Segment Containers in a Pravega Cluster.

## Segment Containers
In this document we refer to a **segment container** as **container**. The total number of containers is fixed for a given deployment. Each container can be owned by only one Pravega host and all containers in the cluster should be running at any given point in time. 

## Pravega Host
A pravega host is an instance of a pravega service which owns and executes a set of containers.

# Detecting Active Pravega Hosts
Every pravega host on startup will register itself with Zookeeper using ephemeral nodes. The ephemeral nodes are present in zookeeper as long as zookeeper receives appropriate heartbeats from the Pravega host. We rely on these ephemeral nodes to detect which pravega hosts are currently active in the cluster.

# Monitoring the Pravega Cluster
Each Pravega Controller runs a service which monitors the ephemeral nodes on the zookeeper and detects all active pravega hosts in the cluster.
On detecting any changes to the cluster membership we verify and re-map all containers to the available set of pravega hosts. This information is persisted in the HostStore atomically. This is stored as a single blob and contains a Map of Host to Set of Containers that a host owns.

We use zookeeper to ensure only one pravega controller is monitoring the cluster to avoid multiple simultaneous writers to the HostStore. This will avoid race conditions and allow the state to converge faster.

## Rebalance Frequency
Rebalance of the container ownership happens when either a pravega host is added or removed from the cluster. Since this is a costly operation we need to guard against doing this very often. Currently we ensure that a configured minimum time interval is maintained between any 2 rebalance operations. In the worst case this will proportionally increase the time it takes to perform ownership change in the cluster.

# Ownership Change Notification
Every pravega host has a long running Segment Manager Service. This constantly polls/watches the HostStore for any changes to the container ownership. On detecting any ownership changes for itself (identified by the host key in the Map) the Segment Manager triggers addition and removal of containers accordingly.

## Ensuring a container is stopped on one host before it is started on another host
We currently plan to use conservative timeouts on the Pravega host to ensure a container is not started before it is stopped on another node. This needs further review/discussion.

## Detecting and handling container start/stop failures
Any container start/stop failures is treated as local failures and we expect the Pravega host to try its best to handle these scenarios locally. The Pravega Controller does not make any effort to correct this by running them on different hosts. This needs further review/discussion.
