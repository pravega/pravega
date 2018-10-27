<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Segment Containers in a Pravega Cluster

This document describes the high level design of how we are managing the lifecyle of **Segment Containers** in a Pravega Cluster.

## Segment Containers
In this document we refer to a segment container as container. The total number of containers is fixed for a given deployment. Each container can be owned by only one Pravega host and all containers in the cluster should be running at any given point in time.

## Pravega Host
A Pravega host is an instance of a Pravega service which owns and executes a set of containers.

# Detecting Active Pravega Hosts
Every Pravega host on startup will register itself with Zookeeper using ephemeral nodes. The ephemeral nodes are present in Zookeeper as long as Zookeeper receives appropriate signals from the Pravega host. These ephemeral nodes are used to detect the active Pravega hosts in the cluster.

# Monitoring the Pravega Cluster
Each Pravega Controller runs a service which monitors the ephemeral nodes on the Zookeeper and detects all active Pravega hosts in the cluster.
If any changes are detected to the cluster membership, then all Containers are verified and re-mapped to the available set of Pravega hosts. This information is persisted in the HostStore atomically. This is stored as a single blob and contains a map of Host to set of Containers that a host owns.

We use Zookeeper to ensure only one Pravega Controller is monitoring the cluster to avoid multiple simultaneous Writers to the HostStore. This will avoid race conditions and allow the state to converge faster.

## Rebalance Frequency

When a Pravega Host is added or removed from the cluster, rebalancing of the Container ownership happens. As it is an expensive operation, Pravega maintains a configured minimum time interval between any two rebalance operations. It ends up in proportionally increasing more time for performing ownership change in the cluster, if the rebalance operation is delayed due to some reason.

# Ownership Change Notification
Every Pravega host has a long running Segment Manager Service. This constantly polls/watches the HostStore for any changes to the container ownership. On detecting any ownership changes for itself (identified by the host key in the Map) the Segment Manager triggers addition and removal of containers accordingly.
