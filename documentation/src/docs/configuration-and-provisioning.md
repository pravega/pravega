# Pravega Configuration and Provisioning Guide

This document summarizes the most important aspects to take consider for configuring and provisioning a
Pravega Cluster in production infrastructure-agnostic way. Note that this document does not cover all the 
parameters in the configuration of Pravega as they are [already documented](/config/config.properties). Similarly,
this is not a guide to [deploy Pravega on Kubernetes](https://github.com/pravega/pravega-operator).

## Configuring Cluster Service Dependencies

A Pravega Cluster consists of a set of services (Zookeeper, Bookkeeper, Segment Store, Controller and Long-Term Storage)
that interact among them. Understanding such dependencies and the parameters that express them is key. Next, we describe
such dependencies and the main parameters in this regard with a practical example: to configure a Pravega Cluster with
Zookeeper and Bookkeeper using NFS as Long-Term Storage.

Relevant configuration parameters:
 
- **`pravegaservice.cluster.name`**: Name of the cluster to which this instance of the SegmentStore belongs.
_Type_: `String`. _Default_: `pravega-cluster`. _Update-mode_: `read-only`.

- **`pravegaservice.zk.connect.uri`**: Full URL (host:port) where to find a ZooKeeper that can be used for coordinating 
this Pravega Cluster. 
_Type_: `String`. _Default_: `localhost:2181`. _Update-mode_: `read-only`.

- **`pravegaservice.dataLog.impl.name`**: DataLog implementation for Durable Data Log Storage. Valid values: BOOKKEEPER, 
INMEMORY.
_Type_: `String`. _Default_: `BOOKKEEPER`. _Update-mode_: `read-only`.

- **`pravegaservice.storage.impl.name`**: Storage implementation for Long-Term Storage. Valid values: HDFS, FILESYSTEM, 
EXTENDEDS3, INMEMORY.
_Type_: `String`. _Default_: `HDFS`. _Update-mode_: `read-only`.

- **`autoScale.controller.connect.uri`**: URI for the Pravega Controller.
_Type_: `String`. _Default_: `pravega://localhost:9090`. _Update-mode_: `cluster-wide`.

- **`bookkeeper.zk.connect.uri`**: Endpoint address (hostname:port) where the ZooKeeper controlling BookKeeper for this 
cluster can be found at. This value must be the same for all Pravega SegmentStore instances in this cluster.
_Type_: `String`. _Default_: `localhost:2181`. _Update-mode_: `read-only`.

- **`bookkeeper.ledger.path`**: Default root path for BookKeeper Ledger metadata. This property tells the BookKeeper 
client where to look up Ledger Metadata for the BookKeeper cluster it is connecting to. If this property isn't uncommented, 
then the default value for the path is the Pravega cluster namespace ("/pravega/<cluster-name>/") with "ledgers" appended:
"/pravega/<cluster-name>/ledgers". Otherwise, it will take the value specified below. 
_Type_: `String`. _Default_: `/pravega/bookkeeper/ledgers`. _Update-mode_: `read-only`.

- **`filesystem.root`**: Root path where NFS shared directory needs to be mounted before segmentstore starts execution.
_Type_: `String`. _Default_: ` `. _Update-mode_: `read-only`.

- **`metadataServiceUri`** (BOOKKEEPER): Metadata service uri in Bookkeeper that is used for loading corresponding 
metadata driver and resolving its metadata service location.
_Type_: `String`. _Default_: `zk+hierarchical://localhost:2181/ledgers`. _Update-mode_: `read-only`.







## Number of Segment Containers


## Segment Store Cache Size and Memory Settings


## Durable Log Configuration: Bookkeeper


## Right-Sizing Long-Term Storage


