<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Prerequisites

This guide details the prerequisites required to run Pravega in production (Distributed mode only).

# HDFS

Setup an HDFS storage cluster running HDFS version 2.7+. HDFS is used as Tier 2 storage and must have 
sufficient capacity to store contents of all streams. The storage cluster is recommended to be run
alongside Pravega on separate nodes.

## Java

Install the latest Java 8 from [java.oracle.com](http://java.oracle.com). Packages are available
for all major operating systems.

# Zookeeper

Pravega requires Zookeeper 3.5.1-alpha+. At least 3 Zookeeper nodes are recommended for a quorum. No special
configuration is required for Zookeeper but it is recommended to use a dedicated cluster for Pravega.

This specific version of Zookeeper can be downloaded from Apache at [zookeeper-3.5.1-alpha.tar.gz](http://www.apache.org/dist/zookeeper/zookeeper-3.5.1-alpha/zookeeper-3.5.1-alpha.tar.gz).

For installing Zookeeper see the [Getting Started Guide](http://zookeeper.apache.org/doc/r3.5.1-alpha/zookeeperStarted.html).

# Bookkeeper

Pravega requires Bookkeeper 4.4.0+. At least 3 Bookkeeper servers are recommended for a quorum.

This specific version of Bookkeeper can be downloaded from Apache at [bookkeeper-server-4.4.0-bin.tar.gz](http://www.apache.org/dist/bookkeeper/bookkeeper-4.4.0/bookkeeper-server-4.4.0-bin.tar.gz).

For installing Bookkeeper see the [Getting Started Guide](http://bookkeeper.apache.org/docs/r4.4.0/bookkeeperStarted.html).
Some specific Pravega instructions are shown below. All sets assuming being run from the `bookkeeper-server-4.4.0` directory.

## Bookkeeper Configuration

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

## Initializing Zookeeper paths

The following paths need to be created in Zookeeper. From the `zookeeper-3.5.1-alpha` directory on the Zookeeper servers run:

```
bin/zkCli.sh -server $ZK_URL create /pravega
bin/zkCli.sh -server $ZK_URL create /pravega/bookkeeper
```

## Running Bookkeeper

Before starting the bookie, it needs to be formatted:

```
bin/bookkeeper shell metaformat -nonInteractive
```

Start the bookie:

```
bin/bookkeeper bookie
```
