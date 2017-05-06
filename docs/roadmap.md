# Pravega OSS Roadmap

## Auto scaling of Streams

Scaling means increasing and decreasing the number of segments in a Stream based
on ingestion rate or number of events being ingested, aka Scaling Policy.
Currently, auto scaling feature is in EXPERIMENTAL stage. With further testing,
we plan to stabilize it. 

## Versioning of Wire Protocol

Given the stage of Pravega, it is likely that evolution will happen rapidly. To
support backward compatibility with older clients, we need to make sure that
protocol is versioned. 

## Improve Metrics and expose more runtime metrics

-   Expose metrics via multiple reporters
-   Improve StatsD reporter
-   Metrics for writer, reader and reader groups
-   Cache monitoring (RocksDB hit/miss)
-   Tier 2 interaction metrics
-   Metrics for requests to/from external components (ZK, BK, HDFS..)

## Security

-   Access Control on Stream operation
-   Encrypted connections among Clients, Controller and SegmentStore
-   Encrypted connections among Controller and SegmentStore, and ZK and HDFS

## Direct Access to Streams in HDFS.

Pravega is storing Streams in the Tier 2 storage as is. To make both Ability for
Hadoop MapReduce jobs to access to streams in key/value format...

## Add support for Apache Mesos

Add support for Mesos as a resource manager. Pravega Mesos integration should be
able to support dynamic acquisition and release of resources. 

## Connectors for Stream Processors

Additional streaming connectors allow data ingestion into Flink and using more
systems as sinks for Flink streaming programs.

-   Flink (source/sink) - [Completed](https://github.com/pravega/flink-connectors)
-   Spark (source/sink)
-   Apache Beam (TBD)
-   (TBD)

## Object Storage as a Tier 2 Storage

Ability to use Object storage as Tier 2 storage Layer. Preferably Amazon S3 or
any storage systems that support S3 API. 

## Low Level Reader API

-   For more flexible way to coordinate readers in a group with changing number of segments. 

## Pravega Mesos Framework

To fully support dynamic scaling of Pravega cluster based on resource
utilization. This would entail that we build Pravega framework that negotiates
with Mesos Resource Manager on securing or releasing additional resource. 

## Future Items - Subject to discussion 

-   REST Proxy for Reader/Writer (REST proxy for Admin operations is already there)
-   Stream aliasing
-   Stream trimming and retention
-   Ability to Geo-Replicate events
-   Ability to logically group multiple Streams
-   Exposing information for administration purposes
-   Provide default Failure Detector
-   Ability to assign arbitrary Key-Value pairs to streams - Tagging
-   Non-disruptive and rolling upgrades for Pravega
-   Policy driven tiering of Streams from Streaming Storage to Long-term storage
-   Ability to track/share schema of Stream message payload (Schema Registry or
    equivalent)
-   Stream consumption/read of a Stream from multiple sites via the Streaming
    API, HDFS and NFS APIs
-   Ability to enable reads/writes to/from Streams across all data-centers
    (Active-Active)
-   Logical isolation of streams provisioned by different tenants
-   Ability for Operator to define soft quotas for tenants
-   Ability to define QoS for tenants (min MB/s per stream segment guarantees)
-   Kafka API Compatibility (Producer and Consumer APIs)
