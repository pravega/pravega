# Pravega Roadmap 

# Version 0.2 (May 2017)

## Auto scaling of Streams

Scaling means increasing and decreasing the number of segments in a Stream based
on ingestion rate or number of events being ingested, aka Scaling Policy.
Auto scaling is currently EXPERIMENTAL. We plan to fine tune and 
stabilize it in the upcoming releases. 

## Reader Group
 
-  Callback when # of segment changes
-  Reader Group Metrics 
-  Independent Reader  (Not member of a group)
-  Manual assignment of segment

## Segment Store

-  Tools for debugging, more tests, etc.
-  Unified Auth with Tier 2 per segment/stream basis

## Improve Metrics and expose more runtime metrics

-   Expose metrics via multiple reporters
-   Improve StatsD reporter
-   Metrics for writer, reader and reader groups
-   Cache monitoring (RocksDB hit/miss)
-   Tier 2 interaction metrics
-   Metrics for requests to/from external components (ZK, BK, HDFS..)

## Flink Connector - Finalize 
-   Flink (source/sink) - [Completed](https://github.com/pravega/flink-connectors)
-   Support dynamic scaling of workers in flink jobs 

# Version 0.3 (July 2017)

## Security

-   Access Control on Stream operation
-   Encrypted connections among Clients, Controller and SegmentStore
-   Encrypted connections among Controller and SegmentStore, and ZK and HDFS

## Direct Access to Streams in HDFS

Allow Hadoop MapReduce jobs to access stream data stored in Tier 2 HDFS.

## Low Level Reader API

-   A more flexible way to coordinate readers in a group with changing number of segments. 

## Connectors for Stream Processors

Additional streaming connectors allow data ingestion into Flink and using more
systems as sinks for Flink streaming programs.

-   Spark (source/sink)
-   Apache Beam (TBD)
-   (TBD)

# Version 0.4 (Sep 2017)

## Object Storage as a Tier 2 Storage

Ability to use Object storage as Tier 2 storage Layer. Preferably Amazon S3 or
any storage systems that support S3 API. 

## Pravega Mesos Framework

To fully support dynamic scaling of Pravega cluster based on resource
utilization. This would entail that we build Pravega framework that negotiates
with Mesos Resource Manager on securing or releasing additional resource. 

# Future Items - Not versioned yet. Subject to discussion 

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
