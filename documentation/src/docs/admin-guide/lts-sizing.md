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

# Storage Right-Sizing
This document provides guidelines to right-sizing the storage capacity of a Pravega cluster (tier-1 and long-term
storage).

## Right-Sizing Long-Term Storage

Pravega's design enables users to store stream data virtually without bounds. However, in practical terms, we need
to understand what will be the storage capacity required in Long-Term Storage, either to provision such a system or
to estimate storage costs. Even more, users may have a legacy Long-Term Storage and want to devote part of its
capacity to store Pravega streams. In either case, we need guidelines to estimate the Long-Term Storage space 
requirements for Pravega[[3]](https://github.com/pravega/pravega/issues/4503 ).    

There are two ways to reduce the amount of data stored in a Pravega Stream: _deleting or truncating the Stream_. 
In this guide, we focus on Stream truncation, and more concretely, on how to provision Long-Term Storage capacity
when Stream truncation is performed automatically in Pravega via 
[_Stream auto-retention_](http://pravega.io/docs/latest/pravega-concepts/#stream-retention-policies). In this regard,
the main configuration parameters involved are:

- **_`controller.retention.frequency.minutes`_**: The Controller service periodically triggers the enforcement of
Stream auto-retention (i.e., truncation) based on this parameter.
_Type_: `Integer`. _Default_: `30`. _Update-mode_: `per-server`.

- **_`controller.retention.bucket.count`_**: Controllers distribute the Streams that require periodic auto-retention
across the number of (Stream) "buckets" defined in this parameter. Multiple Controller instances will share the load related
to auto-retention by owning a subset of buckets.
_Type_: `Integer`. _Default_: `1`. _Update-mode_: `read-only`.

- **_`controller.retention.thread.count`_**: Number of threads in the Controller devoted to execute Stream auto-retention tasks.
_Type_: `Integer`. _Default_: `1`. _Update-mode_: `per-server`.

- **_`writer.rollover.size.bytes.max`_**: The maximum size of a single Segment Chunk in Storage. While this can 
be configured on a per-segment basis via the Segment Store API, this setting establishes a ceiling to that value. 
No Segment Chunk may exceed this length regardless of how it was configured via the API (if at all). Use this setting 
if you need to meet some hardware limitation with the underlying Storage implementation (i.e., maximum file size).
Valid values: Positive number.
 
We can enforce two types of auto-retention policies on our Streams: _size-based_ and _time-based_. There is also
a new [consumption-based retention model](https://github.com/pravega/pravega/wiki/PDP-47-%28Pravega-Consumption-Based-Retention%29), 
but it can be combined with the previous policies as well. Also, in this section we assume that all the Streams in the 
system are configured with some type of auto-retention.

The Long-Term Storage requirements in a production deployment are dictated by:
- _Rate of data ingestion_: This is the main factor that dictates the Long-Term Storage requirements. Clearly, having
a high data ingestion rate unavoidably leads to higher Long-Term Storage requirements. 
- _Stream retention policy used_: The retention configuration of our Streams is almost as important as the ingestion rate.
That is, we can have a data high ingestion rate on Streams configured with frequent/small retention period/size, 
which would still keep the Long-Term Storage requirements low.
- _Efficiency of serializers_: Pravega only understands bytes, not events. This means that applications writing and
reading events from Pravega are required to provide serializers in order to convert events into bytes and the other way
round. Depending on the type of data being stored and the serializer implementation, the write amplification of user
events into bytes may be significant.
- _System metadata storage of Pravega_: Internally, Pravega stores most of its metadata in Key/Value Tables, which are
backed by regular segments. Thus, the metadata of Pravega is also going to consume some space in Long-Term Storage.
While this storage cost is minor, depending on how metadata-heavy the workload is it may represent 1% to 5% of the 
storage space in most cases (i.e., storing some data in 1 Stream has lower metadata cost than storing the same data in 
1 million Streams).

With the above points in mind, let's go for a practical example. Let's assume that our workload consists on an average
ingestion rate of 1GBps and we have only 1 Stream in the system. Also, the serializers we use in our application have 
a write amplification of 1.3x and we foresee an additional 5% of storage cost related to Pravega's metadata. Given 
that, let's exercise how much Long-Term Storage capacity we would need with the following auto-retention policies:

- _Size-based auto-retention of 1TB_: This policy attempts to keep at least 1TB of data related to our Stream.
Therefore, the Long-Term Storage space needed would be at least: `1TB · 1.3 + 1TB · 0.05 = 1.35TB`. 

- _Time-based retention of 1 day_: With this policy, Pravega will keep data for our Stream for at least 1 day.
This means that our Long-Term Storage may require to store at least: `86400s * 1GBps * 1.3 + 86400s * 1GBps * 0.05 = 116.7TB`.

Note that the above storage estimations are lower bounds. They assume that the Pravega auto-retention service immediately
truncates the Stream once the retention policy has been met. However, the Pravega auto-retention service is implemented
as a background task and should be properly configured under stringent retention requirements. Next, we provide some
guidelines to configure the Pravega auto-retention service:

- _Set a proper retention frequency_: By default, `controller.retention.frequency.minutes=30`. This means that Long-Term
Storage should be provisioned to accommodate at least 30 minutes of data beyond what is configured in the auto-retention
policy, until a Controller instance truncates the Stream. If your cluster should minimize the usage of Long-Term Storage
resources, then you may want to set this parameter to a lower value (e.g., 1-5 minutes).

- _Define the right number of buckets_: Similarly to what happens to Segment Containers, we need to think twice before
setting the number of buckets. That is, if we leave the default value of 1, there would be only one Controller instance 
handling all the work related to auto-retention. For this reason, we recommend to estimate the initial number of Controller 
instances required in your Pravega Cluster: as a rule of thumb, having 1 to 2 Controllers every 3 Segment Stores is enough, 
depending on the metadata load expected (e.g., working with small [Transactions](http://pravega.io/docs/latest/pravega-concepts/#transactions) 
or with many Streams induces more metadata workload). Once the number of Controllers for a deployment is estimated,
we can define a number of 4 to 8 times the number of Controllers. That is, if we have 3 Controller instances,
we will configure `controller.retention.bucket.count=24`. With this, we could scale up the number of Controllers and
the new instances will share the load along with with the previous ones.

- _Controller retention thread pool size_: In general, having 1 thread (`controller.retention.thread.count=1`, default)
per Controller devoted to auto-retention should be enough. However, if you expect your workload to be very 
intensive in terms of auto-retention (i.e., thousands of Streams, stringent retention requirements) consider to increase
the thread pool size to 2-4 threads. 

- _Maximum size of single chunk in LTS: In production, it may be required to revisit the value of `writer.rollover.size.bytes.max`.
Limiting the size of chunks in LTS poses a trade-off: if the value is low, the system will achieve fine retention granularity 
(good) but extra metadata overhead (not so good). On the contrary, a high value in this parameter induces coarse retention 
granularity with less metadata overhead. Limiting chunk sizes between 1GB-16GB seems reasonable in many scenarios.

## Right-Sizing Tier-1 Storage (Bookkeeper)

Pravega uses tier-1 storage for durable, low-latency persistence until data can be moved to long-term storage.
Thus, we need guidelines to right-sizing the storage capacity required in tier-1 (i.e., Bookkeeper).

To this end, there are three variables that we need to take into account:
- _Max ledger size (`bookkeeper.ledger.size.max`)_: Maximum Ledger size (bytes) in BookKeeper. Once a Ledger reaches this size, 
it will be closed and another one opened. _Type_: `Long`. _Default_: `1073741824` (1 GB). _Update-mode_: `per-server`.

- _Number of Segment Containers (`pravegaservice.container.count`, `controller.container.count`)_: Number of Segment Containers in the system. 
This value must be the same across all Segment Store instances in this cluster. This value cannot be changed dynamically. 
See [documentation for details](https://cncf.pravega.io/docs/nightly/segment-store-service/#segment-containers).
_Type_: `Integer`. _Default_: `4`. _Update-mode_: `read-only`.

- _Replication degree (`bookkeeper.write.quorum.size`)_: Write Quorum size for BookKeeper ledgers.
_Type_: `Integer`. _Default_: `3`. _Update-mode_: `per-server`.

Each Segment Container has a Durable Log (default implementation is `BookkeeperLog`). For context, a Segment
Container multiplexes and writes all the operations of active Segments to its Durable Log. In the Bookkeeper implementation
of the Durable Log, Segment operations are grouped into `DataFrames` and written as individual entries into a Ledger.
If the Ledger reaches the maximum size specified in `bookkeeper.ledger.size.max`, then the current Ledger is closed and
a new one is created (i.e., Ledger rollover). In parallel, as soon as data for a Segment Container is correctly persisted
in long-term storage, the Durable Log can be truncated. When working with Bookkeeper, truncating a Durable Log means
deleting the Ledgers whose last entry is lower than the last truncation point. Once these Ledgers are marked as deleted,
Bookkeeper can delete them when running garbage collection, thus freeing up space to continue writing data.

With the above context, the question to answer is what is the minimum amount of storage space that is needed
for preventing Pravega from exhausting storage space in Bookkeeper during normal operation? We suggest provisioning storage 
space for at least 2 full-length ledgers per container (so one Ledger can be garbage collected by Bookkeeper while the Segment
Container continues to write to the new one). Therefore, we could estimate the storage space in Bookkeeper as follows:

_Bookie journal/ledger volume_: 2 * `bookkeeper.ledger.size.max` * `pravegaservice.container.count` * (`bookkeeper.write.quorum.size` / `#bookies`)

That is, we need a storage space on Bookies that can allocate 2 max size ledgers per container. Nonetheless, we have also to take
into account the replication factor of Pravega writing to Bookkeeper, as well as the number of Bookies. For example, assuming the
default values, we would need `2 * 4 Segment Containers * 1GB = 8GB` of storage space in Bookkeeper. This is in the
case of having 3 Bookies (as `bookkeeper.write.quorum.size = 3`). If we had 6 Bookies instead, the minimum storage space
to be allocated in Bookie volumes would be 4GB. 

As an important point, note that this is the storage space to be allocated _individually to each journal and ledger volumes of Bookies_.
Also, this is a _lower bound estimation_: there might be situations in which Pravega could still hit Bookies getting rid of
storage space (e.g., if long-term storage is unavailable for very long while Pravega keeps ingesting data). For this reason,
we recommend using this calculation as a bare minimum estimation and always provision Bookie volumes with higher storage capacity.
