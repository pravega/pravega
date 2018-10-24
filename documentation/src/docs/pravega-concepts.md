<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Concepts

Pravega is an open source storage primitive implementing **Streams** for continuous and unbounded data.

An overview of the key concepts of Pravega is discussed. Please, see [Terminology](terminology.md) for a concise definition of key terms of Pravega concepts.

## Streams

Pravega organizes data into **Streams**.  A stream is a durable, elastic, append-only, unbounded sequence of bytes having good performance and strong consistency.  A Pravega stream is
similar to but more flexible than a "topic" in popular message oriented middleware such as [RabbitMQ](https://www.rabbitmq.com/) or [Apache Kafka](https://kafka.apache.org/).

Pravega streams are based on an append-only log data structure. By using the
append-only logs, Pravega rapidly ingest data into durable storage. It supports a large variety of application use cases as follows:

- Supports stream processing by using frameworks like [Flink](https://flink.apache.org).
- Publish/subscribe messaging.
- NoSQL databases like Time Series.
- Database (TSDB).
- Workflow engines.
- Event-oriented applications, etc., 

In Pravega, during the creation of the stream the developers assigns a meaningful
name to the stream like "IoTSensorData" or "WebApplicationLog20170330" to provide
more information on the type of data stored in the stream. The Pravega stream names are organized
within a **Scope**.  A scope is a string containing meaningful informations like "FactoryMachines" or "HRWebsitelogs" to aid in better understanding for the developers. A scope acts as a
namespace for stream names, all stream names are unique within a scope.

A stream is uniquely identified by its combination of the **stream name**
and **scope**. Scope can be used to classify names by tenant (in a multi tenant
environment) or by department in an organization or by geographic location or any
other categorization desired by the developer.

A stream is unbounded in size. Pravega does not impose any limits on the occurrence of number of events in the stream or on the number of total bytes that are stored in a stream.
Pravega’s design architecture scales up horizontally from few machines into a datacenter.

Pravega streams are divided into **Stream Segments**, to handle the large volume of data within a stream. A stream segment is a shared, or partition of the data within a stream. For more information, please see [Stream Segments](#stream-segments) section.

The data from the IoT sensor is extracted or read by a variety of applications (_e.g.,_ Java applications) and writes the read or fetched data to the tail (front) of the stream. Applications, such as a [Flink](https://flink.apache.org), can read from any point in the stream. Many applications can read and write the same stream in parallel. Elasticity, scalability, support for large volume of
stream data and applications are the highlights of Pravega's design. More information on read and write operations in the streams will be discussed in the [Readers and Writers](#writer) section.

## Events

Pravega's client API allows applications to read and write data in Pravega using **Events**.  An event is represented as a set of bytes within a stream. For example, an event could be as simple as a small number of bytes containing a temperature reading from an IoT sensor composed of
a timestamp, a metric identifier and a value or an event could be web log data
associated with a user click on a website. Applications make sense of events using
standard Java **Serializers** and **Deserializers**, allowing them to read and write
objects in Pravega similar to reading and writing objects from
files.

Every event has a **Routing Key**. A routing key is a string used by developers to group similar events. A routing key is often derived from data naturally occurring in the event,
like "customer-id" or "machine-id" or a declared/user-defined string. For example, a routing key could be a date (to group Events together by time) or it could be a IoT sensor id (to group Events by
machine). A routing key is important in defining the read and write semantics that Pravega guarantees.

## Writers, Readers, ReaderGroups <a name= "writer"></a>

![Reader Client](img/producer.consumer.client.new.png)

Pravega provides a client library, written in Java, that implements a convenient
API for writer and reader applications.  The Pravega Java client library
encapsulates the wire protocol used for communication between Pravega clients and
Pravega.

- **Writer:** An application that creates events and writes them into a stream.
All data is written by appending to the tail (front) of a stream.

- **Reader:** An application that reads events from a stream.  Readers can read
from any point in the stream.  Many readers will be reading events from the tail
of the Stream. The tail reads correspond to reading bytes that have been recently written and will be delivered to readers immediately. Some readers will read from earlier parts or history of the stream (called **catch-up reads** or **history reads**). The application developer has control over the reader's start position in the stream.

- **Position:** A concept in Pravega, that represents where in a stream a
reader is currently located. The position object can be used as a recovery
mechanism by replacing the failed reader by a new reader by restoring the last saved successful read position. Using this pattern of persisting position objects, applications can be built guaranteeing exactly once event processing by handling the reader failure.

- **Reader Groups:** Readers are organized into reader groups. A reader group is a named collection of
readers, which together performs parallel read events in a given stream. When a
reader is created through the Pravega data plane API, the developer includes the
name of the reader group associated with it. Pravega guarantees that each event published
to a stream is sent to exactly one reader within the reader group. There could
be one or more reader in the reader group and there could be many different reader groups simultaneously reading from any given stream.

A reader group can be considered as a "composite reader" or "distributed
reader", that allows a distributed application to read and process stream data
in parallel. A large amount of stream data can be consumed by a coordinated fleet of readers in a reader group.  For example, a collection of Flink tasks processing stream data in parallel using reader group.

For more details on the basics of working with Pravega readers and writers, please see [Working with Pravega: Basic Reader and
Writer](basic-reader-and-writer.md#working-with-pravega-basic-reader-and-writer).

## Stream Segments

A stream is decomposed into a set of segments generally referred as **Stream Segments**; a stream segment is a shared or partition of a stream.

![Stream Segment](img/stream.segment.png) 

### Event in a Stream Segment

The stream segments acts as containers for events within the stream. When an
event is written into a stream, it is stored in one of the stream segments based
on the event's routing key. Pravega uses consistent hashing to assign events to
stream segments. Event routing keys are hashed to form a "key space". The key
space is then divided into a number of partitions, corresponding to the number
of stream segments. Consistent hashing determines the allotment of the specific segment to an event.


### Auto Scaling

Varying the number of stream segments over time is referred as **Auto Scaling**. The number of stream segments in a stream can *grow* and *shrink* over time based on the variation in the I/O
load on the stream.

Consider the following figure that shows the relationship between routing keys
and time.

![Stream Segment](img/segment.split.merge.overtime.new.png) 

- A stream starts at time **_t0_** with a configurable number of segments.  If the
rate of data written to the stream is constant, there will be no change in the number of segments. 

- At time **_t1_**, the system noted an increase in the ingestion rate and splits **_Segment 1_** into two parts. This process is referred as **Scale-up** event.

- Before **_t1_**, events with a routing key that hashes to the upper part of the key
space (i.e., values ranging from 200-399) would be placed in **_Segment 1_** and those that hash into the
lower part of the key space (i.e., values ranging from 0-199) would be placed in **_Segment 0_**.

- After **_t1_**, **_Segment 1_** is split into **_Segment 2_** and **_Segment 3_**. The **_Segment 1_** is sealed and stops accepting writes.  At this point in time, Events with routing key **_300_** and _above_ are written to **_Segment 3_** and those between **_200_** and **_299_** would be written into **_Segment 2_**.

- **_Segment 0_** continues accepting the same range of events as before **_t1_**.  

- Another scale-up event occurs at time **_t2_**, as **_Segment 0_**’s range of routing
key is split into **_Segment 5_** and **_Segment 4_**. Also at this time, **_Segment 0_** is sealed
and allows no further writes.

- Segments covering a contiguous range of the key space can also be merged. At
time **_t3_**, **_Segment 2_**’s range and **_Segment 5_**’s range are merged to **_Segment 6_** to
accommodate a decrease in the load on the stream.

When a stream is created, it is configured with a **Scaling Policy** that
determines, how a stream handles the varying changes in its load? In the present scenario, Pravega has three kinds of scaling policy:

1.  **Fixed**:  The number of stream segments does not vary with load.

2.  **Size-based**:  A target rate is set,to decide on increasing or decreasing the number of stream segments. If the number of bytes of data per second written to the stream increases beyond the threshold or target rate, the number of stream segments is
    increased else if it falls below the target rate then the number of stream
    segments are reduced.

3.  **Event-based**:  It is similar to the size-based scaling policy, but it uses number of events instead bytes.

### Events, Stream Segments and AutoScaling


As it was mentioned in the earlier part of the section, that an event is written into one of the stream segments. By considering auto scaling, stream segments performs bucketing of events based on routing key and time. It is obvious that, at any given time, events published to a stream with a given value of routing key will appear in the same stream segment.

![Stream Segment](img/rk.segment.new.png) 

It is also worth emphasizing that events are written only on the active stream
segments. Segments that are sealed do not accept writes. In the figure above,
at time _"now"_, only stream _Segments 3_, _6_ and _4_ are active and the entire key space is covered between those three stream segments.  

### Stream Segments and ReaderGroups

Stream segments play a major role in understanding the way reader groups work.

![Stream Segment](img/segment.readergroup.png) 

Pravega assigns _zero_ or more stream segments to each reader in a reader group. Pravega tries to balances the number of stream segments assigned to each reader. In the figure above, _Reader B1_ reads from two stream segments (*Segment 0* and *Segment 3*), while the other reader group (*Reader B2*, *Reader B3*) have only only one stream segment to read from. Pravega makes sure that each stream segment is read exactly by one reader in any reader group configured with that stream. Irrespective of  readers being added to the reader group or removed from the reader group due to crash, Pravega reassigns stream segments to maintain balance amongst the readers.

The number of stream segments in a stream determines the upper bound of
parallelism of readers within a reader group. If there are more stream segments, different reader groups and many parallel sets of readers can effectively consume the stream. In the
above figure, _Stream 1_ has four stream segments. The largest effective reader group would contain four readers. _Reader group B_ in the above figure is not quite optimal. If one more reader was added to the reader group, each reader would have one stream segment to process, and maximizes the read
parallelism. But, if the number of readers in the reader group increases beyond four, there arises a possibility that at least one of the readers will have unassigned stream segment.


If _Stream 1_ in the figure above experienced a **Scale-Down** event, by reducing the
number of stream segments to three, then the _Reader group B_  will have an
ideal number of readers.

The number of stream segments can be varied dynamically by using the Pravega's auto scaling feature as we discussed in the [Auto Scaling](#auto-scaling) section. The size of any stream is determined by the storage capacity of the Pravega cluster. More streams can be obtained by increasing the storage of the Pravega cluster.

Applications can react to changes in the number of segments in a stream by adjusting the number of readers within a reader group, to maintain optimal read parallelism. Flink application is the best example for this scenario as it allows Flink to increase or decrease the number of task instances that are processing a stream in parallel.

### Ordering Guarantees

A stream comprises a set of segments that can change over time. Segments that overlap in their area of key space have a defined order.

An event written to a stream is written to a single segment, and is ordered with respect to the events of that segment. The existence and position of an event within a segment maintains consistency.

Readers can be assigned multiple parallel segments (from different parts of key space). A reader reading from multiple segments will interleave the events of the segments, but the order of events per segment is retained. Specifically, if _s_ is a segment, and _s_ contains two events _i.e., _s = {_e~1_,_e~2}_ where _e~1_ precedes _e~2_. Thus when a reader tries to read both the events (_e~1_ and _e~2_) the read order is guaranteed by assuring that, the reader is allowed to read _e~1_ before _e~2_.

This results in the following ordering guarantees:

1.  Events with the same routing key are consumed in the order they were written.

2.  Events with different routing keys are sent to a specific segment and will always be
    read in the same order even if the reader performs backs up and re-reads.

3.  If an event has been acknowledged to its writer or has been read by a reader it is guaranteed that  it will continue to exist in the same location or position for all subsequent reads until it is deleted.

4.  If multiple readers are reading a stream and backs up for a while and then again when it performs re-reads, it is assured that no reordering would have happened.

## Reader Group Checkpoints

Pravega provides the ability for an application to initiate a **Checkpoint** on a
reader group.  The idea with a checkpoint is to create a consistent "point in
time" persistence of the state of each reader in the reader group, by using a
specialized event (_Checkpoint Event_) to signal each reader to preserve its
state. Once a checkpoint has been completed, the application can use the
checkpoint to reset all the readers in the reader group to the known consistent
state represented by the checkpoint.

For more details on working with reader groups, Please see [ReaderGroup Basics](basic-reader-and-writer.md#readergroup-basics).


## Transactions

Pravega supports Transactions. The idea of a _Transaction_ is that a writer can
"batch" up a bunch of events and commit them as a unit into a stream. This is
useful, for example, with Flink jobs, using Pravega as a sink.  The Flink job
can continuously produce results of for some data processing and use the transaction
to durably accumulate the results of the processing. For example, at the end of some sort of
time window, the Flink job can commit the transaction and therefore
make the results of the processing available for downstream processing, or in
the case of an error, the transaction is aborted and the results disappear.

A key difference between Pravega's transactions and similar approaches (Kafka's producer-side batching) is related to durability. Events added to a transaction are durable when the event is acknowledged'd back to the writer. However, the events in the transaction are **not** visible to readers until the transaction is committed by the writer. A transaction is a similar to a Stream; a transaction
is associated with multiple stream segments.  When an event is published into a
transaction, the event itself is appended to a stream segment of the
transaction. For example, a stream has five segments, when a transaction is created on that
stream, conceptually that transaction also has five segments. When an event is
published into the transaction, it is routed and assigned to the same numbered segment similar to stream (i.e.,Event assigned to _Segment 3_ in the stream will be assigned _Segment 3_ in the transaction). Once the transaction is committed, each of the transaction's
segments is automatically appended to the corresponding segment in the stream. If the Stream is aborted, the transaction, all its segments and all the events published into the transaction are removed from Pravega.

![Transaction](img/trx.commit.new.png) 

Events published into a transaction are visible to the reader only after the transaction is committed.

For more details on working with transactions, Please see [Working with Pravega:
Transactions](transactions.md).

## State Synchronizers

Pravega is a streaming storage primitive and can also coordinate processes in a distributed computing environment. A **State Synchronizer** uses a Pravega stream to provide a synchronization
mechanism for state shared between multiple processes running in a cluster and making it easier to build distributed applications.  With state synchronizer, an application developer can use Pravega to read and make changes to shared state consistently and perform optimistic locking. 

![State synchroner](img/state.synchronizer.png) 

State synchronizer could be used to maintain a single, shared copy of an
application's configuration property across all instances of that application in
a cloud.  State synchronizer could also be used to store one piece of data or a
map with thousands of different key value pairs. In Pravega, managing the state of reader groups and distribution of readers throughout the network is implemented using state synchronizer.

An application developer creates a state synchronizer on a stream similar to the creation of writer.  The state synchronizer keeps a local copy of the shared state and allows faster access to the data in the application. State synchronizer keeps track of all the changes happening in the shred state and responsible for performing any modification to the shared state in the stream. Each application instance uses the state synchronizer, to remain updated with the
changes by pulling updates to the shared state and modifies the local copy of the
data. Consistency is maintained through a conditional append style of updates
to the shared state through the state synchronizer, making sure that updates are
made only to the most recent version of the shared state.

The State Synchronizer can occasionally be "compacted", by compressing and removing
older updates and retains only the recent version of the state in the backing stream. This feature assures the application developers, that the shared state does not grow unchecked.

State synchronizer works effectively when most updates to shared state are small in
comparison to the total data size being stored. This can be achieved by allowing them to be written as
small deltas. As with any optimistic concurrency system, state synchronizer is
not at its best when many processes attempts for simultaneous updates on the same piece of data.

For more details on working with state synchronizers, please see [Working with Pravega:
State Synchronizer](state-synchronizer.md).

## Architecture

The following figure depicts the components deployed by Pravega:

![pravega high level architecture](img/pravega.arch.new.png)

Pravega is deployed as a distributed system – a cluster of servers and storage
coordinated to run Pravega called a **Pravega cluster**.  

Pravega presents a software-defined storage (SDS) architecture formed by **Controller** instances
(_control plane_) and Pravega Servers (_data plane_).The set of Pravega Servers is collectively known as the **Segment Store**. 

The set of controller instances together forms the control plane of Pravega, providing
functionality to _create, update_ and _delete_ _streams_. Further extend functionality to retrieve information about the streams, monitor the health of the Pravega cluster, gather metrics and etc.,. Multiple (recommended at least 3) controller instances are allowed to be running in a
cluster for high availability.  

The [segment store](segment-store-service.md) implements the Pravega data plane.
Pravega servers provide the API to read and write data in streams. Data storage is comprised of two tiers:
- **Tier 1 Storage:** It provides short term, low-latency, data storage, guaranteeing the durability of data written to streams. Pravega uses [Apache Bookkeeper](http://bookkeeper.apache.org/) to implement
Tier 1 storage.
- **Tier 2 Storage:** It provides long term storage of stream data. pravega uses HDFS, Dell EMC's Isilon or Dell EMC's Elastic Cloud Storage (ECS) to implement Tier 2 Storage.

Tier 1 Storage typically runs within the Pravega cluster. Tier 2 Storage is normally deployed outside the Pravega cluster.

Tiering storage is important to deliver the combination of fast access to stream
data and also allows large data storage for streams. Tier 1 storage
persists on the most recently written stream data. As data in Tier 1 storage ages,
it is moved into Tier 2 Storage.

Pravega uses [Apache Zookeeper](https://zookeeper.apache.org/) as the
coordination mechanism for the components in the Pravega cluster.  

Pravega is built as a data storage primitive first and foremost.  Pravega is
carefully designed to take advantage of software defined storage so that the
amount of data stored in Pravega is limited only by the total storage capacity
of the data center. Once the data is written to Pravega it is durably stored.  Short of a disaster, that
permanently destroys a large portion of a data center but data stored in Pravega is never
lost.

Pravega provides a client library, written in Java, for building client-side
applications such as analytics applications using Flink. The Pravega Java client
library manages the interaction between the application code and Pravega via a
custom TCP wire protocol.


## Putting the Concepts Together

The concepts in Pravega are depicted in the following figure:

![State synchroner](img/putting.all.together.new.png) 

-   Pravega clients are **Writers** and **Readers**.  Writers write events into a
    stream. Readers read events from a stream. Readers are grouped into
    **Reader Groups** to read from a stream in parallel.

-   The **Controller** is a server-side component that manages the control plane of
    Pravega.  Streams are created, updated and listed using the controller API.

-   The **Pravega Server** is a server-side component that implements reads, writes
    and other data plane operations.

-   **Streams** are the fundamental storage primitive in Pravega.  Streams contain a
    set of data elements called events.  Events are appended to the “tail” of
    the stream by writers.  Readers can read events from anywhere in the stream.

-   A stream is partitioned into a set of **Stream Segments**. The number of stream
    segments in a stream can change over time.  Events are written into exactly
    one of the stream segments based on **Routing Key**.  For any reader group,
    reading a Stream, each stream segment is assigned to one reader in that
    reader group. 

-   Each stream segment is stored in a combination of **Tier 1** and **Tier 2** storage. 
    The tail of the segment is stored in Tier 1 providing low latency reads and
    writes. The rest of the segment is stored in Tier 2, providing high
    throughput read access with horizontal scalability and low cost. 

## A Note on Tiered Storage

To deliver an efficient implementation of streams, Pravega is based on a tiered
storage model.  Events are persisted in low latency/high IOPS storage (Tier 1
storage) and higher throughput storage (Tier 2 storage). Writers and readers are
oblivious to the tiered storage model from an API perspective. 

Pravega is based on an append-only **log** data structure.  As Leigh Stewart
[observed](https://blog.twitter.com/2015/building-distributedlog-twitter-s-high-performance-replicated-log-service),
there are really three data access mechanisms in a log:

![State synchroner](img/anatomy.of.log.png) 

All of the write activity, and much of the read activity happens at the tail of
the log.  Writes are appended to the log and many clients tries to read data immediately as it arrives in the log.  These two data access mechanisms are dominated
by the need for low-latency – low latency writes by writers and provide real time
access to the published data by readers. 

Please noete that, not all readers read from the tail of the log, some readers reads
by starting at some arbitrary position in the log.  These reads are known as
**catch-up reads**.  Access to historical data traditionally was done by batch
analytics jobs, often using HDFS and Map/Reduce.  However with new streaming
applications, we can access historical data as well as current data by just
accessing the log.  One approach would be to store all the historical data in
SSDs similar to tail data operations, but that leades to an expensive task and force
customers to economize by deleting historical data.  Pravega offers a mechanism
that allows customers to use cost-effective, highly-scalable, high-throughput
storage for the historical part of the log, that way they won’t have to decide on
when to delete historical data.  Basically, if storage is cheap enough, why not
keep all of the history?

Tier 1 Storage is used to make writing to Streams fast and durable and to make
sure reading from the tail of a Stream is as fast as possible.  Tier 1 Storage
is based on the open source Apache BookKeeper Project. Though not essential, we
presume that the Tier 1 Storage will be typically implemented on faster SSDs or
even non-volatile RAM.

Tier 2 Storage provides a highly-scalable, high-throughput cost-effective
storage. We expect this tier to be typically deployed on spinning disks. Pravega
asynchronously migrates Events from Tier 1 to Tier 2 to reflect the different
access patterns to Stream data.  Tier 2 Storage is based on an HDFS model. 
