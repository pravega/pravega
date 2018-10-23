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
similar to but more flexible than a "topic" in popular message-oriented middleware such as [RabbitMQ](https://www.rabbitmq.com/) or [Apache Kafka](https://kafka.apache.org/).

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
within a **Scope**.  A Scope is a string containing meaningful informations like "FactoryMachines" or "HRWebsitelogs" to aid in better understanding for the developers. A scope acts as a
namespace for stream names, all stream names are unique within a scope.

A stream is uniquely identified by its combination of the **stream name**
and **scope**. Scope can be used to classify names by tenant (in a multi tenant
environment) or by department in an organization or by geographic location or any
other categorization desired by the developer.

A stream is unbounded in size. Pravega does not impose any limits on the occurrence of number of events in the stream or on the number of total bytes that are stored in a stream.
Pravega’s design architecture scales up horizontally from few machines into a datacenter.

Pravega streams are divided into **Stream Segments**, to handle the large volume of data within a stream. A stream segment is a shared, or partition of the data within a stream. For more information, please see [Stream Segments](#stream-segments) section.

The data from the IoT sensor is extracted or read by a variety of applications (_e.g.,_ Java applications) and write the read or fetched data to the tail (front) of the stream. Applications, such as a [Flink](https://flink.apache.org), can read from any point in the stream. Many applications can read and write the same stream in parallel. Elasticity, scalability, support for large volume of
streams, data and applications are the highlights of the Pravega's design. More information on reads and writes in the streams will be discussed in the [Readers and Writers](#writer) section.

## Events

Pravega's client API allows applications to read and write data in Pravega using **Event**.  An event is a set of bytes within a stream. Events are represented as a set of bytes. For _e.g.,_ an event could be as simple as a small number of bytes containing a temperature reading from an IoT sensor composed of
a timestamp, a metric identifier and a value or an event could be web log data
associated with a user click on a website. Applications make sense of events using
standard Java _serializers_ and _deserializers_, allowing them to read and write
objects in Pravega similar to reading and writing objects from
files.

Every event has a **Routing Key**. A routing key is a string used by developers to group similar events. A routing key is often derived from data naturally occurring in the event,
like "customer-id" or "machine-id" or a declared/user-defined string. For _e.g.,_ routing key could be a date (to group Events together by time) or it could be a IoT sensor id (to group Events by
machine). A routing key is important in defining the read and write semantics that Pravega guarantees.

## Writers, Readers, ReaderGroups <a name= "writer"></a>

![Reader Client](img/producer.consumer.client.new.png)

Pravega provides a client library, written in Java, that implements a convenient
API for writer and reader applications.  The Pravega Java Client Library
encapsulates the wire protocol used for communication between Pravega clients and
Pravega.

A **Writer** is an application that creates events and writes them into a stream.
All data is written by appending to the tail (front) of a stream.

A **Reader** is an application that reads events from a stream.  Readers can read
from any point in the stream.  Many readers will be reading events from the tail
of the Stream. The tail reads correspond to reading bytes that have been recently written. and will be delivered to readers immediately. Some readers will read from earlier parts or stored history of the stream (called catch-up reads or history reads). The application developer has control over the reader's start position in the stream.

Pravega has the concept of a **Position**, that represents where in a stream a
reader is currently located. The position object can be used as a recovery
mechanism by replacing the failed reader by a new reader by restoring the last saved successful read position. Using this pattern of persisting position objects, applications can be built guaranteeing exactly once event processing by handling the reader failure.

Readers are organized into **Reader Groups**.  A reader group is a named collection of
readers, which together performs parallel read events in a given stream. When a
reader is created through the Pravega data plane API, the developer includes the
name of the reader group associated with it. Pravega guarantees that each event published
to a stream is sent to exactly one reader within the reader group.  There could
be one or more reader in the reader group. There could be many different reader groups simultaneously reading from any given stream.

A reader group can be considered as a "composite reader" or a "distributed
reader", that allows a distributed application to read and process stream data
in parallel. A large amount of stream data can be consumed by a coordinated fleet of readers in a reader group.  For _e.g.,_  a collection of Flink tasks, processing stream data in parallel is a good example for reader group usage.

For more details on the basics of working with Pravega Readers and Writers, please see [Working with Pravega: Basic Reader and
Writer](basic-reader-and-writer.md#working-with-pravega-basic-reader-and-writer).

## Stream Segments

A stream is decomposed into a set of segments generally referred as **Stream Segments**; a stream segment is a shared or partition of a Stream.

![Stream Segment](img/stream.segment.png) 

### Event in a Stream Segment

The stream segments are container for events within the stream. When an
event is written into a stream, it is stored in one of the stream segments based
on the event's routing key. Pravega uses consistent hashing to assign events to
stream segments. Event routing keys are hashed to form a "key space". The key
space is then divided into a number of partitions, corresponding to the number
of stream segments. Consistent hashing determines in assigning the segment to an event.


### Auto Scaling

Varying the number of stream segments over time is referred as Auto Scaling. The number of stream segments in a stream can *grow* and *shrink* over time based on the variation in the I/O
load on the stream.

Consider the following figure that shows the relationship between routing keys
and time.

![Stream Segment](img/segment.split.merge.overtime.new.png) 

- A stream starts at time _t0_ with a configurable number of segments.  If the
rate of data written to the stream is constant, there will be no change in the number of segments. 

- At time _t1_, the system noted an increase in the ingestion rate and splits _Segment 1_ into two parts. This process is referred as **Scale-up** event.

- Before _t1_, events with a routing key that hashes to the upper part of the key
space (i.e., values ranging from 200-399) would be placed in _Segment 1_ and those that hash into the
lower part of the key space (i.e., values ranging from 0-199) would be placed in _Segment 0_.

- After _t1_, _Segment 1_ is split into _Segment 2_ and _Segment 3_. _Segment 1_ is sealed and stops accepting writes.  At this point in time, Events with routing key _300_ and _above_ are written to _Segment 3_ and those between _200_ and _299_ would be written into _Segment 2_.

- _Segment 0_ continues accepting the same range of events as before _t1_.  

- Another scale-up event occurs at time _t2_, as _Segment 0_’s range of routing
key is split into _Segment 5_ and _Segment 4_. Also at this time, _Segment 0_ is sealed
and allows no further writes.

- Segments covering a contiguous range of the key space can also be merged. At
time _t3_, _Segment 2_’s range and _Segment 5_’s range are merged to _Segment 6_ to
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

Stream Segments are important to understanding the way Reader Groups work.

![Stream Segment](img/segment.readergroup.png) 

Pravega assigns each Reader in a ReaderGroup zero or more Stream Segments to
read from.  Pravega tries to balance out the number of Stream Segments each
Reader is assigned.  In the figure above, Reader B1 reads from 2 Stream Segments
while each of the other Readers in the Reader Group have only 1 Stream Segment
to read from.  Pravega makes sure that each Stream Segment is read by exactly
one Reader in any ReaderGroup configured to read from that Stream. As Readers
are added to the ReaderGroup, or Readers crash and are removed from the
ReaderGroup, Pravega reassigns Stream Segments so that Stream Segments are
balanced amongst the Readers.

The number of Stream Segments in a Stream determines the upper bound of
parallelism of readers within a ReaderGroup – the more Stream Segments, the more
separate, parallel sets of Readers we can have consuming the Stream. In the
above figure, Stream1 has 4 Stream Segments.  That means that the largest
effective Reader Group would contain 4 Readers.  Reader Group named "B" in the
above figure is not quite optimal.  If one more Reader was added to the
ReaderGroup, each Reader would have 1 Stream Segment to process, maximizing read
parallelism.  However, the number of Readers in the ReaderGroup increases beyond
4, at least one of the Readers will not be assigned a Stream Segment.

If Stream1 in the figure above experienced a Scale-Down event, reducing the
number of Stream Segments to 3, then Reader Group B as depicted would have an
ideal number of Readers.

With the AutoScaling feature, Pravega developers don't have to configure their
Streams with a fixed, pre-determined number of Stream Segments – Pravega can
dynamically determine the right number.  With this feature, Pravega Streams can
grow and shrink to match the behavior of the data input.  The size of any Stream
is limited only by the total storage capacity made available to the Pravega
cluster; if you need bigger streams, simply add more storage to your cluster.

Applications can react to changes
in the number of Segments in a Stream, adjusting the number of Readers within a
ReaderGroup, to maintain optimal read parallelism if resources allow.  This is
useful, for example in a Flink application, to allow Flink to increase or
decrease the number of task instances that are processing a Stream in parallel,
as scale events occur over time.

### Ordering Guarantees

A stream comprises a set of segments that can change over time. Segments that overlap in their area of keyspace have a defined order.

An event written to a stream is written to a single segment and it is totally ordered with respect to the events of that segment. The existance and position of an event within a segment is strongly consistent.

Readers can be assigned multiple parallel segments (from different parts of keyspace). A reader reading from multiple segments will interleave the events of the segments, but the order of events per segment respects the one of the segment. Specifically, if s is a segment, events e~1 and e~2 of s are such that e~1 precedes e~2, and a reader reads both e~1 and e~2, then the reader will read e~1 before e~2.

This results in the following ordering guarantees:

1.  Events with the same Routing Key are consumed in the order they were written.

2.  Events with different Routing Keys sent to a specific segment will always be
    seen in the same order even if the Reader backs up and re-reads them.

3.  If an event has been acked to its writer or has been read by a reader it is guaranteed that it will continue to exist in the same place for all subsequent reads until it is deleted.

4.  If there are multiple Readers reading a Stream and they all back up to any given point, they will never see any reordering with respect to that point. (It will never be the case that an event that they read before the chosen point now comes after or vice versa.)

## ReaderGroup Checkpoints

Pravega provides the ability for an application to initiate a Checkpoint on a
ReaderGroup.  The idea with a Checkpoint is to create a consistent "point in
time" persistence of the state of each Reader in the ReaderGroup, by using a
specialized Event (a Checkpoint Event) to signal each Reader to preserve its
state.  Once a Checkpoint has been completed, the application can use the
Checkpoint to reset all the Readers in the ReaderGroup to the known consistent
state represented by the Checkpoint.

For more details on working with ReaderGroups,
see [ReaderGroup Basics](basic-reader-and-writer.md#readergroup-basics).


## Transactions

Pravega supports Transactions.  The idea of a Transaction is that a Writer can
"batch" up a bunch of Events and commit them as a unit into a Stream.  This is
useful, for example, with Flink jobs, using Pravega as a sink.  The Flink job
can continuously produce results of some data processing and use the Transaction
to durably accumulate the results of the processing.  At the end of some sort of
time window (for example) the Flink job can commit the Transaction and therefore
make the results of the processing available for downstream processing, or in
the case of an error, abort the Transaction and the results disappear.

A key difference between Pravega's Transactions and similar approaches (such as
Kafka's producer-side batching) is related to durability.  Events added to a
Transaction are durable when the Event is ack'd back to the Writer.  However,
the Events in the Transaction are NOT visible to readers until the Transaction
is committed by the Writer.  A Transaction is a lot like a Stream; a Transaction
is associated with multiple Stream Segments.  When an Event is published into a
Transaction, the Event itself is appended to a Stream Segment of the
Transaction.  Say a Stream had 5 Segments, when a Transaction is created on that
Stream, conceptually that Transaction also has 5 Segments.  When an Event is
published into the Transaction, it is routed to the same numbered Segment as if
it were published to the Stream itself (if the Event would have been placed in
Segment 3 in the "real" Stream, then it will appear in Segment 3 of the
Transaction).  When the Transaction is committed, each of the Transaction's
Segments is automatically appended to the corresponding Segment in the real
Stream.  If the Stream is aborted, the Transaction, all its Segments and all the
Events published into the Transaction are removed from Pravega.

![Transaction](img/trx.commit.new.png) 

Events published into a Transaction are never visible to the Reader until that
Transaction is committed.

For more details on working with Transactions, see [Working with Pravega:
Transactions](transactions.md).

## State Synchronizers

Pravega is a streaming storage primitive; it can also be thought of as a
mechanism to coordinate processes in a distributed computing environment.  The
State Synchronizer feature of Pravega falls into the latter category.

A State Synchronizer uses a Pravega Stream to provide a synchronization
mechanism for state shared between multiple processes running in a cluster,
making it easier to build distributed applications.  With State Synchronizer, an
app developer can use Pravega to read and make changes to shared state with
consistency and optimistic locking. 

![State synchroner](img/state.synchronizer.png) 

State Synchronizer could be used to maintain a single, shared copy of an
application's configuration property across all instances of that application in
a cloud.  State Synchronizer could also be used to store one piece of data or a
map with thousands of different key value pairs.  In fact, Pravega itself uses
State Synchronizer internally, to manage the state of ReaderGroups and Readers
distributed throughout the network.

An app developer creates a State Synchronizer on a Stream in a fashion similar
to how s/he creates a Writer.  The State Synchronizer keeps a local copy
of the shared state to make access to the data really fast for the application.
 Any changes to shared state are written through the StateSynchronizer to the
Stream keeping track of all changes to the shared state.
 Each application instance uses the State Synchronizer to stay up to date with
changes by pulling updates to shared state and modifying the local copy of the
data.  Consistency is maintained through a conditional append style of updates
to the shared state through the State Synchronizer, making sure that updates are
made only to the most recent version of the shared state.

The State Synchronizer can occasionally be "compacted", compressing and removing
old state updates so that only the most recent version of the state is kept in
the backing stream.  This feature helps app developers make sure that shared
state does not grow unchecked.

 

State Synchronizer works best when most updates to shared state are small in
comparison to the total data size being stored, allowing them to be written as
small deltas. As with any optimistic concurrency system, State Synchronizer is
not at its best when many processes are all attempting to simultaneously update
the same piece of data.

For more details on working with State Synchronizers, see [Working with Pravega:
State Synchronizer](state-synchronizer.md).

## Architecture

The following figure depicts the components deployed by Pravega:

![pravega high level architecture](img/pravega.arch.new.png)

Pravega is deployed as a distributed system – a cluster of servers and storage
coordinated to run Pravega called a "Pravega cluster".  

Pravega presents a software-defined storage (SDS) architecture formed by Controller instances
(control plane) and Pravega Servers (data plane).The set of Pravega Servers is known collectively as the Segment Store. 

The set of Controller instances make up the control plane of Pravega, providing
functionality to create, update and delete Streams, retrieve information about
Streams, monitor the health of the Pravega cluster, gather metrics etc.  There
are usually multiple (recommended at least 3) Controller instances running in a
cluster for high availability.  

The [Segment Store](segment-store-service.md) implements the Pravega data plane.
 Pravega Servers provide the API to read and write data in Streams.  Data
storage is comprised of two tiers: Tier 1 Storage, which provides short term,
low-latency, data storage,  guaranteeing the durability of data written to
Streams and Tier 2 Storage providing longer term storage of Stream data.
 Pravega uses [Apache Bookkeeper](http://bookkeeper.apache.org/) to implement
Tier 1 Storage and uses HDFS, Dell EMC's Isilon or Dell EMC's Elastic Cloud
Storage (ECS) to implement Tier 2 Storage.  Tier 1 Storage typically runs within
the Pravega cluster.  Tier 2 Storage is normally deployed outside the Pravega
cluster.

Tiering storage is important to deliver the combination of fast access to Stream
data but also allow Streams to store a vast amount of data.  Tier 1 storage
persists the most recently written Stream data.  As data in Tier 1 Storage ages,
it is moved into Tier 2 Storage.

Pravega uses [Apache Zookeeper](https://zookeeper.apache.org/) as the
coordination mechanism for the components in the Pravega cluster.  

Pravega is built as a data storage primitive first and foremost.  Pravega is
carefully designed to take advantage of software defined storage so that the
amount of data stored in Pravega is limited only by the total storage capacity
of your data center.  And like you would expect from a storage primitive, once
data is written to Pravega it is durably stored.  Short of a disaster that
permanently destroys a large portion of a data center, data stored in Pravega is never
lost.

Pravega provides a client library, written in Java, for building client-side
applications such as analytics applications using Flink. The Pravega Java Client
Library manages the interaction between application code and Pravega via a
custom TCP wire protocol.


## Putting the Concepts Together

The concepts in Pravega are summarized in the following figure:

![State synchroner](img/putting.all.together.new.png) 

-   Pravega clients are Writers and Readers.  Writers write Events into a
    Stream.  Readers read Events from a Stream.  Readers are grouped into
    ReaderGroups to read from a Stream in parallel.

-   The Controller is a server-side component that manages the control plane of
    Pravega.  Streams are created, updated and listed using the Controller API.

-   The Pravega Server is a server-side component that implements read, write
    and other data plane operations.

-   Streams are the fundamental storage primitive in Pravega.  Streams contain a
    set of data elements called Events.  Events are appended to the “tail” of
    the Stream by Writers.  Readers can read Events from anywhere in the Stream.

-   A Stream is partitioned into a set of Stream Segments. The number of Stream
    Segments in a Stream can change over time.  Events are written into exactly
    one of the Stream Segments based on Routing Key.  For any ReaderGroup
    reading a Stream, each Stream Segment is assigned to one Reader in that
    ReaderGroup. 

-   Each Stream Segment is stored in a combination of Tier1 and Tier2 storage. 
    The tail of the Segment is stored in Tier1 providing low latency reads and
    writes.  The rest of the Segment is stored in Tier2, providing high
    throughput read access with horizontal scalibility and low cost. 

## A Note on Tiered Storage

To deliver an efficient implementation of Streams, Pravega is based on a tiered
storage model.  Events are persisted in low latency/high IOPS storage  (Tier 1
Storage) and higher throughput storage (Tier 2 Storage). Writers and Readers are
oblivious to the tiered storage model from an API perspective. 

Pravega is based on an append-only Log data structure.  As Leigh Stewart
[observed](https://blog.twitter.com/2015/building-distributedlog-twitter-s-high-performance-replicated-log-service),
there are really three data access mechanisms in a Log:

![State synchroner](img/anatomy.of.log.png) 

All of the write activity, and much of the read activity happens at the tail of
the log.  Writes are appended to the log and many clients want to read data as
fast as it arrives in the log.  These two data access mechanisms are dominated
by the need for low-latency – low latency writes by Writers and near real time
access to the published data by Readers. 

Not all Readers read from the tail of the log; some Readers want to read
starting at some arbitrary position in the log.  These reads are known as
*catch-up reads*.  Access to historical data traditionally was done by batch
analytics jobs, often using HDFS and Map/Reduce.  However with new streaming
applications, you can access historical data as well as current data by just
accessing the log.  One approach would be to store all the historical data in
SSDs like we do with the tail data, but that can get very expensive and force
customers to economize by deleting historical data.  Pravega offers a mechanism
that allows customers to use cost-effective, highly-scalable, high-throughput
storage for the historical part of the log, that way they won’t have to decide
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
