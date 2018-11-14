<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
<span id="_Toc506543504" class="anchor"><span id="_Toc506545512" class="anchor"></span></span>Pravega Controller Service
========================================================================================================================
*  [Introduction](#introduction)
*  [Architecture](#architecture)
    - [Stream Management](#stream-management)
    - [Cluster Management](#cluster-management)
* [System Diagram](#system-diagram)
* [Components](#components)
    - [Service Endpoints](#service-endpoints)
    - [Controller Service](#controller-service)
    - [Stream Metadata Store](#stream-metadata-store)
        - [Stream Metadata](#stream-metadata)
        - [Stream Store Caching](#stream-store-caching)
    - [Stream Buckets](#stream-buckets)
    - [Controller Cluster Listener](#controller-cluster-listener)
    - [Host Store](#host-store)
    - [Background workers](#background-workers)
* [Roles and Responsibilities](#roles-and-responsibilities)
    - [Stream Operations](#stream-operations)
        - [Stream State](#stream-state)
        - [Create Stream](#create-stream)
        - [Update Stream](#update-stream)
        - [Scale Stream](#scale-stream)
        - [Truncate Stream](#truncate-stream)
        - [Seal Stream](#seal-stream)
        - [Delete Stream](#delete-stream)
    - [Stream Policy Manager](#stream-policy-manager)
        - [Scaling Infrastructure](#scaling-infrastructure)
        - [Retention Infrastructure](#retention-infrastructure)
    - [Transaction Manager](#transaction-manager)
        - [Create Transaction](#create-transaction)
        - [Commit Transaction](#commit-transaction)
        - [Abort Transaction](#abort-transaction)
        - [Ping Transaction](#ping-transaction)
        - [Transaction Timeout Management](#transaction-timeout-management)
    - [Segment Container to Host Mapping](#segment-container-to-host-mapping)
* [Resources](#resources)

# Introduction

The Controller service is a core component of Pravega that implements
the control plane. It acts as the central coordinator and manager for
various operations performed in the cluster, the major two
categories are: **Stream Management** and **Cluster Management**.

The Controller service, referred to simply as **Controller** henceforth, is
responsible for providing the abstraction of a Pravega [Stream](pravega-concepts.md#streams), which is the main abstraction that Pravega exposes to applications. A Pravega Stream
comprises one or more Stream [Segments](pravega-concepts.md#stream-segments). Each Stream Segment is an append-only data
structure that stores a sequence of bytes. A Stream Segment on its own is
agnostic to the presence of other Stream Segments and is not aware of its logical
relationship with its peer Stream Segments. The Segment Store, which owns and
manages these Stream Segments, does not have any notion of a Stream. A Stream
is a logical view conceptualized by Controller by composing a
dynamically changing set of Stream Segments that satisfy a predefined set of
logical invariants. The Controller provides the Stream abstraction and
orchestrates all lifecycle operations on a Pravega Stream while ensuring that
the abstraction stays consistent.

The Controller plays a central role in the lifecycle of a Stream:
_creation_, _modification_, [_scaling_](pravega-concepts.md#elastic-streams-auto-scaling), and _deletion_. It does these by
maintaining metadata per Stream and performs requisite operations on
Stream Segments when required. For example, as part of Stream’s
lifecycle, new segments can be created and existing segments can be sealed. The
Controller decides on performing these operation by ensuring the availability and consistency of the Streams for the clients accessing them.

# Architecture

The Controller Service is made up of one or more instances of
stateless worker nodes. Each new Controller instance can be invoked independently and becomes part of Pravega cluster by merely pointing to the same [Apache Zookeeper](https://zookeeper.apache.org/). For high availability it is advised to have
more than one instance of Controller service per cluster.

Each Controller instance is capable of working independently and uses a
shared persistent store as the source of truth for all state owned and
managed by Controller service. We currently use Apache ZooKeeper as the
store for persisting all metadata consistently.  Each instance comprises
various subsystems which are responsible for performing specific
operations on different categories of metadata. These subsystems include
different *API endpoints, metadata store handles, policy managers* and
*background workers*.

The Controller exposes two endpoints which can be used to interact with
The Pravega Controller Service. The first port is for providing programmatic
access for Pravega clients and is implemented as an `RPC` using [gRPC](https://grpc.io/). The
other endpoint is for administrative operations and is implemented as a
`REST` endpoint.

## Stream Management

The Controller owns and manages the concept of Stream and is
responsible for maintaining "metadata" and "lifecycle" operations (_creating, updating, scaling,
truncating, sealing_ and _deleting Streams_) for each Pravega Stream.

The Stream management can be divided into the following three categories:

  1. **Stream Abstraction**: A Stream can be viewed as a series of dynamically changing segment sets
where the Stream transitions from one set of consistent segments to the
next. Controller is the place for creating and managing Stream abstraction.
Controller decides when and how a Stream transitions from one state to another and is responsible
for performing these transitions by keeping the state of the Stream consistent and available.
These transitions are governed user-defined policies that the
Controller enforces. Consequently, as part of Stream management, the
Controller also performs roles of Policy Manager for policies like
retention and scaling.


  2. **Policy Management**: Controller is responsible for storing and enforcing user-defined Stream policies by actively monitoring the state of the Stream. In Pravega we
have two policies that users can define, namely [**Scaling** **Policy**](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/Stream/ScalingPolicy.java) and
[**Retention** **Policy**](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/Stream/RetentionPolicy.java).

       - **Scaling policy** describes if and under what circumstances a Stream should automatically scale its number of segments.  
       - **Retention policy** describes a policy about how much data to retain within a Stream based on **time** (*Time Based Retention*) and data **size**(*Size Based Retention*).

  3. [**Transaction**](pravega-concepts.md#transactions) **Management**: Implementing Transactions requires the manipulation of Stream Segments. With
each Transaction, Pravega creates a set of Transaction segments, which
are later merged onto the Stream Segments upon commit or discarded upon
aborts. The Controller performs the role of Transaction manager and is
responsible for creating and committing Transactions on a given Stream.
Upon creating Transactions, Controller also tracks Transaction timeouts
and aborts transactions whose timeouts have elapsed. Details of
Transaction management can be found later in the [Transactions](#transaction-manager) section.

## Cluster Management

The Controller is responsible for managing Segment Store cluster. Controller manages
life cycle of Segment Store nodes as they are added to/removed from the
cluster and performs redistribution of segment containers across
available Segment Store nodes.

# System Diagram

The following diagram shows the main components of a Controller process.
The elements of the diagram are discussed in detail in the following sections.

 ![Controller system_diagram](img/ControllerSystem-Diagram.png)

Controller Process Diagram


# Components

## Service Endpoints

There are two ports exposed by Controller: **Client-Controller APIs** and
**Administration APIs**. The client Controller communication is implemented as `RPC` which
exposes APIs to perform all Stream related control plane operations.
Apart from this Controller also exposes an administrative APIs implemented as `REST`.

Each endpoint performs appropriate call to the Pravega Controller Service *backend subsystem*
which has the actual implementation for various operations like create, read, update and
delete (CRUD) on entities owned and managed by Controller.

### gRPC  

Client Controller communication endpoint is implemented as a [`gRPC`](https://grpc.io/)
interface. Please check the complete list of [APIs](https://github.com/pravega/pravega/blob/master/shared/Controller-api/src/main/proto/Controller.proto).
This exposes APIs used by Pravega clients (Readers, Writers and Stream
Manager) and enables Stream management. Requests enabled by this API
include *creating, modifying,* and *deleting* Streams.
The underlying `gRPC` framework provides both **_synchronous_** and **_asynchronous_** programming models.
We use the asynchronous model in our client Controller interactions so that the client thread does not block on the response from the server.  

To be able to append to and read data from Streams, Writers and Readers
query Controller to get active Stream Segment sets, successor and predecessor
Stream segments while working with a Stream. For Transactions, the client uses
specific API calls to request Controller to _create_ and _commit_
Transactions.

### REST  
For administration, the Controller implements and exposes a `REST`
interface. This includes API calls for Stream management as well as
other administration API primarily dealing with _creation_ and _deletion_ of
[**Scopes**](pravega-concepts.md#streams). We use swagger to describe our `REST` APIs. Please see, the swagger [`yaml`](https://github.com/pravega/pravega/tree/master/shared/Controller-api/src/main/swagger) file.

## Pravega Controller Service

This is the backend layer behind the Controller endpoints `gRPC` and
`REST`. All the business logic required to serve Controller API calls are
implemented here. This layer contains handles to all other subsystems like the various store implementations
(Stream store, Host store and Checkpoint store) and background processing frameworks (Task and Event Processor framework).
Stores are interfaces that provide access to various types of metadata managed by Controller. Background
processing frameworks are used to perform asynchronous processing that typically implement workflows involving metadata updates
and requests to Segment Store.

## Stream Metadata Store

A Stream is dynamically changing sequence of Stream Segments, where regions of
the Routing Key space map to open Stream Segments. As the set of  Segments of a
Stream changes, so does the mapping of the Routing Key space to Segments.

A set of Segments is consistent if, union of key space regions mapping
to Segments in the set covers the entire key space and no
overlap between key space regions.

For example, suppose a set **S** = {**S**<sub>**1**</sub>, **S**<sub>2</sub>, **S**<sub>**3**</sub>}, such that:

   -   Region \[0, 0.3) maps to Segment **S**<sub>**1**</sub>.
   -   Region \[0.3, 0.6) maps to Segment **S**<sub>**2**</sub>.
   -   Region \[0.6, 1.0) maps to Segment **S**<sub>**3**</sub>.

**S** is a consistent Segment set.  

A Stream goes through transformations as it scales over time. A Stream
starts with an initial set of Segments that is determined by the Stream
configuration when created and it transitions to new sets of Segments as
scale operations are performed on the Stream. Each generation of
Segments that constitute Stream at any given point in time are
considered to belong to an _epoch_. So a Stream starts with initial _epoch 0_
and upon each transition, it moves ahead in its _epochs_
to describe the change in generation of Segments in the Stream.

The Controller maintains the Stream, store the information about all
_epochs_ that constitute a given Stream and also about their transition. The store
is designed to optimally store and query information pertaining to
Stream Segments and their inter-relationships.

Apart from the _epoch_ information, it keeps some additional metadata,
such as [state](#Stream-state) and its [policies](#Stream-policy-manager) and ongoing Transactions on the Stream. Various sub-components of Controller access the stored metadata for each
Stream via a well-defined
[interface](https://github.com/pravega/pravega/blob/master/Controller/src/main/java/io/pravega/controller/store/Stream/StreamMetadataStore.java).
We currently have two concrete implementations of the Stream store
interface: _in-memory_ and _Zookeeper_ backed stores. Both share a common
base implementation that relies on Stream objects for providing
store-type specific implementations for all Stream-specific metadata.
The base implementation of Stream store creates and caches these Stream
objects.

The Stream objects implement a store/Stream interface. The concrete
Stream implementation is specific to the store type and is responsible
for implementation of store specific methods to provide consistency and
correctness. We have a common base implementation of all store types
that provide optimistic concurrency. This base class encapsulates the
logic for queries against Stream store for all concrete stores that
support Compare And Swap (CAS). We currently use Zookeeper as our
underlying store which also supports CAS. We store all Stream metadata
in a hierarchical fashion under Stream specific **znodes** (ZooKeeper
data nodes).

For the ZooKeeper based store, we structure our metadata into different
groups to support a variety of queries against this metadata. All Stream
specific metadata is stored under a scoped/Stream root node. Queries
against this metadata include, but not limited to, querying segment sets
that form the Stream at different points in time, segment specific
information, segment predecessors and successors. Refer to [Stream metadata](https://github.com/pravega/pravega/blob/master/controller/src/main/java/io/pravega/controller/store/Stream/StreamMetadataStore.java) interface for details about APIs exposed by Stream metadata
store.

### Stream Metadata

Clients need information about what segments constitute a Stream to
start their processing and they obtain it from the _epoch_ information the
Controller stores in the Stream store. A **Reader** client typically starts
from the **head** of the Stream, but it might also choose to access the
Stream starting from any arbitrarily interesting position. **Writers** on
the other hand always append to the **tail** of the Stream.

Clients need ability to query and find segments at any of the following three
cases efficiently:

- To get initial set of segments.
- Segments at specific time.
- Current set of segments.

The Stream store provides API calls, to enable the above client queries.

As mentioned earlier, a Stream can transition from one set of Segments
(_epoch_) to another set of Segments that constitute the Stream. A Stream
moves from one _epoch_ to another if there is at least one Stream Segment that is
sealed and that is replaced by one or more set of Stream Segments that cover
precisely the key space of the sealed Stream Segments. As clients work on
Streams, they may encounter the end of sealed Stream Segments and consequently
need to find new Stream Segments to be able to move forward. To enable the
clients to query for the next Stream Segments, the Stream store exposes via the
Controller service, efficient queries for finding immediate successors
and predecessors for any arbitrary Segment.  

To enable serving queries like those mentioned above, we need to efficiently store a time series of these Stream Segment transitions and index them against time.
We store this information about the current and historical state of a Stream Segments in a set of tables which are designed to optimize on aforementioned queries.
Apart from Segment specific metadata record, the current state of Stream comprises of other metadata types that are described henceforth.

#### Tables  

To efficiently store and query the Stream Segment information, we have split
Stream Segment data into the following three append only tables.

  - **Segment Table**:  
_Segment-info: ⟨segmentid, time, keySpace-start, keySpace-end⟩_  
The Controller stores the Segment Table in an append-only table with
**i**-**th** row corresponding to metadata for Segment _Id_ (**i**). It is important
to note that each row in the Segment Table is of fixed size. As new
Stream Segments are added, they are assigned new Stream Segment *Ids* in a strictly
increasing order. So this table is very efficient in creating new
Stream Segments and querying Stream Segment information response with *O(1)* processing
for both these operations.

 - **History Table**:  
_Epoch: ⟨time, list-of-segments-in-epoch⟩_
The History Table stores a series of active Stream Segments as they transition
from one _epoch_ to another. Each row in the History Table stores an _epoch_
which captures a logically consistent (as defined earlier) set of
Stream Segments that form the Stream and are valid through the lifespan of the
_epoch_. This table is designed to optimize queries to find set of
Segments that form the Stream at any arbitrary time. There are three
most commonly used scenarios where we want to efficiently know the set
of Segments that form the Stream (_initial set of segments, current set
of segments_ and _segments at any arbitrary time_).  First two queries are
very efficiently answered in _O(1)_ time because they correspond to first
and last rows in the table.
Since rows in the table are sorted by increasing order of time and
capture time series of Streams Segment set changes, so we could easily
perform Binary Search to find row which corresponds to Stream Segment sets at
any arbitrary time.

 - **Index Table**  
_Index: ⟨time, offset-in-history-table⟩_  
Since history rows are of variable length, we index history rows for
timestamps in the Index Table. This enables us to navigate the History
Table and perform Binary Search to efficiently answer queries to get
Stream Segment set at any arbitrary time. We also perform Binary Searches on
History Table to determine successors of any given Stream Segment.

#### Stream Configuration
 Znode under which Stream configuration is serialized and persisted. A
 Stream configuration contains Stream policies that need to be
 enforced.
 Scaling policy and Retention policy are supplied by the application at
 the time of Stream creation and enforced by Controller by monitoring
 the rate and size of data in the Stream.

   - Scaling policy describes if and when to automatically scale based on
 incoming traffic conditions into the Stream. The policy supports two
 flavours - _traffic as rate of events per second_ and _traffic as rate of
 bytes per second_. The application specifies their desired traffic
 rates into each segment by means of scaling policy and the supplied
 value is chosen to compute thresholds that determine when to scale a
 given Stream.

   - Retention Policy describes the amount of data that needs to be
 retained into Pravega cluster for this Stream. We support a _time based_
 and a _size based_ retention policy where applications can choose
 whether they want to retain data in the Stream by size or by time by
 choosing the appropriate policy and supplying their desired values.

#### Stream State
 Znode which captures the state of the Stream. It is an enumerator with
 values from *creating, active, updating, scaling, truncating, sealing,*
 and *sealed*. Once _active_, a Stream transitions between performing a
 specific operation and remanis _active_ until it is sealed. A transition map is
 defined in the
 [State](https://github.com/pravega/pravega/blob/master/Controller/src/main/java/io/pravega/controller/store/Stream/tables/State.java)
 class which allows and prohibits various state transitions.
 Stream State describes the _current state_ of the Stream. It transitions
 from _active_ to respective action based on the action being performed
 on the Stream. For example, during scaling the state of the Stream
 transitions from *active* to *scaling* and once scaling completes, it
 transitions back to *active*. Stream State is used as a barrier to
 ensure only one type of operation is being performed on a given Stream
 at any point in time. Only certain state transitions are allowed and
 are described in the state transition object. Only legitimate state
 transitions are allowed and any attempt for disallowed transition
 results in appropriate exception.

#### Truncation Record
 This corresponds to the `StreamCut` which was last used to truncate the
 given Stream. All Stream Segment queries superimpose the truncation
 record and return Stream Segments that are strictly greater than or equal to
 the `StreamCut` in truncation record.

#### Sealed Segments Record
 Since the Segment Table is append only, any additional information
 that we need to persist when a Stream Segment is sealed and is stored in sealed
 Stream Segments record. At present, it contains a map of Stream Segment number
 to its sealed size.

The following are the Transaction Related metadata records:

   - **Active Transactions**: Each new Transaction is created under the znode. This stores metadata
 corresponding to each Transaction as *Active Transaction Record*. Once a
 Transaction is completed, a new node is created under the global
 _Completed Transaction_ znode and removed from under the Stream specific
 _Active Transaction_ node.

   - **Completed Transactions**: All completed transactions for all Streams are moved under this single
 znode upon completion (via either commit or abort paths). We can
 subsequently garbage collect these values periodically following any
 collection scheme we deem fit. We have not implemented any scheme at
 this point though.

### Stream Store Caching

#### In-memory Cache
Since there could be multiple concurrent requests for a given Stream
being processed by same Controller instance, it is suboptimal to read
the value by querying Zookeeper every time. So we have introduced an
**in-memory cache** that each Stream store maintains. It caches retrieved
metadata per Stream so that there is maximum one copy of the data per
Stream in the cache. There are two in-memory caches:

- _A cache of
multiple Stream objects in the store_
- _Cache properties of a Stream in
the Stream object_.

#### Operation Context
At the start of any new operation a new operation context is created. The creation of a
new operation context invalidates the cached entities for a Stream and
each entity is lazily retrieved from the store whenever requested. If a
value is updated during the course of the operation, it is again
invalidated in the cache so that other concurrent read/update operations
on the Stream get the new value for their subsequent steps.  

## Stream Buckets

To enable some scenarios, we may need our background workers to
periodically work on each of the Streams in our cluster to perform
some specific action on them. The concept of Stream Bucket is to
distribute this periodic background work across all available
Controller instances. Controller instances map all available streams in the system into buckets and these buckets are distributed amongst themselves. Hence, all the long running background work can be uniformly distributed across multiple Controller instances.

**Note**: Number of buckets for a cluster is a fixed (configurable) value for
the lifetime of a cluster.

Each bucket corresponds to a unique znode in
Zookeeper. A qualified scoped Stream name is used to compute a
hash value to assign the Stream to a bucket. All Controller instances, upon
startup, attempt to take ownership of buckets. Upon _failover_, ownerships
are transferred, as surviving nodes compete to acquire ownership of
orphaned buckets. The Controller instance which owns a bucket is
responsible for all long running scheduled background work corresponding
to all nodes under the bucket. Presently this entails running periodic
workflows to capture `StreamCuts` (called Retention-Set) for each Stream at desired frequencies.

### Retention Set

 One retention set per Stream is stored under the corresponding
 bucket/Stream znode. As we compute  `StreamCuts` periodically, we keep
 preserving them under this znode. As some automatic truncation is
 performed, the `StreamCuts` that are no longer valid are purged from
 this set.

## Controller Cluster Listener

Each node in Pravega Cluster registers itself under a cluster znode as
an ephemeral node. This includes both Controller and Segment Store
nodes. Each Controller instance registers a watch on the cluster znode
to listen for cluster change notifications. These notify about the added and removed nodes.

One Controller instance assumes leadership amongst all Controller
instances. This leader Controller instance is responsible for handling
Segment Store node change notifications. Based on the changes in topology,
Controller instance periodically rebalances segment containers to
Segment Store node mapping.

All Controller instances listen for Controller node change
notifications. Each Controller instance has multiple sub components that
implement the _failover sweeper_ interface. Presently there are three
components that implement _failover_ sweeper interface namely:

- `TaskSweeper`
- `EventProcessors`
- `TransactionSweeper`

Whenever a Controller instance is identified to have been removed from the cluster,
the cluster listener invokes all registered _failover sweepers_ to
optimistically try to sweep all the orphaned work previously owned by
the failed Controller host.

## Host Store

Host store interface is used to store _Segment Container_ to _Segment Store_
node mapping. It exposes APIs like `getHostForSegment` where it computes
consistent hash of Segment _Id_ to compute the owner Segment Container.
Then based on the container-host mapping, it returns the appropriate URI
to the caller.

## Background Workers

Controller process has two different mechanisms or frameworks for
processing background work. These background works typically entail
multiple steps and updates to metadata under a specific metadata root
entity and potential interactions with one or more Segment Stores.

We initially started with a simple task framework that allows ability to
run tasks that take exclusive rights over a given resource (typically a
Stream) and allow for tasks to _failover_ from one Controller instance to
another. However, this model was limiting in its scope and locking
semantics and had no inherent notion of ordering of tasks as multiple
tasks could race to acquire working rights (lock) on a resource
concurrently and any one of them could succeed. To overcome this limitation we came up with a new infrastructure called [**Event Processor**](#event-processor-framework)(It is built using Pravega Streams. Event Processor provides a clear mechanism to ensure **_mutually exclusive_** and **_ordered processing_).**

### Task Framework

Task framework is designed to run exclusive background processing per
resource such that in case of Controller instance failure, the work can
easily _failover_ to another Controller instance and brought to
completion. The framework, on its own, does not guarantee idempotent
processing and the author of a task has to handle it if required. The
model of tasks is defined to work on a given resource exclusively, which
means no other task can run concurrently on the same resource. This is
implemented by way of a persisted distributed lock implemented on
Zookeeper. The _failover_ of a task is achieved by following a scheme of
indexing the work a given process is performing. So if a process fails,
another process will sweep all outstanding work and attempt to transfer
ownership to itself.

Note that, Upon failure of a Controller process, multiple surviving Controller processes can concurrently attempt sweeping of orphaned tasks. Each of them will index the task in their
host-index but exactly one of them will be able to successfully acquire
the lock on the resource and hence permission to process the task. The
parameters for executing a task are serialized and stored under the
resource.

Currently we use Task framework only to create Stream tasks. All the
other background processing is done using Event Processor framework.

### Event Processor Framework

Event processors framework is a background worker sub system which reads
events from an internal Stream and processes it, hence the name event
processor. All event processors in our system provide **at least once
processing** guarantee. And in its basic flavor, the framework also
provides strong ordering guarantees. But we also have different subtypes
of event processors that allow concurrent processing.

We create different event processors for different kinds of work.
Presently we have _three_ different event processors in our system for
_committing transaction, aborting transactions_ and _processing Stream
specific requests like scale, update, seal, etc_. Each Controller instance
has one event processor of each type. The event processor framework
allows for multiple readers to be created per event processor. All
readers for a specific event processor across Controller instances share
the same reader group, which guarantees mutually exclusive distribution
of work across Controller instances. Each reader gets a dedicated thread
where it reads the event, calls for its processing and upon completion
of processing, updates its **Checkpoint**. Events are posted in the event
processor specific Stream and are routed to specific segments based on
using scoped Stream name as the routing key.

We have two flavors of event processors, one that performs serial
processing, which essentially means it reads an event and initiates its
processing and waits on it to complete before moving on to next event.
This provides strong ordering guarantees in processing. And it
checkpoints after processing each event.  Commit transaction is
implemented using this base flavor of event processor. The degree of
parallelism for processing these events is upper bounded by number of
segments in the internal Stream and lower bounded by number of readers.
Multiple events from across different Streams could land up in the same
segment and since we perform serial processing, serial processing has
the drawback that processing stalls or flooding of events from one
Stream could adversely impact latencies for unrelated Streams.

To overcome these drawbacks we designed **Concurrent Event Processor** as an
overlay on Serial Event processor. Concurrent event processor, as name
implies, allows us to process multiple events concurrently. Here the
reader thread reads an event, schedules it’s asynchronous processing and
returns to read the next event. There is a ceiling on number of events
that are concurrently processed at any point in time and as processing
of some event completes, newer events are allowed to be fetched.  The
checkpoint scheme here becomes slightly more involved because we want to
guarantee at least once processing.

However, with concurrent processing the ordering guarantees get broken.
However, it is important to note that we only need ordering guarantees
for processing events from a Stream and not across Streams. In order to
satisfy ordering guarantee, we overlay concurrent event processor with
**Serialized Request Handler**, which queues up events from the same Stream
in an in-memory queue and processes them in order.

  - **Commit Transaction** processing is implemented on a dedicated serial event
processor because we want strong commit ordering while ensuring that
commit does not interfere with processing of other kinds of requests on
the Stream.

  - **Abort Transaction** processing is implemented on a dedicated Concurrent
Event Processor which performs abort processing on transactions from
across Streams concurrently.

All other requests for Streams is implemented on a serialized request
handler which ensures exactly one request per Stream is being processed
at any given time and there is ordering guarantee within request
processing. However, it allows for concurrent requests from across
Streams to go on concurrently. Workflows like _scale, truncation, seal,
update_ and _delete Stream_ are implemented for processing on the request
event processor.

# Roles and Responsibilities

## Stream Operations

Controller is the store of truth for all Stream related metadata.
Pravega clients (`EventStreamReaders` and `EventStreamWriters`), in
conjunction with the Controller, ensure that Stream invariants are
satisfied and honored as they work on Streams. The Controller maintains
the metadata of Streams, including the entire history of segments.
Client accessing a Stream need to contact the Controller to obtain
information about segments.

Clients query Controller in order to know how to navigate Streams. For
this purpose Controller exposes appropriate APIs to get active segments,
successors, predecessors and segment information and _URIs_. These queries
are served using metadata stored and accessed via Stream store
interface.

Controller also provides workflows to modify state and behavior of the
Stream. These workflows include _create, scale, truncation, update, seal,_
and _delete_. These workflows are invoked both via direct APIs and in some
cases as applicable via background policy manager (auto scale and retention).

<p>
<img src="img/Request-Processing_Flow.png" width="880" height="670" alt="request processing">

Request Processing Flow
</p>

### Create Stream

Create Stream is implemented as a task on **Task Framework**. Create Stream
workflow first creates initial Stream metadata with Stream State set to
*Creating*. Following this, it identifies segment containers that should
own and create segments for this Stream and calls create segment
concurrently. Once all create segments complete, the create Stream task
completes thus moving the Stream to *Active* state. All failures are
retried few times with exponential backoffs. However, if it is unable to
complete any step, the Stream is left dangling in *Creating* state.

### Update Stream

Update Stream is implemented as a task on serialized sequest
handler over concurrent event processor framework. Update Stream is invoked
by an explicit API call into Controller. It first posts an _Update
Request_ event into request Stream. Following that it tries to create a
temporary update property. If it fails to create the temporary update
property, the request is failed and the caller is notified of the
failure to update a Stream due to conflict with another ongoing update.

The event is picked by **Request Event processor**. When the processing
starts, the update Stream task expects to find the temporary update
Stream property to be present. If it does not find the property, the
update processing is delayed by pushing event the back in the in-memory
queue until it deems the event expired. If it finds the property to be
updated during this period, before the expiry, the event is processed
and update Stream operation is performed. Now that the processing
starts, it first sets the state to *Updating*. Following this the Stream
configuration is updated in the metadata store followed by notifying
Segment Stores for all active segments of the Stream about change in
policy. Now the state is reset to *Active*.

### Scale Stream

Scale can be invoked either by explicit API call (referred to as manual
scale) or performed automatically based on scale policy (referred to as
auto-scale). We first write the event followed by updating the segment
table by creating new entries for desired segments to be created. This
step is idempotent and ensures that if an existing ongoing scale
operation is in progress, then this attempt to start a new scale fails.
The start of processing is similar to mechanism followed in update
Stream. If metadata is updated, the event processes and proceeds with
executing the task. If the metadata is not updated within the desired
time frame, the event is discarded.

Once scale processing starts, it first sets the Stream State *Scaling*.
This is followed by creating new segments in Segment Stores. After
successfully creating new segments, it updates the history table with a
partial record corresponding to new epoch which contains list of
segments as they would appear post scale. Each new epoch creation also
creates a new root epoch node under which metadata for all transactions
from that epoch reside. So as the scale is performed, there would be a
node corresponding to old epoch and now there will also be a root node
for new epoch. Any transaction creation from this point on will be done
against new epoch. Now the workflow attempts to complete scale by
opportunistically attempting to delete the old epoch. Old epoch can be
deleted if and only if there are no transactions under its tree. Once we
are sure there are no transactions on old epoch, we can proceed with
sealing old segments and completing the scale. After old segments are
sealed successfully, the partial record in history table is now
completed whereby completing the scale workflow. The state is now reset
to *Active*.

### Truncate Stream

Truncate follows similar mechanism to update and has a temporary
Stream property for truncation that is used to supply input for truncate Stream. Once truncate workflow identifies that it can proceed, it first
sets the state to *Truncating*. Truncate workflow then looks at the
requested StreamCut, and checks if it is greater than or equal to the
existing truncation point, only then is it a valid input for truncation
and the workflow commences. The truncation workflow takes the requested
StreamCut and computes all segments that are to be deleted as part of
this truncation request. It then calls into respective Segment Stores to
delete identified segments. Post deletion, we call truncate on segments
that are described in the Stream cut at the offsets as described in the
Stream cut. Following this the truncation record is updated with the new
truncation point and deleted segments.  The state is reset to *Active*.

### Seal Stream

Seal Stream can be requested via an explicit API call into Controller.
It first posts a seal-Stream event into request Stream followed by
attempts to set the state of Stream to *Sealing*. If the event is picked
and does not find the Stream to be in desired state, it postpones the
seal Stream processing by reposting it at the back of in-memory queue.
Once the Stream is set to sealing state, all active segments for the
Stream are sealed by calling into Segment Store. After this the Stream
is marked as *Sealed* in the Stream metadata.

### Delete Stream

Delete Stream can be requested via an explicit API call into Controller.
The request first verifies if the Stream is in *Sealed* state. Only sealed
Streams can be deleted and an event to this effect is posted in request
Stream. When the event is picked for processing, it verifies the Stream
state again and then proceeds to delete all segments that belong to this
Stream from its inception by calling into Segment Store. Once all
segments are deleted successfully, the Stream metadata corresponding to
this Stream is cleaned up.

## Stream Policy Manager

As described earlier, there are two types of user defined policies that Controller is responsible for enforcing, namely _Automatic Scaling_ and _Automatic Retention_.
Controller is not just the store for Stream policy but it actively enforces those user-defined policies for their Streams.

### Scaling Infrastructure

Scaling infrastructure is built in conjunction with Segment Stores. As
Controller creates new segments in Segment Stores, it passes user
defined scaling policies to Segment Stores. The Segment Store then
monitors traffic for the said segment and reports to Controller if some
thresholds, as determined from policy, are breached. Controller receives
these notifications via events posted in dedicated internal Streams.
There are two types of traffic reports that can be received for
segments. First type identifies if a segment should be **scaled up** (split)
and second type identifies if a segment should be **scaled down**. For segments eligible for scale up, Controller immediately posts request for
segment scale up in the request Stream for request event processor to
process. However, for scale down, Controller needs to wait for at least
two neighboring segments to become eligible for scale down. For this
purpose it simply marks the segment as **cold** in the metadata store. And
if and when there are neighboring segments that are marked as cold,
Controller consolidates them and posts a scale down request for them.
The scale requests processing is then performed asynchronously on the
request event processor.   

### Retention Infrastructure

The retention policy defines how much data should be retained for a
given Stream. This can be defined as _time based_ or _size based_. To apply
this policy, Controller periodically collects StreamCuts for the Stream
and opportunistically performs truncation on previously collected Stream
cuts if policy dictates it. Since this is a periodic background work
that needs to be performed for all Streams that have a retention policy
defined, there is an imperative need to fairly distribute this workload
across all available Controller instances. To achieve this we rely on
bucketing Streams into predefined sets and distributing these sets
across Controller instances. This is done by using Zookeeper to store
this distribution. Each Controller instance, during bootstrap, attempts
to acquire ownership of buckets. All Streams under a bucket are
monitored for retention opportunities by the owning Controller. At each
period, Controller collects a new Stream cut and adds it to a
retention set for the said Stream. Post this it looks for candidate
StreamCuts stored in retention set which are eligible for truncation
based on the defined retention policy. For example, in time based
retention, the latest StreamCut older than specified retention period
is chosen as the truncation point.

## Transaction Manager

Another important role played by Controller is that of Transaction
manager. It is responsible for creation and committing and abortion of
transactions. Since Controller is the central brain and agency in our
cluster, and is the holder of truth about Stream, the writers request
Controller to perform all control plane actions with respect to
Transactions. Controller plays active roles in providing guarantees for
Transactions from the time since they are created till the time they are
committed or aborted. Controller tracks each Transaction for their
specified timeouts and if the timeout exceeds, it automatically aborts
the transaction.

Controller is responsible for ensuring that the Transaction and a
potential concurrent scale operation play well with each other and
ensure all promises made with respect to either are honored and
enforced.
<p>
<img src="img/Transaction_Management.png" width="880" height="670" alt="Transaction Management">

Transaction Management
</p>
Client calls into Controller process to _create, ping commit_ or _abort
transactions_. Each of these requests is received on Controller and handled by the _Transaction Utility_ module which
implements the business logic for processing each request.

### Create Transaction

Writers interact with Controller to create new Transactions. Controller Service passes the create transaction request to Transaction Utility module.

The create transaction function in the module performs the following steps:

1. Generates a unique UUID for the Transaction.
2. It fetches current active set of segments for the Stream from metadata store and its corresponding epoch identifier from the history.
3. It creates a new transaction record in the Zookeeper using the metadata store interface.
4. It then requests Segment Store to create special Transaction segments that are inherently linked to the parent active segments.

While creating Transactions, Controller ensures that parent segments are not sealed as we attempt to create corresponding transaction segments.
And during the lifespan of a transaction, should a scale commence, it should wait for Transactions on older epoch to finish before the scale proceeds
to seal segments from old epoch.

### Commit Transaction

Upon receiving request to commit a transaction, Controller Service passes the request to Transaction Utility module.
This module first tries to mark the transaction for commit in the transaction specific metadata record via metadata store.
Following this, it posts a commit event in the internal Commit Stream.
Commit transaction workflow is implemented on commit event processor and
thereby processed asynchronously. The commit transaction workflow checks for eligibility of transaction to be committed, and if true,
it performs the commit workflow with indefinite retries until it
succeeds. If the Transaction is not eligible for commit, which typically
happens if Transaction is created on a new epoch while the old epoch is
still active, then such events are reposted into the internal Stream to
be picked later.

Once a Transaction is committed successfully, the record for the
transaction is removed from under its epoch root. Then if there is an
ongoing scale, then it calls to attempt to complete the ongoing scale.
Trying to complete scale hinges on ability to delete old epoch which can
be deleted if and only if there are no outstanding active transactions against the said epoch (refer to scale workflow for more details).   

### Abort Transaction

Abort, like commit, can be requested explicitly by the application.
However, abort can also be initiated automatically if the Transaction’s
timeout elapses. Controller tracks the timeout for each and every
transaction in the system and whenever timeout elapses, or upon explicit
user request, Transaction utility module marks the transaction for abort in its
respective metadata. Post this, the event is picked for processing by
abort event processor and Transactions abort is immediately attempted.
There is no ordering requirement for abort Transaction and hence it is
performed concurrently and across Streams.

Like commit, once the Transaction is aborted, its node is deleted from
its epoch root and if there is an ongoing scale, it complete scale flow
is attempted.

### Ping Transaction

Since Controller has no visibility into data path with respect to data
being written to segments in a transaction, Controller is unaware if a
transaction is being actively worked upon or not and if the timeout
elapses it may attempt to abort the transaction. To enable applications
to control the destiny of a transaction, Controller exposes an API to
allow applications to renew transaction timeout period. This mechanism
is called ping and whenever application pings a transaction, Controller
resets its timer for respective transaction.

### Transaction Timeout Management

Controllers track each Transaction for their timeouts. This is
implemented as timer wheel service. Each Transaction, upon creation gets
registered into the timer service on the Controller where it is created.
Subsequent pings for the Transaction could be received on different
Controller instances and timer management is transferred to latest
Controller instance based on ownership mechanism implemented via
Zookeeper. Upon timeout expiry, an automatic abort is attempted and if
it is able to successfully set transaction status to abort, the abort
workflow is initiated.

Each Transaction that a Controller is monitoring for timeouts is added
to this processes index. If such a Controller instance fails or crashes,
other Controller instances will receive node failed notification and
attempt to sweep all outstanding Transactions from the failed instance
and monitor their timeouts from that point onward.

## Segment Container to Host Mapping

Controller is also responsible for assignment of segment containers to
Segment Store nodes. The responsibility of maintaining this mapping
befalls a single Controller instance that is chosen via a leader
election using Zookeeper. This leader Controller monitors lifecycle of
Segment Store nodes as they are added to/removed from the cluster and
performs redistribution of segment containers across available segment
store nodes. This distribution mapping is stored in a dedicated znode.
Each Segment Store periodically polls this znode to look for changes and
if changes are found, it shuts down and relinquishes containers it no
longer owns and attempts to acquire ownership of containers that are
assigned to it.

The details about implementation, especially with respect to how the metadata is stored and managed is already discussed [here](#controller-cluster-listener).

# Resources

- [Pravega](http://pravega.io/)
- [Code](https://github.com/pravega/pravega/tree/master/Controller)
