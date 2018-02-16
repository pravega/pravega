<span id="_Toc506543504" class="anchor"><span id="_Toc506545512" class="anchor"></span></span>Pravega Controller Service
========================================================================================================================
*  [Introduction](#introduction)
*  [Architecture](#architecture)
    - [Controller as Stream Manager    ](#controllerAsStreamManager) 
    - [Controller as Container Mapper    ](#controllerAsContainerMapper)
* [System Diagram    ](#systemDiagram)
* [Components    ](#components)
    - [Service Endpoints    ](#serviceEndpoints)
        - [Client-Controller Endpoint    ](#clientControllerEP)
        - [Administrative API Endpoint    ](#administrativeApiEP)
    - [Controller Service    ](#controllerService)
    - [Stream Metadata Store    ](#streamStore)
        - [Stream-Segment Metadata    ](#streamSegmentMetadata)
        - [Stream Store Caching    ](#streamStoreCaching)
    - [Host Store    ](#hostStore)
    - [Stream Buckets    ](#streamBuckets)
    - [Controller Cluster Listener    ](#controllerClusterListener)
    - [Background workers    ](#backgroundWorkers)
* [Roles and Responsibilities    ](#rolesAndResponsibilities)
    - [Stream Operations    ](#streamOperations)
        - [Stream State    ](#streamState)
        - [Create Stream    ](#createStream)
        - [Update Stream    ](#updateStream)
        - [Scale Stream    ](#scaleStream)
        - [Truncate Stream    ](#truncateStream)
        - [Seal Stream    ](#sealStream)
        - [Delete Stream    ](#deleteStream)
    - [Stream Policy Manager    ](#streamPolicyManager)
        - [Scaling infrastructure    ](#scalingInfrastructure)
        - [Retention infrastructure    ](#retentionInfrastructure)
    - [Transaction Manager    ](#transactionManager)
        - [Create Transaction    ](#createTxn)
        - [Commit Transaction    ](#commitTxn)
        - [Abort Transaction    ](#abortTxn)
        - [Ping Transaction    ](#pingTxn)
        - [Transaction Timeout Management ](#txnTimeout)
    - [Segment Container to Host Mapping    ](#containerToHost)
* [Resources](#resources)

Introduction <a name="introduction"></a>
------------

The controller service is a core component of Pravega that implements
the control plane. It acts as the central coordinator and manager for
various operations performed in the cluster, mainly divided into two
categories: 1) stream management 2) cluster management.

The controller service, referred to simply as controller henceforth, is
responsible for providing the abstraction of a *stream*, which is the
main abstraction that Pravega exposes to applications. A stream
comprises one or more *segments*. Each segment is an append-only data
structure that stores a sequence of bytes. A segment on its own is
agnostic to presence of other segments and is not aware of its logical
relationship with its peer segments. The segment store, which owns and
manages these segments, does not have any notion of a stream. A stream
is a logical view conceptualized by *Controller* by composing a
dynamically changing set of segments that satisfy a predefined set of
logical invariants. The controller provides the stream abstraction and
orchestrates all lifecycle operations on a stream while ensuring that
the abstraction stays consistent.

The controller plays a central role in the lifecycle of a stream:
creation, modification, scaling, and deletion. It does these by
maintaining metadata per stream and performs requisite operations on
segments as and when necessary. For example, as part of stream’s
lifecycle, new segments can be created and existing segments sealed. The
controller decides when to perform these operation such that streams
continue to be available and consistent to the clients accessing them.

Architecture <a name="architecture"></a>
------------

The *Controller Service* is made up of one or more instances of
stateless worker nodes. Each new controller instance can be brought up
independently and to become part of pravega cluster it merely needs to
point to the same zookeeper. For high availability it is advised to have
more than one instance of controller service per cluster.

Each controller instance is capable of working independently and uses a
shared persistent store as the source of truth for all state owned and
managed by controller service. We currently use Apache ZooKeeper as the
store for persisting all metadata consistently.  Each instance comprises
various subsystems which are responsible for performing specific
operations on different categories of metadata. These subsystems include
different API endpoints, metadata store handles, policy managers and
background workers  

The controller exposes two endpoints which can be used to interact with
a controller service. The first port is for providing programmatic
access for pravega clients and is implemented as an RPC using gRPC. The
other endpoint is for administrative operations and is implemented as a
REST endpoint.

Each endpoint calls into a subsystem christened *Controller Service*
which has the actual implementation for various create, read, update and
delete (CRUD) operations on entities owned and managed by controller.

We broadly have two categories of entities that controller owns and
manages:
1. Streams
2. Segment Container mapping

### Controller as a Stream Manager <a name="controllerAsStreamManager"></a>

Streams are logical abstractions composed using individual segments such
that a predefined set of logically consistent stream invariants are
satisfied. The controller owns and manages the concept of stream and is
responsible for maintaining metadata and lifecycle for each stream.
Specifically, it is responsible for creating, updating, scaling,
truncating, sealing and deleting streams.

A stream can be viewed as a series of dynamically changing segment sets
where the stream transitions from one set of consistent segments to the
next. This transition is governed by user-defined policies that the
controller enforces. Consequently, as part of stream management, the
controller also performs roles of Policy Manager for policies like
retention and scale.

Implementing transactions requires the manipulation of segments. With
each transaction, Pravega creates a set of transaction segments, which
are later merged onto the stream segments upon commit or discarded upon
aborts. The controller performs the role of transaction manager and is
responsible for creating and committing transactions on a given stream.
Upon creating transactions, controller also tracks transaction timeouts
and aborts transactions whose timeouts have elapsed. Details of
transaction management can be found later in the document.

### Controller as a Container Mapper <a name="controllerAsContainerMapper"></a>

The controller is responsible for managing mapping of Segment Containers
to Segment Store nodes. One controller instance is chosen as a leader
using zookeeper based leader election recipe. The leader monitors
lifecycle of segment store nodes as they are added to/removed from the
cluster and performs redistribution of segment containers across
available segment store nodes.

System diagram <a name="systemDiagram"></a>
--------------

The following diagram shows the main components of a controller process.
We discuss the elements of the diagram in detail next.

 <img src="./img/ControllerServiceDiagram.png" width="624" height="334" />

Controller Process Diagram

Components <a name="components"></a>
----------

### Service Endpoints <a name="serviceEndpoints"></a>

There are two ports exposed by controller: client-controller APIs and
admin APIs. The client controller communication is exposed as RPC which
exposes APIs to perform all stream related control plane operations.
Apart from this controller also exposes an administrative API set
implemented as REST.

#### Client-Controller API<a name="clientControllerEP"></a>

Client Controller communication endpoint is implemented as a gRPC
interface. The complete list of APIs can be found
[here](https://github.com/pravega/pravega/blob/master/shared/controller-api/src/main/proto/Controller.proto).
This exposes APIs used by Pravega clients (readers, writers and stream
manager) and enables stream management. Requests enabled by this API
include creating, modifying, and deleting streams.

To be able to append to and read data from streams, writers and readers
query controller to get active segment sets, successor and predecessor
segments while working with a stream. For transactions, the client uses
specific API calls to request controller to create and commit
transactions.

#### Administration API <a name="administrativeApiEP"></a>

For administration, the controller implements and exposes a REST
interface. This includes API calls for stream management as well as
other administration API primarily dealing with creation and deletion of
scopes. We use swagger to describe our REST APIs. The swagger yaml file
can be found
[here](https://github.com/pravega/pravega/tree/master/shared/controller-api/src/main/swagger).

### Controller Service<a name="controllerService"></a>

This is the backend layer behind the controller endpoints (gRPC and
REST). All the business logic required to serve controller API calls are
implemented here. This has a handle the various store implementations
(stream store, host store and checkpoint store) which it uses to serve
all read queries. Update queries on metadata require background
processing and this layer posts the work for asynchronous processing,
tracks its progress and delivers results back to the caller.

### Stream Metadata Store<a name="streamStore"></a>

A stream is dynamically-changing sequence of segments, where regions of
the routing key space map to open segments. As the set of segments of a
stream changes, so does the mapping of the routing key space to segment.

A set of segments is consistent if 1) union of key space regions mapping
to segments in the set covers the entire key space; and 2) There is no
overlap between key space regions. For example, suppose a set *S* =
{*S*<sub>1</sub>, *S*<sub>2</sub>, *S*<sub>3</sub>}, such that:
-   Region \[0, 0.3) maps to segment *S*<sub>1</sub>
-   Region \[0.3, 0.6) maps to segment *S*<sub>2</sub>
-   Region \[0.6, 1.0) maps to segment *S*<sub>3</sub>

*S* is a consistent segment set.  

A stream goes through transformations as it scales over time. A stream
starts with an initial set of segments that is determined by the stream
configuration when created and it transitions to new sets of segments as
scale operations are performed on the stream. Each generation of
segments that constitute stream at any given point in time are
considered to belong to an epoch. So a stream starts with initial epoch
which is epoch 0 and upon each transition, it moves ahead in its epochs
to describe the change in generation of segments in the stream.

The controller maintains the stream store the information about all
epochs that constitute a given stream and how they transition. The store
is designed to optimally store and query information pertaining to
segments and their inter-relationships.

Apart from the epoch information, it keeps some additional metadata,
such as [state](#streamState) and its [policies](#streamPolicyManager) and ongoing transactions on the stream.

Various sub-components of controller access the stored metadata for each
stream via a well-defined
[interface](https://github.com/pravega/pravega/blob/master/controller/src/main/java/io/pravega/controller/store/stream/StreamMetadataStore.java).
We currently have two concrete implementations of the stream store
interface: in-memory and zookeeper backed stores. Both share a common
base implementation that relies on stream objects for providing
store-type specific implementations for all stream-specific metadata.
The base implementation of stream store creates and caches these stream
objects.

The stream objects implement a store/stream interface. The concrete
stream implementation is specific to the store type and is responsible
for implementation of store specific methods to provide consistency and
correctness. We have a common base implementation of all store types
that provide optimistic concurrency. This base class encapsulates the
logic for queries against stream store for all concrete stores that
support Compare and Swap (CAS). We currently use zookeeper as our
underlying store which also supports CAS. We store all stream metadata
in a hierarchical fashion under stream specific znodes (ZooKeeper
data nodes).

For the ZooKeeper based store, we structure our metadata into different
groups to support a variety of queries against this metadata. All stream
specific metadata is stored under a scoped/stream root node. Queries
against this metadata include, but not limited to, querying segment sets
that form the stream at different points in time, segment specific
information, segment predecessors and successors. Refer to stream
metadata interface for details about APIs exposed by stream metadata
store.

#### Stream Segment Metadata<a name="streamSegmentMetadata"></a>

Clients need information about what segments constitute a stream to
start their processing and they obtain it from the epoch information the
controller stores in the stream store. A reader client typically starts
from the head of the stream, but it might also choose to access the
stream starting from any arbitrarily interesting position. Writers on
the other hand always append to the tail of the stream.

Clients need ability to query and find segments at any of the three
cases efficiently. To enable such queries, the stream store provides API
calls like getInitialSegments, getSegmentsAtTime and getCurrentSegments.

As mentioned earlier, a stream can transition from one set of segments
(epoch) to another set of segments that constitute the stream. A stream
moves from one epoch to another if there is at least one segment that is
sealed and that is replaced by one or more set of segments that cover
precisely the key space of the sealed segments. As clients work on
streams, they may encounter the end of sealed segments and consequently
need to find new segments to be able to move forward. To enable the
clients to query for the next segments, the stream store exposes via the
controller service efficient queries for finding immediate successors
and predecessors for any arbitrary segment.  

To efficiently store and query the segment information, we have split
segment data into three append only tables, namely, segment-table,
history-table and index-table.

So the three tables are:
1.  Segment Table: segment-info: ⟨segmentid, time, keySpace-start,
    keySpace-end⟩
2.  History Table: epoch: ⟨time, list-of-segments-in-epoch⟩
3.  Index Table: index: ⟨time, offset-in-history-table⟩

##### Segment Table<a name="segmentTable"></a>

The Segment Table stores metadata for each segment. This includes:
1.  Segment id
2.  Key space
3.  Creation time.

The controller stores the segment table in an append-only table with
i-*th* row corresponding to metadata for segment id *i*. It is important
to note that each row in the segment table is of fixed size. As new
segments are added, they are assigned new segment ids in a strictly
increasing order. So this table is very efficient in creating new
segments and querying segment information response with O(1) processing
for both these operations.

##### History Table <a name="historyTable"></a>

The History Table stores a series of active segments as they transition
from one epoch to another. Each row in the history table stores an epoch
which captures a logically consistent (as defined earlier) set of
segments that form the stream and are valid through the lifespan of the
epoch. This table is designed to optimize queries to find set of
segments that form the stream at any arbitrary time. There are three
most commonly used scenarios where we want to efficiently know the set
of segments that form the stream - initial set of segments, current set
of segments and segments at any arbitrary time.  First two queries are
very efficiently answered in O(1) time because they correspond to first
and last rows in the table.

Since rows in the table are sorted by increasing order of time and
capture time series of streams segment set changes, so we could easily
perform binary search to find row which corresponds to segment sets at
any arbitrary time.

##### Index Table<a name="indexTable"></a>

Since history rows are of variable length, we index history rows for
timestamps in the index table. This enables us to navigate the history
table and perform binary search to efficiently answer queries to get
segment set at any arbitrary time. We also perform binary searches on
history table to determine successors of any given segment.

##### Other Stream Metadata<a name="otherSreamMetadata"></a>

Apart from the tables, we have the following additional metadata stored
per stream:

1. ###### Stream Configuration

 Znode under which stream configuration is serialized and persisted. A
 stream configuration contains stream policies that need to be
 enforced. Presently, we have two policies, namely *ScalingPolicy* and
 *RetentionPolicy.*

 Scaling policy and Retention policy are supplied by the application at
 the time of stream creation and enforced by controller by monitoring
 the rate and size of data in the stream.

 Scaling policy describes if and when to automatically scale based on
 incoming traffic conditions into the stream. The policy supports two
 flavours - traffic as rate of events per second and traffic as rate of
 bytes per second. The application specifies their desired traffic
 rates into each segment by means of scaling policy and the supplied
 value is chosen to compute thresholds that determine when to scale a
 given stream.

 Retention Policy describes the amount of data that needs to be
 retained into pravega cluster for this stream. We support a time based
 and a size based retention policy where applications can choose
 whether they want to retain data in the stream by size or by time by
 choosing the appropriate policy and supplying their desired values.

2. ###### Stream State<a name="streamState"></a>

 Znode which captures the state of the stream. It is an enum with
 values from Creating, Active, Updating, Scaling, Truncating, Sealing,
 and Sealed*.* Once Active, a stream transitions between performing a
 specific operation and active until it is sealed. A transition map is
 defined in the
 [State](https://github.com/pravega/pravega/blob/master/controller/src/main/java/io/pravega/controller/store/stream/tables/State.java)
 class which allows and prohibits various state transitions.

 Stream state describes the current state of the stream. It transitions
 from ACTIVE to respective action based on the action being performed
 on the stream. For example, during scaling the state of the stream
 transitions from ACTIVE to *SCALING* and once scaling completes, it
 transitions back to ACTIVE. Stream state is used as a barrier to
 ensure only one type of operation is being performed on a given stream
 at any point in time. Only certain state transitions are allowed and
 are described in the State Transition object. Only legitimate state
 transitions are allowed and any attempt for disallowed transition
 results in appropriate exception.

3. ###### Truncation Record

 This corresponds to the stream cut which was last used to truncate the
 given stream. All stream segment queries superimpose the truncation
 record and return segments that are strictly greater than or equal to
 the stream cut in truncation record.

4. ###### Sealed Segments Record

 Since the segment table is append only, any additional information
 that we need to persist when a segment is sealed is stored in sealed
 segments record. Presently, it simple contains a map of segment number
 to its sealed size.

5. ###### Active Transactions

 Each new transaction is created under this Znode. This stores metadata
 corresponding to each transaction as *ActiveTransactionRecord*. Once a
 transaction is completed, a new node is created under the global
 Completed Transaction Znode and removed from under the stream specific
 active transaction node.

##### Buckets

1. ###### Bucket

 To enable some scenarios, we may need our background workers to
 periodically work on each of the streams in our cluster to perform
 some specific action on them. We bring in a notion of a bucket to
 distribute this periodic background work across all available
 controller instances. For this we hash each stream into one of the
 predefined buckets and then distribute buckets across available
 controller instances.

 Number of buckets for a cluster is a fixed (configurable) value for
 the lifetime of a cluster.

2. ###### Scoped-Stream-Names

 Created directly under a specific bucket Znode. Each stream has a
 corresponding Znode under one of the buckets in the system.

3. ###### Retention Set

 One retention set per stream is stored under the corresponding
 bucket/stream Znode. As we compute stream-cuts periodically, we keep
 preserving them under this Znode. As some automatic truncation is
 performed, the stream-cuts that are no longer valid are purged from
 this set.

##### Completed Transactions

 All completed transactions for all streams are moved under this single
 znode upon completion (via either commit or abort paths). We can
 subsequently garbage collect these values periodically following any
 collection scheme we deem fit. We have not implemented any scheme at
 this point though.

#### Stream Store Caching<a name="streamStoreCaching"></a>

Since there could be multiple concurrent requests for a given stream
being processed by same controller instance, it is suboptimal to read
the value by querying zookeeper every time. So we have introduced an
in-memory cache that each stream store maintains. It caches retrieved
metadata per stream so that there is maximum one copy of the data per
stream in the cache. We have two in-memory caches – a) a cache of
multiple stream objects in the store, b) cache properties of a stream in
the stream object.

We have introduced a concept of operation context and at the start of
any new operation a new operation context is created. The creation of a
new operation context invalidates the cached entities for a stream and
each entity is lazily retrieved from the store whenever requested. If a
value is updated during the course of the operation, it is again
invalidated in the cache so that other concurrent read/update operations
on the stream get the new value for their subsequent steps.  

### Host Store<a name="hostStore"></a>

Host store interface is used to store Segment Container to Segment Store
node mapping. It exposes APIs like getHostForSegment where it computes
consistent hash of segment id to compute the owner Segment Container.
Then based on the container-host mapping, it returns the appropriate Uri
to the caller.

### Stream Buckets<a name="streamBuckets"></a>

Controller instances map all available streams in the system into
buckets and distribute buckets amongst themselves so that all long
running background work can be uniformly distributed across multiple
controller instances. Each bucket corresponds to a unique Znode in
zookeeper. Fully qualified scoped stream name is used to compute a
hashed to assign the stream to a bucket. All controller instances, upon
startup, attempt to take ownership of buckets. Upon failover, ownerships
are transferred as surviving nodes compete to acquire ownership of
orphaned buckets. The controller instance which owns a bucket is
responsible for all long running scheduled background work corresponding
to all nodes under the bucket. Presently this entails running periodic
workflows to capture stream-cuts for each stream at desired frequencies.

### Controller Cluster Listener<a name="controllerClusterListener"></a>

Each node in Pravega Cluster registers itself under a cluster Znode as
an ephemeral node. This includes both controller and segment store
nodes. Each controller instance registers a watch on the cluster Znode
to listen for cluster change notifications. These notifications are of
the kind node-added and node-removed.

One controller instance assumes leadership amongst all controller
instances. This leader controller instance is responsible for handling
segment store node change notifications. Based on change in topology,
controller instance periodically rebalances segment containers to
segment store node mapping.

All controller instances listen for controller node change
notifications. Each controller instance has multiple sub components that
implement the failover-sweeper interface. Presently there are three
components that implement failover sweeper interface namely,
*TaskSweeper, EventProcessors* and *TransactionSweeper*. Whenever a
controller instance is identified to have been removed from the cluster,
the cluster listener invokes all registered failover sweepers to
optimistically try to sweep all the orphaned work previously owned by
the failed controller host.

### Background workers<a name="backgroundWorkers"></a>

Controller process has two different mechanisms/frameworks for
processing background work. These background works typically entail
multiple steps and updates to metadata under a specific metadata root
entity and potential interactions with one or more segment stores.

We initially started with a simple task framework that allows ability to
run tasks that take exclusive rights over a given resource (typically a
stream) and allow for tasks to failover from one controller instance to
another. However, this model was limiting in its scope and locking
semantics and had no inherent notion of ordering of tasks as multiple
tasks could race to acquire working rights (lock) on a resource
concurrently and any one of them could succeed.

To overcome this limitation we came up with a new infrastructure called
Event Processor. Event processor is the classic eat-your-own-dog-food.
It is built using pravega streams. This provides us a neat mechanism to
ensure mutually exclusive and ordered processing.

#### Task Framework<a name="taskFramework"></a>

Task framework is designed to run exclusive background processing per
resource such that in case of controller instance failure, the work can
easily failover to another controller instance and brought to
completion. The framework, on its own, does not guarantee idempotent
processing and the author of a task has to handle it if required. The
model of tasks is defined to work on a given resource exclusively, which
means no other task can run concurrently on the same resource. This is
implemented by way of a persisted distributed lock implemented on
zookeeper. The failover of a task is achieved by following a scheme of
indexing the work a given process is performing. So if a process fails,
another process will sweep all outstanding work and attempt to transfer
ownership to itself. Note: upon failure of a controller process,
multiple surviving controller processes can concurrently attempt
sweeping of orphaned tasks. Each of them will index the task in their
host-index but exactly one of them will be able to successfully acquire
the lock on the resource and hence permission to process the task. The
parameters for executing a task are serialized and stored under the
resource.

Currently we use task framework only for create stream tasks. All the
other background processing is done using event processor framework.

#### Event Processor Framework<a name="eventProcFramework"></a>

Event processors framework is a background worker sub system which reads
events from an internal stream and processes it, hence the name event
processor. All event processors in our system provide *at least* *once*
*processing* guarantee. And in its basic flavor, the framework also
provides strong ordering guarantees. But we also have different subtypes
of event processors that allow concurrent processing.

We create different event processors for different kinds of work.
Presently we have three different event processors in our system for
committing transaction, aborting transactions and processing stream
specific requests like scale update seal etc. Each controller instance
has one event processor of each type. The event processor framework
allows for multiple readers to be created per event processor. All
readers for a specific event processor across controller instances share
the same reader group, which guarantees mutually exclusive distribution
of work across controller instances. Each reader gets a dedicated thread
where it reads the event, calls for its processing and upon completion
of processing, updates its checkpoint. Events are posted in the event
processor specific stream and are routed to specific segments based on
using scoped stream name as the routing key.

We have two flavors of event processors, one that performs serial
processing, which essentially means it reads an event and initiates its
processing and waits on it to complete before moving on to next event.
This provides strong ordering guarantees in processing. And it
checkpoints after processing each event.  Commit transaction is
implemented using this base flavor of event processor. The degree of
parallelism for processing these events is upper bounded by number of
segments in the internal stream and lower bounded by number of readers.
Multiple events from across different streams could land up in the same
segment and since we perform serial processing, serial processing has
the drawback that processing stalls or flooding of events from one
stream could adversely impact latencies for unrelated streams.

To overcome these drawbacks we designed Concurrent Event Processor as an
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
for processing events from a stream and not across streams. In order to
satisfy ordering guarantee, we overlay Concurrent Event Processor with
Serialized Request Handler, which queues up events from the same stream
in an in-memory queue and processes them in order.

Commit Transaction processing is implemented on a dedicated Serial Event
Processor because we want strong Commit ordering while ensuring that
commit does not interfere with processing of other kinds of requests on
the stream.

Abort Transaction processing is implemented on a dedicated Concurrent
Event Processor which performs abort processing on transactions from
across streams concurrently.

All other requests for streams is implemented on a Serialized Request
Handler which ensures exactly one request per stream is being processed
at any given time and there is ordering guarantee within request
processing. However, it allows for concurrent requests from across
streams to go on concurrently. Workflows like scale, truncation, seal,
update and delete stream are implemented for processing on the Request
Event processor.

Roles and Responsibilities<a name="rolesAndResponsibilities"></a>
--------------------------

### Stream Operations<a name="streamOperations"></a>

Controller is the store of truth for all stream related metadata.
Pravega clients (EventStreamReaders and EventStreamWriters), in
conjunction with the controller, ensure that stream invariants are
satisfied and honored as they work on streams. The controller maintains
the metadata of streams, including the entire history of segments.
Client accessing a stream need to contact the controller to obtain
information about segments.

Clients query controller in order to know how to navigate streams. For
this purpose controller exposes appropriate APIs to get active segments,
successors, predecessors and segment information and Uris. These queries
are served using metadata stored and accessed via stream store
interface.

Controller also provides workflows to modify state and behavior of the
stream. These workflows include create, scale, truncation, update, seal,
and delete. These workflows are invoked both via direct APIs and in some
cases as applicable via background  
policy manager (auto-scale and retention).

<img src="./img/RequestProcessing.png" width="624" height="338" />

Request Processing Flow

#### Create Stream<a name="createStream"></a>

Create stream is implemented as a task on Task Framework. Create stream
workflow first creates initial stream metadata with stream state set to
CREATING*.* Following this, it identifies segment containers that should
own and create segments for this stream and calls create-segment
concurrently. Once all create segments complete, the create stream task
completes thus moving the stream to ACTIVE state. All failures are
retried few times with exponential backoffs. However, if it is unable to
complete any step, the stream is left dangling in CREATING state. * *

#### Update Stream

Update stream is implemented as a task on Serialized Request
Handler/Concurrent Event Processor framework. Update stream is invoked
by an explicit API call into controller. It first posts an Update
Request event into request-stream. Following that it tries to create a
temporary update property. If it fails to create the temporary update
property, the request is failed and the caller is notified of the
failure to update a stream due to conflict with another ongoing update.

The event is picked by Request Event processor. When the processing
starts, the update stream task expects to find the temporary update
stream property to be present. If it does not find the property, the
update processing is delayed by pushing event the back in the in-memory
queue until it deems the event expired. If it finds the property to be
updated during this period, before the expiry, the event is processed
and update stream operation is performed. Now that the processing
starts, it first sets the state to UPDATING. Following this the stream
configuration is updated in the metadata store followed by notifying
segment stores for all active segments of the stream about change in
policy. Now the state is reset to ACTIVE.

#### Scale Stream<a name="scaleStream"></a>

Scale can be invoked either by explicit API call (referred to as manual
scale) or performed automatically based on scale policy (referred to as
auto-scale). We first write the event followed by updating the segment
table by creating new entries for desired segments to be created. This
step is idempotent and ensures that if an existing ongoing scale
operation is in progress, then this attempt to start a new scale fails.
The start of processing is similar to mechanism followed in update
stream. If metadata is updated, the event processes and proceeds with
executing the task. If the metadata is not updated within the desired
time frame, the event is discarded.

Once scale processing starts, it first sets the stream state SCALING.
This is followed by creating new segments in segment stores. After
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
to ACTIVE.

#### Truncate Stream<a name="truncateStream"></a>

Truncate follows similar mechanism to update and has a temporary
stream-property for truncation that is used to supply input for truncate
stream. Once truncate workflow identifies that it can proceed, it first
sets the state to TRUNCATING. Truncate workflow then looks at the
requested stream-cut, and checks if it is greater than or equal to the
existing truncation point, only then is it a valid input for truncation
and the workflow commences. The truncation workflow takes the requested
stream-cut and computes all segments that are to be deleted as part of
this truncation request. It then calls into respective segment stores to
delete identified segments. Post deletion, we call truncate on segments
that are described in the stream cut at the offsets as described in the
stream cut. Following this the truncation record is updated with the new
truncation point and deleted segments.  The state is reset to ACTIVE.

#### Seal Stream

Seal stream can be requested via an explicit API call into controller.
It first posts a seal-stream event into request stream followed by
attempts to set the state of stream to *SEALING*. If the event is picked
and does not find the stream to be in desired state, it postpones the
seal stream processing by reposting it at the back of in-memory queue.
Once the stream is set to sealing state, all active segments for the
stream are sealed by calling into segment store. After this the stream
is marked as SEALED in the stream metadata.

#### Delete Stream<a name="deleteStream"></a>

Delete stream can be requested via an explicit API call into controller.
The request first verifies if the stream is in SEALED state. Only sealed
streams can be deleted and an event to this effect is posted in request
stream. When the event is picked for processing, it verifies the stream
state again and then proceeds to delete all segments that belong to this
stream from its inception by calling into segment store. Once all
segments are deleted successfully, the stream metadata corresponding to
this stream is cleaned up.

### Stream Policy Manager<a name="streamPolicyManager"></a>

Controller is not just the store for stream policy but it actively
enforces those user-defined policies for their streams. Presently we
have two policies that users can define, namely scaling policy and
retention policy. Controller provides elaborate scaling infrastructure
that actively monitors traffic for streams and based on requested
policies decides on automatic scaling of segments.

Similarly, users can define retention policies and controller
periodically monitors data in streams and decides when and where to
truncate automatically based on user-defined policy.

#### Scaling infrastructure<a name="scalingInfra"></a>

Scaling infrastructure is built in conjunction with segment stores. As
controller creates new segments in segment stores, it passes user
defined scaling policies to segment stores. The Segment store then
monitors traffic for the said segment and reports to controller if some
thresholds, as determined from policy, are breached. Controller receives
these notifications via events posted in dedicated internal streams.
There are two types of traffic reports that can be received for
segments. First type identifies if a segment should be scaled up (split)
and second type identifies if a segment should be scaled down. For
segments eligible for scale up, controller immediately posts request for
segment scale up in the request stream for Request Event Processor to
process. However, for scale down, controller needs to wait for at least
two neighboring segments to become eligible for scale down. For this
purpose it simply marks the segment as cold in the metadata store. And
if and when there are neighboring segments that are marked as cold,
controller consolidates them and posts a scale down request for them.
The scale requests processing is then performed asynchronously on the
Request Event processor.   

#### Retention infrastructure

The retention policy defines how much data should be retained for a
given stream. This can be defined as time based or size based. To apply
this policy, controller periodically collects stream-cuts for the stream
and opportunistically performs truncation on previously collected stream
cuts if policy dictates it. Since this is a periodic background work
that needs to be performed for all streams that have a retention policy
defined, there is an imperative need to fairly distribute this workload
across all available controller instances. To achieve this we rely on
bucketing streams into predefined sets and distributing these sets
across controller instances. This is done by using zookeeper to store
this distribution. Each controller instance, during bootstrap, attempts
to acquire ownership of buckets. All streams under a bucket are
monitored for retention opportunities by the owning controller. At each
period, controller collects a new stream cut and adds it to a
retention-set for the said stream. Post this it looks for candidate
stream-cuts stored in retention-set which are eligible for truncation
based on the defined retention policy. For example, in time based
retention, the latest stream-cut older than specified retention period
is chosen as the truncation point.

### Transaction Manager<a name="transactionManager"></a>

Another important role played by controller is that of transaction
manager. It is responsible for creation and committing and abortion of
transactions. Since controller is the central brain and agency in our
cluster, and is the holder of truth about stream, the writers request
controller to perform all control plane actions with respect to
transactions. Controller plays active roles in providing guarantees for
transactions from the time since they are created till the time they are
committed or aborted. Controller tracks each transaction for their
specified timeouts and if the timeout exceeds, it automatically aborts
the transaction.

Controller is responsible for ensuring that the transaction and a
potential concurrent scale operation play well with each other and
ensure all promises made with respect to either are honored and
enforced.

<img src="./img/TransactionManagement.png" width="624" height="340" />

Transaction Management Diagram

Client calls into controller process to create, ping commit or abort
transactions. Each of these flows is described in detail below.

#### Create transaction<a name="createTxn"></a>

Writers interact with Controller to create new transactions. Controller
looks at active segments and requests segment store to create special
transaction segments that are inherently linked to the parent active
segments. While creating transactions, controller ensures that parent
segments are not sealed as we attempt to create a transaction. And
during the lifespan of a transaction, should a scale commence, it should
wait for transactions on older epoch to finish before the scale proceeds
to seal segments from old epoch.   

#### Commit Transaction<a name="commitTxn"></a>

Upon receiving request to commit a transaction, controller first marks
the transaction for commit in the transaction specific metadata.
Following this, it posts a commit event in the internal Commit Stream.
Commit transaction workflow is implemented on commit event processor and
it checks for eligibility of transaction to be committed, and if true,
it performs the commit workflow with indefinite retries until it
succeeds. If the transaction is not eligible for commit, which typically
happens if transaction is created on a new epoch while the old epoch is
still active, then such events are reposted into the internal stream to
be picked later.

Once a transaction is committed successfully, the node for the
transaction is removed from under its epoch root. Then if there is an
ongoing scale, then it calls to attempt to complete the ongoing scale.
Trying to complete scale hinges on ability to delete old epoch which can
be deleted if and only if the tree under it is empty, meaning there is
no outstanding transaction on old epoch (refer to scale workflow).   

#### Abort Transaction<a name="abortTxn"></a>

Abort, like commit, can be requested explicitly by the application.
However, abort can also be initiated automatically if the transaction’s
timeout elapses. Controller tracks the timeout for each and every
transaction in the system and whenever timeout elapses, or upon explicit
user request, controller marks the transaction for abort in its
respective metadata. Post this, the event is picked for processing by
abort event processor and transactions abort is immediately attempted.
There is no ordering requirement for abort transaction and hence it is
performed concurrently and across streams.

Like commit, once the transaction is aborted, its node is deleted from
its epoch root and if there is an ongoing scale, it complete scale flow
is attempted.

#### Ping Transaction<a name="pingTxn"></a>

Since controller has no visibility into data path with respect to data
being written to segments in a transaction, Controller is unaware if a
transaction is being actively worked upon or not and if the timeout
elapses it may attempt to abort the transaction. To enable applications
to control the destiny of a transaction, controller exposes an API to
allow applications to renew transaction timeout period. This mechanism
is called ping and whenever application pings a transaction, controller
resets its timer for respective transaction.

#### Transaction Timeout Management<a name="txnTimeout"></a>

Controllers track each transaction for their timeouts. This is
implemented as timer wheel service. Each transaction, upon creation gets
registered into the timer service on the controller where it is created.
Subsequent pings for the transaction could be received on different
controller instances and timer management is transferred to latest
controller instance based on ownership mechanism implemented via
zookeeper. Upon timeout expiry, an automatic abort is attempted and if
it is able to successfully set transaction status to abort, the abort
workflow is initiated.

Each transaction that a controller is monitoring for timeouts is added
to this processes index. If such a controller instance fails or crashes,
other controller instances will receive node failed notification and
attempt to sweep all outstanding transactions from the failed instance
and monitor their timeouts from that point onward.

### Segment Container to Host Mapping<a name="containerToHost"></a>

Controller is also responsible for assignment of segment containers to
segment store nodes. The responsibility of maintaining this mapping
befalls a single controller instance that is chosen via a leader
election using zookeeper. This leader controller monitors lifecycle of
segment store nodes as they are added to/removed from the cluster and
performs redistribution of segment containers across available segment
store nodes. This distribution mapping is stored in a dedicated ZNode.
Each segment store periodically polls this znode to look for changes and
if changes are found, it shuts down and relinquishes containers it no
longer owns and attempts to acquire ownership of containers that are
assigned to it.

Resources<a name="resources"></a>
---------
- [Pravega](http://pravega.io/)
- [Code](https://github.com/pravega/pravega/tree/master/controller)
- [Other Documents](http://pravega.io/docs/latest/)
