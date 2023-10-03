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
# Working with Pravega: Basic Reader and Writer

Let's examine how to build Pravega applications. The simplest kind of Pravega application uses a Reader to read from a Stream or a
Writer that writes to a Stream. A simple sample application of both can be
found in the [Pravega samples repository](https://github.com/pravega/pravega-samples/blob/v0.5.0/pravega-client-examples/README.md) (`HelloWorldReader` and  `HelloWorldWriter`) applications. These samples give a sense on how a Java application could use the Pravega's Java Client Library to access Pravega functionality.

Instructions for running the sample applications can be found in the [Pravega
Samples](https://github.com/pravega/pravega-samples/blob/v0.5.0/pravega-client-examples/README.md). Get familiar with the [Pravega Concepts](https://pravega.io/docs/latest/pravega-concepts/)
before executing the sample applications.

## HelloWorldWriter

The `HelloWorldWriter` application demonstrates the usage of
`EventStreamWriter` to write an Event to Pravega.

The key part of `HelloWorldWriter` is in the `run()` method. The purpose of the `run()` method is to create a Stream and output the given Event to that Stream.

```Java

public void run(String routingKey, String message) {
    StreamManager streamManager = StreamManager.create(controllerURI);

    final boolean scopeCreation = streamManager.createScope(scope);
    StreamConfiguration streamConfig = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.fixed(1))
            .build();
    final boolean streamCreation = streamManager.createStream(scope, streamName, streamConfig);

    try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
         EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build()))
    {
         System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                           message, routingKey, scope, streamName);
         final CompletableFuture<Void> writeFuture = writer.writeEvent(routingKey, message);
    }
}

```

## Creating a Stream and the StreamManager Interface

[Scopes and Streams](pravega-concepts.md#streams) are created and manipulated via the `StreamManager` interface
to the Pravega Controller. An URI to any of the Pravega Controller instance(s) in your cluster is required to create a `StreamManager` object. In the setup for the `HelloWorld` sample applications, the `controllerURI` is
configured as a command line parameter when the sample application is launched.



**Note:** For the "standalone" deployment of Pravega, the Controller is listening on
localhost, port 9090.

The `StreamManager` provides access to various control plane functions in Pravega
related to Scopes and Streams:

| **Method**      | **Parameters**                                                    | **Discussion**                                                                                                 |
|-----------------|-------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| (static) `create` | (`URI controller`)                                                  | Given a URI to one of the Pravega Controller instances in the Pravega Cluster, create a Stream Manager object. |
| `createScope`     | (String `scopeName`)                                                | Creates a Scope with the given name.                                                                           |
|                 |                                                                   | Returns _True_ if the Scope is created, returns _False_ if the Scope already exists.                                                                                                               |
|                 |                                                                   | This method can be called even if the Stream is already existing.                                                                                                              |
| `deleteScope`     | (String `scopeName`)                                                | Deletes a Scope with the given name.                                                                           |
|                 |                                                                   | Returns _True_ if the scope was deleted, returns _False_ otherwise.                                                                                                               |
|                 |                                                                   | If the Scope contains Streams, the `deleteScope` operation will fail with an exception.                                                                                                               |
|                 |                                                                   | If we delete a non-existent Scope, the method will succeed and return _False_.                                                                                                               |
| `createStream`    | (String `scopeName`, String `streamName`, `StreamConfiguration config`) | Create a Stream within a given Scope.                                                                          |
|                 |                                                                   | Both Scope name and Stream name are limited using the following characters: Letters (a-z A-Z), numbers (0-9) and delimiters: "." and "-" are allowed.                                                                                                               |
|                 |                                                                   | The Scope must exist, an exception is thrown if we create a Stream in a non-existent Scope.                                                                                                               |
|                 |                                                                   | A Stream Configuration is built using a builder pattern.                                                                                                               |
|                 |                                                                   | Returns _True_ if the Stream is created, returns _False_ if the Stream already exists.                                                                                                               |
|                 |                                                                   | This method can be called even if the Stream is already existing.                                                                                                               |
| `updateStream`    | (String `scopeName`, String `streamName`, `StreamConfiguration config`) | Swap out the Stream's configuration.                                                                           |
|                 |                                                                   |  The Stream must already exist, an exception is thrown if we update a non-existent Stream.                                                                                                              |
|                 |                                                                   | Returns _True_ if the Stream was changed.                                                                                                               |
| `sealStream`      | (String `scopeName`, String `streamName`)                             | Prevent any further writes to a Stream.                                                                        |
|                 |                                                                   | The Stream must already exist, an exception is thrown if you seal a non-existent Stream.                                                                                                                |
|                 |                                                                   | Returns _True_ if the Stream is successfully sealed.                                                                                                               |
| `deleteStream`    | (String `scopeName`, String `streamName`)                             | Remove the Stream from Pravega and recover any resources used by that Stream.                                  |
|                 |                                                                   | Returns _False_ if the Stream is non-existent.                                                                                                              |
|                 |                                                                   | Returns _True_ if the Stream was deleted.                                                                                                               |



The execution of API `createScope(scope)` establishes that the Scope exists. Then we can create the Stream using the API `createStream(scope, streamName, streamConfig)`. The `StreamManager` requires three parameters to create a Stream:

- Scope Name.
- Stream Name.
- Stream Configuration.

The most interesting task is to create the Stream Configuration (`streamConfig`). Like many objects in Pravega, a Stream takes a configuration object that allows
a developer to control various behaviors of the Stream. All configuration object instantiated via builder pattern. That allows a developer to control various aspects of a Stream's behavior in terms of _policies_; [**Retention Policy**](pravega-concepts.md#stream-retention-policies) and [**Scaling
Policy**](pravega-concepts.md#elastic-streams-auto-scaling) are the most important ones related to Streams. For the sake of simplicity, in our sample application we instantiate a Stream with a single segment (`ScalingPolicy.fixed(1)`) and using the default (infinite) retention policy.

Once the Stream Configuration (`streamConfig`) object is built, creating the Stream is straight
forward using `createStream()`. After the Stream is created, we are all set to start writing
Event(s) to the Stream.


## Writing an Event using EventWriter

Applications use an `EventStreamWriter` object to write Events to a Stream. The
`EventStreamWriter` is created using the `ClientFactory` object.  The
`ClientFactory` is used to create Readers, Writers and other types of Pravega
Client objects such as the State Synchronizer (see [Working with Pravega: State
Synchronizer](state-synchronizer.md)).

A `ClientFactory` is created in the context of a Scope, since all Readers, Writers and other Clients created by the `ClientFactory` are created in the context of that Scope. The `ClientFactory`
also needs a URI to one of the Pravega Controllers (`ClientFactory.withScope(scope, controllerURI)`) , just like `StreamManager`.

As the `ClientFactory` and the objects it creates consume resources from Pravega and implement `AutoCloseable`, it is a good practice to create these objects using a try-with-resources. By doing this, we make sure that, regardless of how the application ends, the Pravega resources will be properly closed in the right order.


Once the `ClientFactory` is instantiated, we can use it to create a Writer. There are
several things a developer needs to know before creating a Writer:

1.  _What is the name of the Stream to write to?_
    (The Scope has already
    been determined when the `ClientFactory` was created.)

2.  _What Type of Event objects will be written to the Stream?_

3.  _What serializer will be used to convert an Event object to bytes?_ (Recall
    that Pravega only knows about sequences of bytes, it is unaware about Java objects.)

4.  _Does the Writer need to be configured with any special behavior?_


```Java

EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
        new JavaSerializer<String>(),
        EventWriterConfig.builder().build()))

```

The `EventStreamWriter` writes to the
Stream specified in the configuration of the `HelloWorldWriter` sample application (by
default the stream is named "helloStream" in the "examples" Scope). The Writer
processes Java String objects as Events and uses the built in Java serializer
for Strings.

**Note:**  Pravega allows users to write their own serializer. For more information and example, please refer to [Pravega Serializer](https://github.com/pravega/pravega-serializer). 

The `EventWriterConfig` allows the developer to specify things like the number of
attempts to retry a request before giving up and associated exponential back-off
settings. Pravega takes care to retry requests in the presence of connection
failures or Pravega component outages, which may temporarily prevent a request from
succeeding, so application logic doesn't need to be complicated by dealing
with intermittent cluster failures. In the sample application, `EventWriterConfig` was considered as the default settings.

`EventStreamWriter` provides a `writeEvent()` operation that writes the given non-null Event object to
the Stream using a given Routing key to determine which Stream Segment it should
written to.  Many operations in Pravega, such as `writeEvent()`, are asynchronous
and return some sort of `Future` object. If the application needed to make sure
the Event was durably written to Pravega and available for Readers, it could
wait on the `Future` before proceeding. In the case of Pravega's `HelloWorld`
example, it does wait on the `Future`.

`EventStreamWriter` can also be used to begin a Transaction.  We cover
Transactions in more detail in [Working with Pravega:
Transactions](transactions.md).

## HelloWorldReader

The `HelloWorldReader` is a simple demonstration of using the `EventStreamReader`.
The application reads Events from the given Stream and prints a string
representation of those Events onto the console.

Just like the `HelloWorldWriter` example, the key part of the `HelloWorldReader` application
is in the `run()` method:

```Java

public void run() {
    StreamManager streamManager = StreamManager.create(controllerURI);

    final boolean scopeIsNew = streamManager.createScope(scope);
    StreamConfiguration streamConfig = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.fixed(1))
            .build();
   final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

   final String readerGroup = UUID.randomUUID().toString().replace("-", "");
   final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
            .stream(Stream.of(scope, streamName))
            .build();
    try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI))
    {
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
    }

    try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        EventStreamReader<String> reader = clientFactory.createReader("reader",
                readerGroup,
                new JavaSerializer<String>(),
                ReaderConfig.builder().build()))
    {
        System.out.format("Reading all the events from %s/%s%n", scope, streamName);
        EventRead<String> event = null;
        do {
            try {
                event = reader.readNextEvent(READER_TIMEOUT_MS);
                if (event.getEvent() != null) {
                    System.out.format("Read event '%s'%n", event.getEvent());
                }
            } catch (ReinitializationRequiredException e) {
                //There are certain circumstances where the reader needs to be reinitialized
                e.printStackTrace();
            }
        } while (event.getEvent() != null);
        System.out.format("No more events from %s/%s%n", scope, streamName);
    }
}
```

The API `streamManager.createScope()` and `streamManager.createStream()` set up the Scope and Stream just like in the `HelloWorldWriter`
application. The API `ReaderGroupConfig` set up the Reader Group as the prerequisite to creating
the `EventStreamReader` and using it to read Events from the Stream (`createReader()`,`reader.readNextEvent()`).

## Reader Group Basics

Any Reader in Pravega belongs to some `ReaderGroup`.  A `ReaderGroup` is a grouping
of one or more Readers that consume from a Stream in parallel. Before we create
a Reader, we need to either create a `ReaderGroup` (or be aware of the name of an
existing `ReaderGroup`). This application only uses the basics from Reader Group.

`ReaderGroup` objects are created from a `ReaderGroupManager` object. The `ReaderGroupManager` object, in turn, is created on a given Scope with a URI to one of the Pravega Controllers, very much
like a `ClientFactory` is created. Note that, the `createReaderGroup` is also in a try-with-resources statement to make sure that the `ReaderGroupManager` is properly cleaned up. The `ReaderGroupManager` allows a developer to create, delete and retrieve `ReaderGroup` objects using the name.

To create a `ReaderGroup`, the developer needs a name for the Reader Group and a
configuration with a set of one or more Streams to read from. The Reader Group's name (alphanumeric) might be meaningful to the application, like
"WebClickStreamReaders".  In cases where we require multiple Readers reading in parallel and each Reader in a separate
process, it is helpful to have a human readable name for the Reader Group. In this example, we have one Reader, reading in isolation, so a UUID is a safe way to
name the Reader Group. The Reader Group is created via the
`ReaderGroupManager` and since the `ReaderGroupManager` is created within the
context of a Scope, we can safely conclude that Reader Group names are namespaced
by that Scope.  

The developer specifies the Stream which should be the part of the Reader Group and its lower and
upper bounds. In the sample application, we start at the beginning of the Stream as follows:

```Java
final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
        .stream(Stream.of(scope, streamName))
        .build();
```
Other configuration items, such as Checkpointing are options
that will be available through the `ReaderGroupConfig`.

The Reader Group can be configured to read from multiple Streams. For example, imagine a situation where there is a collection of Stream of sensor data coming from a factory floor, each machine has its own Stream of sensor data.  We can build applications that uses a Reader Group per Stream so that the
application reasons about data from exactly one machine. We can build other applications that
use a Reader Group configured to read from all of the Streams. To keep it simple, in the sample application the Reader Group only reads from one Stream.

We can call `createReaderGroup` with the same parameters multiple times and the same Reader Group will be returned each time after it is initially created (idempotent operation). Note that in other cases, if the developer knows the name of the Reader Group to use and knows it has already been created, they can use `getReaderGroup()` on `ReaderGroupManager` to retrieve the `ReaderGroup` object by name.

At this point, we have the Scope and Stream is set up and the `ReaderGroup` object created. Next, we need to create a Reader and start reading Events.

## Reading Event using an EventStreamReader

First, we create a `ClientFactory` object, the same way we did it in the
`HelloWorldWriter` application. Then we use the `ClientFactory` to create an `EventStreamReader` object. The following are the four parameters to create a Reader:

 - Name for the Reader.
 - Reader Group it should be part of.
 - The type of object expected on the Stream.
 - Serializer to convert from the bytes stored in Pravega into the Event
objects and a `ReaderConfig`.

```Java
EventStreamReader<String> reader = clientFactory.createReader("reader",
        readerGroup,
        new JavaSerializer<String>(),
        ReaderConfig.builder().build()))
```

The name of the Reader can be any valid Pravega naming convention (numbers and letters). Note that the name of the Reader is namespaced within the Scope. `EventStreamWriter` and `EventStreamReader` uses Java generic types to allow a developer to specify a type safe Reader. In the sample application,
we read Strings from the stream and use the standard Java String Serializer to
convert the bytes read from the stream into String objects.

**Note:**  Pravega allows users to write their own serializer. For more information and example, please refer to [Pravega Serializer](https://github.com/pravega/pravega-serializer). 

Finally, we use a `ReaderConfig` object with default values. Note that you cannot create the same Reader multiple times. That is, an application may call `createReader()` to add new Readers to the Reader Group. But if the Reader Group already contains a Reader with that name, an exception is thrown.

After creating an `EventStreamReader`, we can use it to read
Events from the Stream. The `readNextEvent()` operation returns the next Event available on the Stream, or if there is no such Event, blocks for a specified time. After the expiry of the timeout period, if no Event is available for reading, then _Null_ is returned. The null check (`EventRead<String> event = null`) is used to avoid printing out a spurious _Null_ event message to the console and also used to terminate the loop. Note that the Event itself is wrapped in an `EventRead` object.

It is worth noting that `readNextEvent()` may throw an exception `ReinitializationRequiredException` and the object is reinitialized. This exception would be handled in cases where the Readers in the Reader Group need to reset to a Checkpoint or the Reader Group itself has been altered and the set of Streams being read has been therefore changed. `TruncatedDataException` is thrown when we try to read the deleted data. It is however possible to recover from the later by calling `readNextEvent()` again (it will just skip forward).

Thus, the simple `HelloWorldReader` loops, reading Events from a Stream
until there are no more Events, and then the application terminates.

# Experimental Batch Reader

`BatchClient` is used for applications that require parallel, unordered reads of historical stream data.
Using the Batch Reader all the segments in a Stream can be listed and read from. Hence, the Events for a given Routing Key which can reside on multiple segments are not read in order.

Obviously this API is not for every application, the main advantage is that it allows for low level integration with batch processing frameworks such as `MapReduce`.

#### Example

To iterate over all the segments in the stream:

```Java
//Passing null to fromStreamCut and toStreamCut will result in using the current start of stream and the current end of stream respectively.
Iterator<SegmentRange> segments = client.listSegments(stream, null, null).getIterator();
SegmentRange segmentInfo = segments.next();

```
To read the events from a segment:

```Java
SegmentIterator<T> events = client.readSegment(segmentInfo, deserializer);
while (events.hasNext())
{
    processEvent(events.next());
}

```
#### getNextStreamCut API:  
For a streaming application like Spark, which uses micro-batch reader connectors, needs a streamCut to read Pravega Streams in batches.
```Java
StreamCut startingStreamCut = streamManager.fetchStreamInfo(streamScope, streamName).join().getHeadStreamCut();
long approxDistanceToNextOffset = 50 * 1024 * 1024; // 50MB in bytes
StreamCut nextStreamCut = client.getNextStreamCut(startingStreamCut, approxDistanceToNextOffset);
```
This api provides a streamCut that is a bounded distance from another streamcut. It takes a starting streamCut and an approximate distance in bytes as parameters and return a new stream cut. 
No segments from the starting streamCut is skipped over. The position for each segment in the new StreamCut is either present inside the segment or at the tail of it. 
The successors for the respective segments are called only if its position in the startingStreamCut is at the tail.