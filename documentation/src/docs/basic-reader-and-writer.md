<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Working with Pravega: Basic Reader and Writer

Lets examine how to build simple Pravega applications. The simplest kind of
Pravega application uses a Pravega Reader to read from a Pravega Stream or a
Pravega Writer that writes to a Pravega Stream. A simple example of both can be
found in the [Pravega Samples](https://github.com/pravega/pravega-samples/blob/v0.4.0/pravega-client-examples/README.md) `HelloWorld` application. These sample applications
provide a very basic example of how a Java application could use the Pravega
Java Client Library to access Pravega functionality.

Instructions for running the sample applications can be found in the [Pravega
Samples](https://github.com/pravega/pravega-samples/blob/v0.4.0/pravega-client-examples/README.md).

Get familiar with [Pravega Concepts](http://pravega.io/docs/latest/pravega-concepts/)
before trying the sample applications.

## HelloWorldWriter

The `HelloWorldWriter` application is a simple demonstration of using the
`EventStreamWriter` to write an Event to Pravega.

Taking a look first at the `HelloWorldWriter` example application, the key part of
the code is in the `run()` method:


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
                                                   EventWriterConfig.builder().build())) {

         System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                message, routingKey, scope, streamName);
         final CompletableFuture<Void> writeFuture = writer.writeEvent(routingKey, message);
    }
}

```

The purpose of the `run()` method is to create a Stream and output the
given Event to that Stream.

## Creating a Stream and the StreamManager Interface

Pravega Stream names are organized within Scopes. A Scope acts as a namespace for Stream names; all Stream names are unique within their Scope. Therefore, a Stream is uniquely identified by the combination of its name and Scope. Developers can also define meaningful Scope names, such as "FactoryMachines" or "HRWebsitelogs", to effectively organize collections of Streams. For example, Scopes can be used to classify Streams by tenant (in a multi tenant environment), by department in an organization or by geographic location.


Scopes and Streams are created and manipulated via the `StreamManager` interface
to the Pravega Controller. An URI(`StreamManager.create(controllerURI)`) is a must to any of the Pravega
Controller instances in the cluster in order to create a `StreamManager` object. In the setup for the `HelloWorld` sample applications, the `controllerURI` is
configured as a command line parameter when the sample application is launched.

**Note:** For the "single node" deployment of Pravega, the Controller is listening on
localhost, port 9090.

The `StreamManager` provides access to various control plane functions in Pravega
related to Scopes and Streams:

| **Method**      | **Parameters**                                                    | **Discussion**                                                                                                 |
|-----------------|-------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| (static) `create` | (URI controller)                                                  | Given a URI to one of the Pravega Controller instances in the Pravega Cluster, create a Stream Manager object. |
| `createScope`     | (String scopeName)                                                | Creates a Scope with the given name.                                                                           |
|                 |                                                                   | Returns true if the Scope is created, returns false if the Scope already exists.                                                                                                               |
|                 |                                                                   | You can call this method even if the Scope already exists, it won't harm anything.                                                                                                               |
| `deleteScope`     | (String scopeName)                                                | Deletes a Scope with the given name.                                                                           |
|                 |                                                                   | Returns true if the scope was deleted, false otherwise.                                                                                                               |
|                 |                                                                   | **Note:** If the Scope contains Streams, the `deleteScope` operation will fail with an exception.                                                                                                               |
|                 |                                                                   | If you delete a nonexistent Scope, the method will succeed and return false.                                                                                                               |
| `createStream`    | (String scopeName, String streamName, StreamConfiguration config) | Create a Stream within a given Scope.                                                                          |
|                 |                                                                   | **Note:** Both Scope name and Stream name are limited by the following pattern: [a-zA-Z0-9]+ (i.e. letters and numbers only, no punctuation)                                                                                                               |
|                 |                                                                   | **Note:** The Scope must exist, an exception is thrown if we create a Stream in a nonexistent Scope.                                                                                                               |
|                 |                                                                   | A Stream Configuration is built using a builder pattern                                                                                                               |
|                 |                                                                   | Returns true if the Stream is created, returns false if the Stream already exists.                                                                                                               |
|                 |                                                                   | This method can be called even if the Stream is already existing.                                                                                                               |
| `updateStream`    | (String scopeName, String streamName, StreamConfiguration config) | Swap out the Stream's configuration.                                                                           |
|                 |                                                                   | **Note:** The Stream must already exist, an exception is thrown if we update a nonexistent Stream.                                                                                                              |
|                 |                                                                   | Returns true if the Stream was changed.                                                                                                               |
| `sealStream`      | (String scopeName, String streamName)                             | Prevent any further writes to a Stream.                                                                        |
|                 |                                                                   | **Note:** The Stream must already exist, an exception is thrown if you seal a nonexistent Stream.                                                                                                                |
|                 |                                                                   | Returns true if the Stream is successfully sealed.                                                                                                               |
| `deleteStream`    | (String scopeName, String streamName)                             | Remove the Stream from Pravega and recover any resources used by that Stream.                                  |
|                 |                                                                   | **Note:** The Stream must already exist, an exception is thrown if we delete a nonexistent Stream.                                                                                                              |
|                 |                                                                   | Returns true if the Stream was deleted.                                                                                                               |



The execution of API `createScope(scope)` establishes that the Scope exists. Then we can create the Stream using the API `createStream(scope, streamName, streamConfig)`.

The `StreamManager` requires three parameters to create a Stream:

- Scope's name
- Stream's name
- Stream Configuration.

The most interesting task is to create the Stream Configuration(`streamConfig`). Like many objects in Pravega, a Stream takes a configuration object that allows
a developer to control various behaviors of the Stream.  All configuration
objects in Pravega use a builder pattern for construction. **Retention Policy** and **Scaling
Policy** are the two important configuration items related to streams.   

**Retention Policy:** It allows the developer to control how long the data is kept in a
Stream before it is deleted. The developer can specify the time limit to keep the data for a certain
period of time (ideal for situations like regulatory compliance that mandate
certain retention periods) or can retain the data until a certain number of bytes
have been consumed. In Pravega, the Retention Policy is in  WIP state and is not completely
implemented. By default, the Retention Policy is set to "unlimited" (data will not be removed from the Stream).

**Scaling Policy:** When a Stream is created, it is configured with a Scaling Policy that determines how a Stream handles the varying changes in its load. Developers configure a Stream to take advantage
Pravega's Auto Scaling feature. Pravega has three kinds of Scaling Policy:

1. **Fixed:** The API `ScalingPolicy.fixed`, states that the number of Stream Segments does not vary with load.

2. **Data-based:** Pravega splits a Stream Segment into multiple ones (i.e., Scale-up Event) if the number of bytes per second written to that Stream Segment increases beyond a defined threshold. Similarly, Pravega merges two adjacent Stream Segments (i.e., Scale-down Event) if the number of bytes written to them fall below a defined threshold. Note that, even if the load for a Stream Segment reaches the defined threshold, Pravega does not immediately trigger a Scale-up/down Event. Instead, the load should be satisfying the scaling policy threshold for a [sufficient amount of time]((https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/stream/ScalingPolicy.java).

3. **Event-based:** To the data-based scaling policy, but it uses number of Events instead of bytes.

The minimum number of Segments is a factor that sets the minimum degree of read parallelism to be maintained; for example if this value is set as three, there will always be three Stream Segments available on the Stream.  Currently, this property is effective only when the stream is
created; at some point in the future, update stream will allow this factor to be
used to change the minimum degree of read parallelism on an existing Stream.

Once the Stream Configuration (`streamConfig`) object is created, creating the Stream is straight
forward using `createStream()`. After the Stream is created, we are all set to start writing
Event(s) to the Stream.


## Writing an Event using EventWriter

Applications use an `EventStreamWriter` object to write Events to a Stream.  The
key object to creating the `EventStreamWriter` is the `ClientFactory`.  The
`ClientFactory` is used to create Readers, Writers and other types of Pravega
Client objects such as the State Synchronizer (see [Working with Pravega: State
Synchronizer](state-synchronizer.md)).

A `ClientFactory` is created in the context of a Scope, since all Readers, Writers and other Clients created by the `ClientFactory` are created in the context of that Scope. The `ClientFactory`
also needs a URI to one of the Pravega Controllers (`ClientFactory.withScope(scope, controllerURI)`) , just like `StreamManager`.

As the `ClientFactory` and the objects it creates consumes resources from
Pravega, it is a good practice to create these objects in a try-with-resources
statement.  Since the `ClientFactory` and the objects it creates, implements
`Autocloseable` feature, the try-with-resources approach makes sure that, regardless of how
the application ends, the Pravega resources will be properly closed in the
right order.

Once the `ClientFactory` is made ready, we can use it to create a Writer. There are
several things a developer needs to know before creating a Writer:

1.  What is the name of the Stream to write to?
    (**Note:** The Scope has already
    been determined when the `ClientFactory` was created.)

2.  What Type of Event objects will be written to the Stream?

3.  What serializer will be used to convert an Event object to bytes? (Recall
    that Pravega only knows about sequences of bytes, it is unaware about Java objects.)

4.  Does the Writer need to be configured with any special behavior?


```Java
     EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                                                      new JavaSerializer<String>(),
                                               EventWriterConfig.builder().build()))
```
This Writer writes to the
Stream specified in the configuration of the `HelloWorldWriter` object itself (by
default the stream is named "helloStream" in the "examples" Scope). The Writer
processes Java String objects as Events and uses the built in Java serializer
for Strings.  

The `EventWriterConfig` allows the developer to specify things like the number of
attempts to retry a request before giving up and associated exponential back
settings. Pravega takes care to retry requests in the case where connection
failures or Pravega component outages, may temporarily prevent a request from
succeeding, so application logic doesn't need to be complicated with dealing
with intermittent cluster failures. In Pravega the default settings
for `EventWriterConfig` was considered.

`EventStreamWriter` provides a `writeEvent()` operation that writes the given non-null Event object to
the Stream using a given `Routing key` to determine which Stream Segment it should
appear on.  Many operations in Pravega, such as `writeEvent()`, are asynchronous
and return some sort of `Future` object. If the application needed to make sure
the Event was durably written to Pravega and available for Readers, it could
wait on the `Future` before proceeding. In the case of Pravega's `HelloWorld`
example, we don't bother waiting.

`EventStreamWriter` can also be used to begin a Transaction.  We cover
Transactions in more detail elsewhere ([Working with Pravega:
Transactions](transactions.md)).

## HelloWorldReader

The `HelloWorldReader` is a simple demonstration of using the `EventStreamReader`.
The application simply reads Events from the given Stream and prints a string
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
   try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
       readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
   }

   try (ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        EventStreamReader<String> reader = clientFactory.createReader("reader",
                                                                      readerGroup,
                                                     new JavaSerializer<String>(),
                                                  ReaderConfig.builder().build())) {
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
```

The API `streamManager.createScope()` and `streamManager.createStream()` set up the Scope and Stream just like in the `HelloWorldWriter`
application. The API `ReaderGroupConfig` set up the Reader Group as the prerequisite to creating
the `EventStreamReader` and using it to read Events from the Stream.(`createReader()`,`reader.readNextEvent()`).

## ReaderGroup Basics

Any Reader in Pravega belongs to some Reader Group.  A Reader Group is a grouping
of one or more Readers that consume from a Stream in parallel. Before we create
a Reader, we need to either create a ReaderGroup (or be aware of the name of an
existing Reader Group). This application only uses the basics from Reader Group.

Reader Group objects are created from a `ReaderGroupManager` object. The `ReaderGroupManager` object, in turn, is created on a given Scope with a URI to one of the Pravega Controllers, very much
like a `ClientFactory` is created. Note that, the `createReaderGroup()` is also in a try-with-resources statement to make sure that the `ReaderGroupManager` is properly cleaned up. The `ReaderGroupManager` allows a developer to create, delete and retrieve Reader Group objects using the name.

To create a Reader Group, the developer needs a name for the Reader Group, a
configuration with a set of one or more Streams to read from.  

The Reader Group's name might be meaningful to the application, like
"WebClickStreamReaders".  In our example ```UUID.randomUUID().toString().replace("-", "")``` we have a simple UUID as the
name (*note the modification of the UUID string to remove the "-" character
because Reader Group names can only have letters and numbers*). In cases where
we require multiple Readers reading in parallel and each Reader in a separate
process, it is helpful to have a human readable name for the Reader Group. In this example, we have one Reader, reading in isolation, so a UUID is a safe way to
name the Reader Group. Since the Reader Group is created via the
`ReaderGroupManager` and since the `ReaderGroupManager` is created within the
context of a Scope, we can safely conclude that Reader Group names are namespaced
by that Scope.  

The `ReaderGroupConfig` right now doesn't have much behavior. The developer
specifies the Stream which should be the part of the Reader Group and its lower and
upper bounds. In our case, on line 11, we start at the beginning of the Stream.
Other configuration items, such as specifying checkpointing etc., are options
that will be available through the `ReaderGroupConfig`.

The Reader Group can be configured to read from multiple Streams. For example, imagine a situation where there is a collection of Stream of sensor data coming from a factory floor, each machine has its own Stream of sensor data.  We can build applications that use a Reader Group per Stream so that the
application reasons about data from exactly one machine. We can build other applications that
use a Reader Group configured to read from all of the Streams. In the sample application, the Reader Group only reads from one Stream.

We can call `createReaderGroup()` with the same parameters multiple times and the same Reader Group will be returned each time after it is initially created.

In cases when the developer knows the name of the required existing Reader Group, s/he can use `getReaderGroup()` on
`ReaderGroupManager` to retrieve the Reader Group object by name. Once, the Scope and Stream is set up, then the Reader Group can be created using the API `createReaderGroup()` and we can create a Reader and start reading Events.

## Reading Event using an EventStreamReader

First, we create a `ClientFactory` object, the same way we did it in the
`HelloWorldWriter` application. Then we use the `ClientFactory` to create an `EventStreamReader` object. The following are the four parameters to create a Reader:

 - Name for the Reader.
 - Reader Group it should be part of.
 - The type of object expected on the Stream.
 - Serializer to convert from the bytes stored in Pravega into the Event
objects and a `ReaderConfig`.

```
EventStreamReader<String> reader = clientFactory.createReader("reader",
                                                              readerGroup,
                                             new JavaSerializer<String>(),
                                          ReaderConfig.builder().build()))
  ```                                        
The name of the Reader can be any valid Pravega naming convention (numbers and letters). Note that the name of the Reader is namespaced within the Scope. `EventStreamWriter` and `EventStreamReader` uses Java generic types to allow a developer to specify a type safe Reader. In the sample application,
we read Strings from the stream and use the standard Java String Serializer to
convert the bytes read from the stream into String objects. Finally, the
`ReaderConfig` is created, but at the moment, there are no configuration items
associated with a Reader, so the empty `ReaderConfig` is just a place holder as
Pravega evolves to include configuration items on Readers.

Note that you cannot create the same Reader multiple times. Basically overtime
you can call `createReader()` and it tries to add the Reader to the Reader Group. If the
Reader Group already contains a Reader with that name, an exception is thrown.

Now that we have created an `EventStreamReader`, we can use it to read
Events from the stream. The `readNextEvent()` operation returns the next Event available on the Stream, or if there is no such Event, blocks for a specified time. After the expiry of the timeout period, if no Event is available for reading, then null is returned. The null check (`EventRead<String> event = null`) is used to avoid printing out a spurious "null" event message to the console and also used to terminate the loop. Note that the Event itself is wrapped in an `EventRead` object.

It is worth noting that `readNextEvent()` may throw an exception and this exception would be handled in cases where the Readers in the Reader Group need to be reset to a checkpoint or the Reader Group itself has been altered and the set of Streams being read has been therefore changed.

Thus, the simple `HelloWorldReader` loops, reading Events from a Stream
until there are no more Events, and then the application terminates.

# Experimental Batch Reader

For applications that requires batch reads of historical stream data, the `BatchClient` provides a way to do this. It allows for listing all of the segments in a stream, and reading their data. When the data is read this way, rather than joining a Reader Group which automatically partitions the data, the underlying structure of the stream is exposed and it is up to the application to decide how to process it. So events read in this way need not be read in order.

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
while (events.hasNext()) {
    processEvent(events.next());
}
```
