<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Working with Pravega: Basic Reader and Writer

Lets examine how to build simple Pravega applications.  The simplest kind of
Pravega application uses a Pravega Reader to read from a Pravega Stream or a
Pravega Writer that writes to a Pravega Stream.  A simple example of both can be
found in the Pravega Samples "hello world" app. These sample applications
provide a very basic example of how a Java application could use the Pravega
Java Client Library to access Pravega functionality.

Instructions for running the sample applications can be found in the [Pravega
Samples
readme](https://github.com/pravega/pravega-samples/blob/v0.4.0/pravega-client-examples/README.md).

You really should be familiar with Pravega Concepts (see Pravega Concepts)
before continuing reading this page.

## HelloWorldWriter

The `HelloWorldWriter` application is a simple demonstration of using the
`EventStreamWriter` to write an Event to Pravega.

Taking a look first at the `HelloWorldWriter` example application, the key part of
the code is in the `run()` method:

```java

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

The purpose of the `run()` method is to create a Stream (lines 2-9) and output the
given Event to that Stream (lines 10-18).

## Creating a Stream and the StreamManager Interface

A Stream is created in the context of a Scope; the Scope acts as a namespace
mechanism so that different sets of Streams can be categorized for some purpose.
 For example, I might have a separate scope for each application.  I might
choose to create a set of Scopes, one for each department in my organization.
 In a multi-tenant environment, I might have a separate Scope per tenant.  As a
developer, I can choose whatever categorization scheme I need and use the Scope
concept for organizing my Streams along that categorization scheme.

Scopes and Streams are created and manipulated via the StreamManager Interface
to the Pravega Controller. You need to have a URI to any of the Pravega
Controller instances in your cluster in order to create a StreamManager object.
 This is shown in line 2.

In the setup for the HelloWorld sample applications, the controllerURI is
configured as a command line parameter when the sample application is launched.
 For the "single node" deployment of Pravega, the Controller is listening on
localhost, port 9090.

The StreamManager provides access to various control plane functions in Pravega
related to Scopes and Streams:

| **Method**      | **Parameters**                                                    | **Discussion**                                                                                                 |
|-----------------|-------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| (static) create | (URI controller)                                                  | Given a URI to one of the Pravega Controller instances in the Pravega Cluster, create a Stream Manager object. |
| createScope     | (String scopeName)                                                | Creates a Scope with the given name.                                                                           |
|                 |                                                                   | Returns true if the Scope is created, returns false if the Scope already exists.                                                                                                               |
|                 |                                                                   | You can call this method even if the Scope already exists, it won't harm anything.                                                                                                               |
| deleteScope     | (String scopeName)                                                | Deletes a Scope with the given name.                                                                           |
|                 |                                                                   | Returns true if the scope was deleted, false otherwise.                                                                                                               |
|                 |                                                                   | Note, if the Scope contains Streams, the deleteScope operation will fail with an exception.                                                                                                               |
|                 |                                                                   | If you delete a nonexistent Scope, the method will succeed and return false.                                                                                                               |
| createStream    | (String scopeName, String streamName, StreamConfiguration config) | Create a Stream within a given Scope.                                                                          |
|                 |                                                                   | Note that both scope name and stream name are limited by the following pattern: [a-zA-Z0-9]+ (i.e. letters and numbers only, no punctuation)                                                                                                               |
|                 |                                                                   | Note also: the Scope must exist, an exception is thrown if you create a Stream in a nonexistent scope.                                                                                                               |
|                 |                                                                   | A StreamConfiguration is built using a builder pattern                                                                                                               |
|                 |                                                                   | Returns true if the Stream is created, returns false if the Stream already exists.                                                                                                               |
|                 |                                                                   | You can call this method even if the Stream already exists, it won't harm anything.                                                                                                               |
| updateStream    | (String scopeName, String streamName, StreamConfiguration config) | Swap out the Stream's configuration.                                                                           |
|                 |                                                                   | Note the Stream must already exist, an exception is thrown if you update a nonexistent stream.                                                                                                              |
|                 |                                                                   | Returns true if the Stream was changed                                                                                                               |
| sealStream      | (String scopeName, String streamName)                             | Prevent any further writes to a Stream                                                                         |
|                 |                                                                   | Note the Stream must already exist, an exception is thrown if you seal a nonexistent stream.                                                                                                                |
|                 |                                                                   | Returns true if the Stream is successfully sealed                                                                                                               |
| deleteStream    | (String scopeName, String streamName)                             | Remove the Stream from Pravega and recover any resources used by that Stream                                   |
|                 |                                                                   | Note the Stream must already exist, an exception is thrown if you delete a nonexistent stream.                                                                                                              |
|                 |                                                                   | Returns true if the stream was deleted.                                                                                                               |

After line 3 in the code is finished, we have established that the Scope exists,
we can then go on and create the Stream in lines 5-8. 

The StreamManager needs 3 things to create a Stream, the Scope's name, the
Stream's name and a StreamConfiguration.  The most interesting task is to create
the StreamConfiguration.

Like many objects in Pravega, a Stream takes a configuration object that allows
a developer to control various behaviors of the Stream.  All configuration
objects in Pravega use a builder pattern for construction.  There are really two
important configuration items related to streams: Retention Policy and Scaling
Policy.  

Retention Policy allows the developer to control how long data is kept in a
Stream before it is deleted.  S/he can specify data should be kept for a certain
period of time (ideal for situations like regulatory compliance that mandate
certain retention periods) or to retain data until a certain number of bytes
have been consumed.  At the moment, Retention Policy is not completely
implemented.  By default, the RetentionPolicy is set as "unlimited" meaning, data
will not be removed from the Stream.

Scaling Policy is the way developers configure a Stream to take advantage
Pravega's auto-scaling feature.  In line 6, we use a fixed policy, meaning the
Stream is configured with the given number of Stream Segments and that won't
change.  The other options are to scale by a given number of Events per second
or a given number of Kilobytes per second.  In these two policies, the developer
specifies a target rate, a scaling factor and a minimum number of Segments.  The
target rate is straight forward, if ingest rate exceeds a certain number of
Events or Kilobytes of data for a sustained period of time, Pravega will attempt
to add new Stream Segments to the Stream.  If the rate drops below that
threshold for a sustained period of time, Pravega will attempt to merge adjacent
Stream Segments.  The scaling factor is a setting on the Scaling Policy that
determines how many Stream Segments should be added when the target rate (of
Events or Kilobytes) is exceeded.  The minimum number of Segments is a factor
that sets the minimum degree of read parallelism to be maintained; if this value
is set at 3, for example, there will always be 3 Stream Segments available on
the Stream.  Currently, this property is effective only when the stream is
created; at some point in the future, update stream will allow this factor to be
used to change the minimum degree of read parallelism on an existing Stream.

Once the `StreamConfiguration` object is created, creating the Stream is straight
forward (line 8).  After the Stream is created, we are all set to start writing
Event(s) to the Stream.

## Writing an Event using EventWriter

Applications use an `EventStreamWriter` object to write Events to a Stream.  The
key object to creating the `EventStreamWriter` is the ClientFactory.  The
ClientFactory is used to create Readers, Writers and other types of Pravega
Client objects such as the State Synchronizer (see [Working with Pravega: State
Synchronizer](state-synchronizer.md)).

Line 10 shows the creation of a ClientFactory.  A ClientFactory is created in
the context of a Scope, since all Readers, Writers and other Clients created by
the ClientFactory are created in the context of that Scope.  The ClientFactory
also needs a URI to one of the Pravega Controllers, just like StreamManager.

Because ClientFactory and the objects it creates consumes resources from
Pravega, it is a good practice to create these objects in a try-with-resources
statement.  Because ClientFactory and the objects it creates all implement
Autocloseable, the try-with-resources approach makes sure that regardless of how
your application ends, the Pravega resources will be properly closed in the
right order.

Now that we have a ClientFactory, we can use it to create a Writer.  There are
several things a developer needs to know before s/he creates a Writer:

1.  What is the name of the Stream to write to?  Note: the Scope has already
    been determined when the ClientFactory was created

2.  What Type of Event objects will be written to the Stream?

3.  What serializer will be used to convert an Event object to bytes?  Recall
    that Pravega only knows about sequences of bytes, it does not really know
    anything about Java objects.

4.  Does the Writer need to be configured with any special behavior?

In our example, lines 11-13 show all these decisions.  This Writer writes to the
Stream specified in the configuration of the HelloWorldWriter object itself (by
default the stream is named "helloStream" in the "examples" Scope).  The Writer
processes Java String objects as Events and uses the built in Java serializer
for Strings.  

The EventWriterConfig allows the developer to specify things like the number of
attempts to retry a request before giving up and associated exponential back
settings.  Pravega takes care to retry requests in the case where connection
failures or Pravega component outages may temporarily prevent a request from
succeeding, so application logic doesn't need to be complicated with dealing
with intermittent cluster failures.  In our case, we took the default settings
for EventWriterConfig in line 13.

Now we can write the Event to the Stream as shown in line 17.  EventStreamWriter
provides a writeEvent() operation that writes the given non-null Event object to
the Stream using a given routing key to determine which Stream Segment it should
appear on.  Many operations in Pravega, such as writeEvent(), are asynchronous
and return some sort of Future object.  If the application needed to make sure
the Event was durably written to Pravega and available for Readers, it could
wait on the Future before proceeding.  In the case of our simple "hello world"
example, we don't bother waiting.

EventStreamWriter can also be used to begin a Transaction.  We cover
Transactions in more detail elsewhere ([Working with Pravega:
Transactions](transactions.md)).

With the need to write events in sequence into Pravega stream, there are two options:
1. Create a stream with fixed one segment only, using ScalingPolicy.fixed(1); no writing parallelism for 
this option as there is only one segment available. 
2. Create a stream with fixed multiple segments using ScalingPolicy.fixed(n), then write event directly to 
a segment using writer.writeEvent(segmentId, message). Note segmentId is between 0 to (n-1) inclusive.
With this option, writing parallelism could be provided by the client side instead of Pravega. 

That's it for writing Events.  Now lets take a look at how to read Events using
Pravega.

## HelloWorldReader

The HelloWorldReader is a simple demonstration of using the EventStreamReader.
The application simply reads Events from the given Stream and prints a string
representation of those Events onto the console.

Just like the HelloWorldWriter example, the key part of the HelloWorldReader app
is in the run() method:

```java
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

Lines 2-8 set up the Scope and Stream just like in the HelloWorldWriter
application.  Lines 10-15 set up the ReaderGroup as the prerequisite to creating
the EventStreamReader and using it to read Events from the Stream (lines 17-36).

## ReaderGroup Basics

Any Reader in Pravega belongs to some ReaderGroup.  A ReaderGroup is a grouping
of one or more Readers that consume from a Stream in parallel.  Before we create
a Reader, we need to either create a ReaderGroup (or be aware of the name of an
existing ReaderGroup).  This application only uses the basics from ReaderGroup.

Lines 10-15 show basic ReaderGroup creation.  ReaderGroup objects are created
from a ReaderGroupManager object.  The ReaderGroupManager object, in turn, is
created on a given Scope with a URI to one of the Pravega Controllers, very much
like a ClientFactory is created.  A ReaderGroupManager object is created on line
14.  Note the creation is also in a try-with-resources statement to make sure
the ReaderGroupManager is properly cleaned up.   The ReaderGroupManager allows a
developer to create, delete and retrieve ReaderGroup objects by name.

To create a ReaderGroup, the developer needs a name for the ReaderGroup, a
configuration with a set of 1 or more Streams to read from.  

The ReaderGroup's name might be meaningful to the application, like
"WebClickStreamReaders".  In our case, on line 10, we have a simple UUID as the
name (note the modification of the UUID string to remove the "-" character
because ReaderGroup names can only have letters and numbers).  In cases where
you will have multiple Readers reading in parallel and each Reader in a separate
process, it is helpful to have a human readable name for the ReaderGroup.  In
our case, we have one Reader, reading in isolation, so a UUID is a safe way to
name the ReaderGroup.  Since the ReaderGroup is created via the
ReaderGroupManager and since the ReaderGroupManager is created within the
context of a Scope, we can safely conclude that ReaderGroup names are namespaced
by that Scope.  

The ReaderGroupConfig right now doesn't have much behavior.  The developer
specifies the Stream which should be part of the ReaderGroup and its lower and
upper bounds. In our case, on line 11, we start at the beginning of the Stream.
Other configuration items, such as specifying checkpointing etc. are options
that will be available through the ReaderGroupConfig.  But for now, we keep it
simple.

The fact that a ReaderGroup can be configured to read from multiple Streams is
kind of cool.  Imagine a situation where I have a collection of Stream of sensor
data coming from a factory floor, each machine has its own Stream of sensor
data.  I can build applications that use a ReaderGroup per Stream so that the
app reasons about data from exactly one machine.  I can build other apps that
use a ReaderGroup configured to read from all of the Streams.  In our case, on
line 14, the ReaderGroup only reads from one Stream.

You can call createReaderGroup() with the same parameters multiple times, it
doesn't hurt anything, and the same ReaderGroup will be returned each time after
it is initially created.

Note that in other cases, if the developer knows the name of the ReaderGroup to
use and knows it has already been created, s/he can use getReaderGroup() on
ReaderGroupManager to retrieve the ReaderGroup object by name.

So at this point in the code, we have the Scope and Stream set up, we have the
ReaderGroup created and now we need to create a Reader and start reading Events.

## Reading Event using an EventStreamReader

Lines 17-36 show an example of setting up an EventStreamReader and reading
Events using that EventStreamReader.

First, we create a ClientFactory on line 17, in the same way we did it in the
HelloWorldWriter app.  

Then we use the ClientFactory to create an EventStreamReader object.  There are
four things the developer needs to create a Reader: a name for the reader, the
readerGroup it should be part of, the type of object expected on the Stream, the
serializer to use to convert from the bytes stored in Pravega into the Event
objects and a ReaderConfig.  Lines 18-21 show the creation of an
EventStreamReader.  The name of the Reader can be any valid Pravega name
(numbers and letters).  Of course, the name of the reader is namespaced within
the Scope.  We talked about the creation of the ReaderGroup in the previous
section.  Just like with the EventStreamWriter, EventStreamReader uses Java
generic types to allow a developer to specify a type safe Reader.  In our case,
we read Strings from the stream and use the standard Java String Serializer to
convert the bytes read from the stream into String objects.  Finally, the
ReaderConfig is created, but at the moment, there are no configuration items
associated with a Reader, so the empty ReaderConfig is just a place holder as
Pravega evolves to include configuration items on Readers.

Note that you cannot create the same Reader multiple times.  Basically overtime
you call createReader() it tries to add the Reader to the ReaderGroup.  If the
ReaderGroup already contains a Reader with that name, an exception is thrown.

Now that we have an EventStreamReader created, we can start using it to read
Events from the stream.  This is done on line 26.  The readNextEvent() operation
returns the next Event available on the Stream, or if there is no such Event,
blocks for a specified timeout period.  If, after the timeout period has expired
and no Event is available for reading, null is returned. That is why there is a
null check on line 27 (to avoid printing out a spurious "null" event message to
the console).  It is also used as the termination of the loop on line 34.  Note
that the Event itself is wrapped in an EventRead object.

It is worth noting that readNextEvent() may throw an exception (handled in lines
30-33).  This exception would be handled in cases where the Readers in the
ReaderGroup need to be reset to a checkpoint or the ReaderGroup itself has been
altered and the set of Streams being read has therefore been changed.

So that's it.  The simple HelloWorldReader loops, reading Events from a Stream
until there are no more Events, and then the application terminates.

# Experimental batch reader

For applications that want to perform batch reads of historical stream data, the BatchClient provides a way to do this.
It allows for listing all of the segments in a stream, and reading their data.

When the data is read this way, rather than joining a reader group which automatically partitions the data, the underlying structure of the stream is exposed and it is up to the application to decide how to process it. So events read in this way need not be read in order.

Obviously this API is not for every application, the main advantage is that it allows for low level integration with batch processing frameworks such as MapReduce.

As an example to iterate over all the segments in the stream:
```java
//Passing null to fromStreamCut and toStreamCut will result in using the current start of stream and the current end of stream respectively.
Iterator<SegmentRange> segments = client.listSegments(stream, null, null).getIterator();
SegmentRange segmentInfo = segments.next();
```
Or to read the events from a segment:
```java
SegmentIterator<T> events = client.readSegment(segmentInfo, deserializer);
while (events.hasNext()) {
    processEvent(events.next());
}
```
