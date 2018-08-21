<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Working with Pravega: StreamCuts

This section describes `StreamCut`s and how they can be used with streaming clients and batch clients.
Pre-requisites: You should be familiar with [Pravega Concepts](pravega-concepts.md).

## Definition

A Pravega stream is formed by one or multiple parallel segments for storing/reading events. A Pravega stream
is elastic, which means that the number of parallel segments may change along time to accommodate
fluctuating workloads. That said, a `StreamCut` represents a consistent position in the stream. It contains
a set of segment and offset pairs for a single stream which represents the complete keyspace at a given
point in time. The offset always points to the event boundary and hence there will be no offset pointing to
an incomplete event.

The `StreamCut` representing the tail of the stream (with the newest event) is an ever changing one since
events can be continuously added to the stream and the `StreamCut` pointing to the tail of the stream with
newer events would have a different value. Similarly the `StreamCut` representing the head of the
stream (with the oldest event) is an ever changing one as the stream retention policy could truncate the stream
and the `StreamCut` pointing to the head of the stream post truncation would have a different value.
```StreamCut.UNBOUNDED``` is used to represent such a position in the stream and the user can use it to
specify this ever changing stream position (both head and tail of the stream).

It should be noted that `StreamCut`s obtained using the streaming client and batch client can be used
interchangeably.

## StreamCut with Reader

A ReaderGroup is a named collection of Readers that together, in parallel, read Events from a given Stream. Every
Reader is always associated with a ReaderGroup. `StreamCut`(s) can be obtained from a ReaderGroup using the
following api ```io.pravega.client.stream.ReaderGroup.getStreamCuts ```. This api returns a
```Map<Stream, StreamCut>``` which represents the last known position of the Readers for all the streams managed by
the ReaderGroup.

A `StreamCut` can be used to configure a ReaderGroup to enable bounded processing of a Stream. The start
and/or end `StreamCut` of a Stream can be passed as part of the ReaderGroup configuration. The below example
shows the different ways to use `StreamCut`s as part of the ReaderGroup configuration.

```java
/*
 * The below ReaderGroup configuration ensures that the readers belonging to
 * the ReaderGroup read events from
 *   - Stream "s1" from startStreamCut1 (representing the oldest event) upto
          endStreamCut1 (representing the newest event)
 *   - Stream "s2" from startStreamCut2 upto the tail of the stream, this is similar to using StreamCut.UNBOUNDED
 *        for endStreamCut.
 *   - Stream "s3" from the current head of the stream upto endStreamCut2
 *   - Stream "s4" from the current head of the stream upto the tail of the stream.
 */
ReaderGroupConfig.builder()
                .stream("scope/s1", startStreamCut1, endStreamCut1)
                .stream("scope/s2", startStreamCut2)
                .stream("scope/s3", StreamCut.UNBOUNDED, endStreamCut2)
                .stream("scope/s4")
                .build();
```

The below API can be used to reset an existing ReaderGroup with a new ReaderGroup configuration instead creating a
ReaderGroup.
```
/*
 * ReaderGroup api used to reset a ReaderGroup to a newer ReaderGroup configuration.
 */
io.pravega.client.stream.ReaderGroup.resetReaderGroup(ReaderGroupConfig config)
```
## StreamCut with BatchClient

`StreamCut` representing the current head and current tail of a stream can be obtained using below BatchClient API.
```
/*
 * The API io.pravega.client.batch.BatchClient.getStreamInfo(Stream stream) fetches the StreamCut representing the
 * current head and tail of the stream. StreamInfo.getHeadStreamCut() and StreamInfo.getTailStreamCut() can be
 * used to fetch the StreamCuts.
 */
CompletableFuture<StreamInfo> getStreamInfo(Stream stream);

```
BatchClient can be used to perform bounded processing of the stream given the start and end `StreamCut`s. BatchClient
api ```io.pravega.client.batch.BatchClient.getSegments(stream, startStreamCut, endStreamCut)``` is used to
fetch segments which reside between the given startStreamCut and endStreamCut. With the retrieved segment information
the user can consume all the events in parallel without adhering to time ordering of events.

It must be noted that passing ```StreamCut.UNBOUNDED``` to startStreamCut and endStreamCut will result in using the
current head of stream and the current tail of the stream, respectively.


We have provided a simple yet illustrative example of using StreamCut [here.](https://github.com/pravega/pravega-samples/tree/{{Pravgeversion.gradle.properties}}/pravega-client-examples/src/main/java/io/pravega/example/streamcuts)
