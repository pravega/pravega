<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Working with Pravega: StreamCuts

This section describes about StreamCuts and how it can be used with streaming clients and
batch clients.
You really should be familiar with Pravega Concepts (see Pravega Concepts) and Working with
and Writers (see Working with Reader and Writer) before continuing to read this page.

## Definition

StreamCut represents a consistent position in the stream. It essentially contains a set of segment
and offset pairs for a single stream which represents complete keyspace. The offsets point to the
correct event boundary across the keyspace.

Given that data can be continously added to a stream the streamcut representing the tail of the
stream(with the newest event) is an ever changing one. Similiary the stream cut representing the
head of the stream(with the oldest event) is an ever changing one as the stream retention policy
could truncate the stream. StreamCut.UNBOUNDED is used to represent such a position in the stream and the user can use it to
specify this ever changing stream position.

It should be noted that streamcuts obtained using the streaming client and batch client can be used
interchangably.

## StreamCut with Streaming clients.

StreamCut(s) can be obtained from a Reader group using the following api.
```io.pravega.client.stream.ReaderGroup.getStreamCuts ```. This api returns a ```Map<Stream, StreamCut>``` which
contains a StreamCut for every stream managed by the ReaderGroup.

A StreamCut can be used to configure a ReaderGroup to enable bounded processing of a Stream. The start
and/or end StreamCut of a Stream can be passed as part of the ReaderGroup configuration. The below example
shows the different ways to use StreamCuts as part of the reader group configuration.

```
/*
 * The below ReaderGroup configuration ensures that the readers belonging to
 * the ReaderGroup read events from
 *   - Stream "s1" from startStreamCut1 (representing the oldest event) upto endStreamCut1 (representing the newest event)
 *   - Stream "s2" from startStreamCut2 upto the tail of the stream, this is similar to using StreamCut.UNBOUNDED
 *        for endStreamCut.
 *   - Stream "s3" from the current head of the stream upto endStreamCut2
 *   - Stream "s4" from the current head of the stream upto the tail of the stream.
 */
ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream("scope/s1", startStreamCut1, endStreamCut1)
                .stream("scope/s2", startStreamCut2)
		        .stream("scope/s3", StreamCut.UNBOUNDED, endStreamCut2)
                .stream("scope/s4")
                .build();

```

It should be noted that apart from creating a new ReaderGroup with the new configuration we can reset an existing
readergroup to the newer configuration.
```
/*
 * ReaderGroup api used to reset a ReaderGroup to a newer ReaderGroup configuration.
 */
io.pravega.client.stream.ReaderGroup.resetReaderGroup(ReaderGroupConfig config)
```
## StreamCut with BatchClient.

StreamCut representing the current HEAD and current TAIL of a stream can be obtained using below BatchClient API.
```
/*
 * The API io.pravega.client.batch.BatchClient.getStreamInfo(Stream stream) fetches the streamCut representing the
 * current head and tail of the stream. StreamInfo.getHeadStreamCut() and StreamInfo.getTailStreamCut() can be used to
 * fetch the streamCuts.
 */
CompletableFuture<StreamInfo> getStreamInfo(Stream stream);

```

BatchCLient api ```io.pravega.client.batch.BatchClient.getSegments(stream, startStreamCut, endStreamCut)``` is used to
fetch segments which reside between the provided startStreamCut and endStreamCut. With the retrieved segment information
the user can consume all the events in parallel without adhering to time ordering of events.

It must be noted that the passing StreamCut.UNBOUNDED to startStreamCut and endStreamCut will result in using the current
start of stream and the current end of stream respectively.
