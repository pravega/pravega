
<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
-->

# Pravega Watermarking

  * [Introduction](#introduction)
  * [Overview](#overview)
  * [List of API](#list-of-api)
     - [Client Factory](#client-factory)  
     - [Event Writer API Changes](#event-writer-api-changes)
     - [Event Reader API Changes](#event-reader-api-changes)
 *  [Controller and Watermarking](#controller-and-watermarking)
        - [Writer Marks](#writer-marks)
  * [References](#references)

## Introduction
The goal of watermarking is to provide a time bound from the Writers to the Readers so that they can identify their presence in the Stream based on the specified watermark. If the Writer’s time bound is within the watermark then it indicates the Reader that it has received everything before a `timestamp`.

Thus **time** and its properties are decided based on the following:

1. Different applications may have different requirements for time.
2. In many applications, meaningful time is necessarily related to the data itself.

## Overview
The general approach is add new APIs to allow the Writer to provide time information. This is aggregated by the Controller and stored in a special Segment associated with the Stream. The information stored as `timestamp` and `streamCut` pairs.

- `timestamp` is derived by considering the minimum of the most recently reported `timestamp` across all Writers.
- `streamCut` is a location in the Stream at an event boundary that is at least as far into the Stream as the location of the Writers when the `timestamps` were provided.

The Readers can read these `streamCut`s and `timestamp`s from the special Segment, and use the `ReaderGroupState` to coordinate where they are relative to one another.

# List of API

## Client Factory
When Writers are created, `writerId` would be specified.

```
<T> EventStreamWriter<T> createEventWriter(String writerId, String streamName, Serializer<T> s, EventWriterConfig config);
```
the same goes for transnational writers:

```
<T> TransactionalEventStreamWriter<T> createTransactionalEventWriter(String writerId, String streamName, Serializer<T> s, EventWriterConfig config);

```

## Event Writer API

Alternatively `writerId` can be added as an optional parameter. If `writerId` was not supplied, then `noteTime` exception would be invoked.

A new API `EventStreamWriter` (optional) was added to the public interface.

```
/**
     * Notes a time that can be seen by readers which read from this stream by
     * {@link EventStreamReader#getMostRecentWatermark()}. The semantics or meaning of the timestamp
     * is left to the application. Readers might expect timestamps to be monotonic. So this is
     * recommended but not enforced.
     *
     * There is no requirement to call this method. Never doing so will result in readers invoking
     * {@link EventStreamReader#getMostRecentWatermark()} receiving a null.
     *
     * Calling this method can be automated by setting
     * {@link EventWriterConfigBuilder#automaticallyNoteTime(boolean)} to true when creating a
     * writer.
     *
     * @param timestamp a timestamp that represents the current location in the stream.
     */
    void noteTime(long timestamp);
```
As using this API was optional, instead the user can use a configuration parameter to automate invoking the method that passes wall clock time.

Continuous emission of data would not happen in Transactional Writer as in normal Writer. Instead, new data would be added at the commit time, hence the `commit()` is overloaded by adding an optional `timestamp` parameter. Note that support should be added to the Transactional Writer.

```
/**
     * Commits the transaction similar to {@link #commit()}, but also notes an associated timestamp.
     * Similar to {@link EventStreamWriter#noteTime(long)}.
     *
     * @param timestamp A timestamp associated with this transaction.
     * @throws TxnFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void commit(long timestamp) throws TxnFailedException;
```
The `timestamp` is passed to the Controller as part of the commit. This way a commit simultaneously commits the data and supplies the corresponding `timestamp`.

## Config API
Following is the new parameter for the `StreamConfiguration`:

```
/**
 * The duration after the last call to {@link EventStreamWriter#noteTime(long)} which the
 * timestamp should be considered valid before it is forgotten. Meaning that after this long of
 * not calling {@link EventStreamWriter#noteTime(long)} the writer will be forgotten.
 * If there are no known writers, readers that call {@link EventStreamReader#getCurrentTimeWindow()}
 * will receive a `null` when they are at the corresponding position in the stream.
 */
private final long timestampAggregationTimeout;
```
Following is the new parameter for the `EventWriterConfig`:
```
/**
     * Automatically invoke {@link EventStreamWriter#noteTime(long)} passing
     * {@link System#currentTimeMillis()} on a regular interval.
     */
    private final boolean automaticallyNoteTime;

```
As it takes some time for all the Writers to come online, for a new Stream, an "intentional delay" is added before writing the first mark to the marks Segment.

## Event Reader API
On the Reader side, a new method is added on the `EventStreamReader` to obtain the location of the Reader in terms of time:

```
/**
     * Returns a window which represents the range of time that this reader is currently reading as
     * provided by writers via the {@link EventStreamWriter#noteTime(long)} API.
     *
     * If no writers were providing timestamps at the current position in the stream `null` will be returned.
     *  
     * @return A TimeWindow which bounds the current location in the stream, or null if one cannot be established.
     */
    TimeWindow getCurrentTimeWindow();

```
Where `TimeWindow` is defined as:

```
/**
 * Represents a time window for the events which are currently being read by a reader.
 *
 * The upper time bound is a timestamp which is greater than or equal to any that were provided by
 * any writer via the {@link EventStreamWriter#noteTime(long)} API prior to the current location in
 * the stream.
 *
 * Similarly the lower time bound is a timestamp which is less than or equal to the most recent
 * value provided via the {@link EventStreamWriter#noteTime(long)} API for by any writer using that
 * API at the current location in the stream.
 *
 * upperTimeBound will always be greater than or equal to lowerTimeBound.
 */
@Data
public class TimeWindow {
    private final long lowerTimeBound;
    private final long upperTimeBound;
}

```

Note that the `TimeWindow` that is seen by Readers can be affected by `timestampAggrigationTimeout` in the config above. Different applications may wish to be more or less conservative as to when Writers are 'alive' and need to be considered.

As part of this watermarking feature many  internal API Changes (Server and Client) were made to support this new API.

## Controller and Watermarking
The Controller takes the `timestamps` from all of the Writers on the Stream and write out a `streamCut` that is greater than or equal to the maximum of all Writer’s positions and a `timestamp` that is the minimum of all of the Writers.

A call to `noteTimestampFromWriter` or `commitTransaction` updates a structure with the position of the Writer (or where the Transaction committed) and the `timestamp` provided. In the case of commit the Controller obtains the position information itself, where as in the case of `noteTimestampFromWriter` it is explicitly provided by the client.

To do this there needs to be a single Controller instance responsible for aggregating this data for any given Stream. It may be useful for this to be the same instance that is managing Transactions for that stream.

In the event that a Controller instance crashes, the in memory structure will be temporarily lost. It can be recovered by reading the last value written. During recovery there will be a gap where time does not advance.

There are two main objectives that was achieved:

1. Recording Writer marks.
2. Generating consolidated watermarks.

Writers may contact any arbitrary Controller instance and report their `timestamps` and `Positions`. This Controller conditionally persists the reported value in the shared metadata store. For generating watermarks, all available Controller instances divide ownership of Streams amongst themselves such that each Controller instance is exclusively responsible for computing watermarks for only a subset of Streams. So for any Stream there is exactly for one Controller instance which is periodically generating marks by consolidating all the individual Writer marks which could be processed by any arbitrary Controller instance.

### Writer Marks
There are two types of Writers (Event Writers and Transaction Writers) each with their own characteristics with respect to time to position tracking. So Event Writers and Transaction Writers will have their corresponding marks generated differently. However, their marks will be reported at the same place in the shared metadata store described above.

- **Event Writer Marks**: Event Writers are expected to supply their positions and corresponding times explicitly to any arbitrary Controller instance. This Controller instance will durably record the received mark in the store after checking the mark for sanity.

- **Transaction Writer Marks**: Transaction writers, on the other hand, are only aware of Transaction ids and the Transaction commits happen asynchronously and are orchestrated by Controller. Since Controller knows when a Transaction is committed, it is best suited to compute the "position" where it was committed.

### Watermark Computation
A watermark is composed of a time **T** and `streamCut` **S**. A watermark effectively indicates that "until `streamCut` **S**, all data corresponding to time **T** has been written across all Writers". To compose such a watermark from the given positions of Writers, we need to take the **lowest** time from marks across Writers and compute a greatest upper bound on all positions from across Writers marks. To compute greatest upper bound on, we will loop over each Writer's mark and loop over each Segment in Writer's position and include it in the set conditionally - a Segment is included in upper bound only if none of its successors already exist in the upper bound. An included Segment will replace all its predecessor Segments in the result set. A Segment included in the upper bound will include the highest offset across all Writer marks that contain that Segment.

It is important to understand that the Writer's position is not same as a `streamCut` because a Writer could be writing concurrently to a predecessor and successor Segment but for different Routing Keys. This means any composition of Writer positions may not yield a well formed and complete `streamCut`. So the above computed greatest upper bound may have missing key ranges whereby we will need to augment above algorithm to compute a complete `streamCut`.

# References
Watermarking in other systems:
- [Millwheel](https://ai.google/research/pubs/pub41378.pdf)
- [Dataflow](https://ai.google/research/pubs/pub43864)
- [Gigascope](https://vldb.org/conf/2005/papers/p1079-johnson.pdf)
