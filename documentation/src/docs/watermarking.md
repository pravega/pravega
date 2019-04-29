# Pravega Watermarking
  * [Introduction](#introduction)
  * [Overview](#overview)
  * [API Changes](#api-changes)
     - [Client Factory Changes](#client-factory-changes)  
     - [Event Writer API Changes](event-writer-api-changes)
     - [Event Reader API Changes](#event-reader-api-changes)
 * [Internal Changes](#internal-changes)
     - [Server Changes](#server-changes)
     - [Client changes](#client-changes)
     - [Controller Changes](#controller-changes)
        - [Distributing Streams Across Controller Instances](#distributing-streams-across-controller-instances)
        - [Metadata](#metadata)
        - [In-memory State](#in--memory-state)
        - [Writer Marks](#writer-marks)
        - [Periodic Watermark Workflow](#periodic-watermark-workflow)
  * [Compatibility and Migration](compatibility-and-migration)
  * [Future Possibilities](#future-possibilities)
  * [Disadvantages](#disadvantages)
  * [Discarded Approaches](#discard-approaches)
  * [References](#references)

## Introduction
The Goal of watermarking is to provide a time bound from the Writers to the Readers so that they can identify where they are in the Stream. The bound should always be conservative. Meaning that if the Writer’s time is well behaved, the mark should indicate to the Reader that it has received everything before a `timestamp`. (It may well have received more than this.)

This immediately raises the issue of how **time** is defined and what properties does it have. This proposal avoids answering that question by leaving it up to the application. This decision was made for two reasons:

1. Different applications may have different requirements for time.
2. In many applications, meaningful time is necessarily related to the data itself.

## Overview
The general approach is add new APIs to allow the Writer to provide time information. This is aggregated by the Controller and stored in a special Segment associated with the Stream. The information stored is `timestamp`, `streamCut` pairs.

The `timestamp` is derived by taking the minimum of the most recently reported `timestamp` across all Writers. The `streamCut` is a location in the Stream at an event boundary that is at least as far into the Stream as the location of the Writers when the `timestamps` were provided.

The Readers can read these `streamCut`s and `timestamps` from the special Segment, and use the `ReaderGroupState` to coordinate where they are relative to one another.

# API Changes
## Client Factory changes
When Writers are created a new field would need to be specified `writerId`

```
<T> EventStreamWriter<T> createEventWriter(String writerId, String streamName, Serializer<T> s, EventWriterConfig config);
```
the same goes for transnational writers:

```
<T> TransactionalEventStreamWriter<T> createTransactionalEventWriter(String writerId, String streamName, Serializer<T> s, EventWriterConfig config);

```
This would be a breaking API change. Alternatively we could add `writerId` as an optional parameter and if it is not supplied have `noteTime` (below) throw if invoked.

## Event Writer API Changes

In our public interface we add a new API on `EventStreamWriter`.

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
Calling this API is totally optional. Instead a user can use a configuration parameter to automate invoking the method passing wall clock time.

On the Transactional writer we would also need to add support. However because unlike the normal writer it does not continuously emit data, but rather new data is only added at commit time, it makes sense to overload `commit()` by adding an optional `timestamp` parameter:

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
The `timestamp` is passed to the controller as part of the commit. This way a commit simultaneously commits the data and supplies the corresponding `timestamp`.

## Config Changes
The `StreamConfiguration` would have the new parameter:

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
The `EventWriterConfig` would have the new parameter:
```
/**
     * Automatically invoke {@link EventStreamWriter#noteTime(long)} passing
     * {@link System#currentTimeMillis()} on a regular interval.
     */
    private final boolean automaticallyNoteTime;

```
Because it may take some time for all the Writers to come online, for a new Stream we may also want to add an intentional delay before writing the first mark to the marks Segment. This may or may not be configured on a per-stream basis.

## Event Reader API Changes
On the Reader side, we would add a new method on the `EventStreamReader` to obtain the location of the reader in terms of time:

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

# Internal Changes
To support this new API some additional information is needed.

For each Event Stream we add a metadata Segment which contains `timestamps`. (This is referred to below as the ‘marks segment’) It contains records which consist of:

- A `timestamp`
- A set of writer ids
- A `streamCut`

To write this data the Controller would need more information supplied to it by the client and the service. On the Controller interface on the client we would need to add the `writerId` and the `timestamp` to a transaction commit call:

```
/**
      * Commits a transaction, atomically committing all events to the stream, subject to the
      * ordering guarantees specified in {@link EventStreamWriter}. Will fail with
      * {@link TxnFailedException} if the transaction has already been committed or aborted.
      *
      * @param stream Stream name
      * @param writerId The writer that is comiting the transaction.
      * @param timestamp The timestamp the writer provided for the commit (or null if they did not specify one).
      * @param txId Transaction id
      * @return Void or TxnFailedException
      */
     CompletableFuture<Void> commitTransaction(final Stream stream, final String writerId, final Long timestamp, final UUID txId);
```
and we would need to add a brand new call to propagate the information obtained from a call to `noteTimestamp()` on `EventStreamWriter`. That would look like:

```
/**
     * Notifies that the specified writer has noted the provided timestamp when it was at
     * lastWrittenPosition.
     *
     * This is called by writers via {@link EventStreamWriter#noteTime(long)} or
     * {@link Transaction#commit(long)}. The controller should aggrigate this information and write
     * it to the stream's marks segment so that it read by readers who will in turn ultimately
     * surface this information through the {@link EventStreamReader#getCurrentTimeWindow()} API.
     *
     * @param writer The name of the writer. (User defined)
     * @param stream The stream the timestamp is associated with.
     * @param timestamp The new timestamp for the writer on the stream.
     * @param lastWrittenPosition The position the writer was at when it noted the time.
     */
    void noteTimestampFromWriter(String writer, Stream stream, long timestamp, Position lastWrittenPosition);

    /**
     * Notifies the controller that the specified writer is shutting down gracefully and no longer
     * needs to be considered for calculating entries for the marks segment. This may not be called
     * in the event that writer crashes.
     *
     * @param writerId The name of the writer. (User defined)
     * @param stream The stream the writer was on.
     */
    void writerShutdown(String writerId, Stream stream);
```
In order to supply this Position the `EventStreamWriterImpl` would need to internally track the offsets for the segments it was writing to. To do this we would piggyback this information onto the acks it is already receiving from the server. This would involve making a backwards compatible change to the wire protocol to add a `currentSegmentWriteOffset` to `DataAppended`:

```
@Data
public static final class DataAppended implements Reply, WireCommand {
   final WireCommandType type = WireCommandType.DATA_APPENDED;
   final UUID writerId;
   final long eventNumber;
   final long previousEventNumber;
   final long currentSegmentWriteOffset;
//...
```

A similar field would be required for `SegmentMerged`:
```
@Data
public static final class SegmentsMerged implements Reply, WireCommand {
   final WireCommandType type = WireCommandType.SEGMENTS_MERGED;
   final long requestId;
   final String target;
   final String source;
   final long newTargetWriteOffset;
//...
```

This would expand the size of each of these messages by 8 bytes. It would be compatible because the absence of the field can be detected.

## Server Changes
Besides returning the current offsets on merge and ack there are no changes required. In the event that a `SegmentMerged` is lost and the client reconnects to see the segment is already merged, the client can synthesize a valid if conservative offset by calling `getSegmentLength()`.

## Client Changes
Internally the writer needs to collect the offsets returned in the Acks it is receiving from the server. When `noteTime` is invoked it sends the `timestamp` provided along with the latest offsets it has for the Segments it is writing to are sent to the Controller’s `noteTimestampFromWriter` method.

For a transactional writer, it just passes the `timestamp` specified on commit to the Controller.

Each reader needs to read from the mark Segment and compare its position to the `streamCuts`. In the Reader Group state, a new Map needs to be added from Reader to `timestamp`. Every time a Reader advances beyond a `streamCut` (counting as its own any unassigned Segments) it updates its entry in the Reader Group state providing the `timestamp` it just passed. If a Reader loses its last Segment it removes itself from the Map.

When the `getCurrentTimeWindow` method is called on the Reader or the Reader Group state, the minimum time in the Map in the Reader Group state for each Stream is returned. (i.e. The result is a `TimeWindow` where the value is the greatest lower bound of the writer times.)

When `getCheckpoint` or `getStreamCut` is called on the Reader Group, it will have to include the not just the offsets in the normal Segments, but also the minimum offset where the Readers are in the special Segment.

## Controller Changes
The role of the Controller is to take the `timestamps` from all of the Writers on the Stream and write out a `streamCut` that is greater than or equal to the maximum of all writer’s positions and a `timestamp` that is the minimum of all of the Writers.

A call to `noteTimestampFromWriter` or `commitTransaction` updates a structure with the position of the Writer (or where the Transaction committed) and the `timestamp` provided. In the case of commit the Controller will need to obtain the position information itself, where as in the case of `noteTimestampFromWriter` it is explicitly provided by the client.

To do this there needs to be a single Controller instance responsible for aggregating this data for any given Stream. It may be useful for this to be the same instance that is managing Transactions for that stream.

In the event that a Controller instance crashes, the in memory structure will be temporarily lost. It can be recovered by reading the last value written. During recovery there will be a gap where time does not advance.

There are two main objectives we want to achieve while designing this scheme:

1. We do not want any affinity requirements for writers while interacting with Controller instances.
2. We want workload of computing watermarks for different stream to be fairly distributed across available Controller instances.

To achieve above stated goals, we break down the work into two categories:

1. Recording Writer marks
2. Generating consolidated watermarks.

Writers may contact any arbitrary Controller instance and report their `timestamps` and `Positions`. This Controller conditionally persists the reported value in the shared metadata store. For generating watermarks, all available Controller instances divide ownership of Streams amongst themselves such that each Controller instance is exclusively responsible for computing watermarks for only a subset of Streams. So for any Stream there is exactly one Controller instance which is periodically generating marks by consolidating all the individual Writer marks which could be processed by any arbitrary Controller instance.

### Distributing Streams across Controller instances:
As a Stream is created, a reference to it is added to one of the fixed number of "buckets" by computing a hash of its fully qualified name. Each Controller instance contests with others to acquire ownership of each bucket individually. So every Controller instance will be able to acquire ownership for a subset of buckets whereby becoming leader process for all Streams that fall into the bucket. For each bucket that Controller owns, it schedules a periodic background watermarking workflow for each stream within this bucket. It also registers for bucket change notifications and adds (or cancels) this scheduled work as Streams are added (or removed). It is important to note that exactly one Controller instance will be responsible for running background workflow for any given Stream. We will refer to this Controller instance as the "leader" when we talk in context of watermark processing for that Stream.

The watermarking workflow for the Stream will be invoked periodically on the leader Controller instance and in each cycle it will collect the mark as reported by Writers from the shared metadata store. All marks across all available Writers are consolidated into an in-memory data structure and once marks from all known Writers has been received, a new watermark can be emitted for the Stream.

### Metadata
The information stored in the metadata store is a Map of `writerId` to {time, position} tuple. This record can be updated by any Controller instance and updates are conditional and optimistic concurrency protected. The record is updated only if a new update strictly advances time with sanity check that position is also greater than or equal to previously recorded position for the Writer. The second condition is important if multiple clients have the same `writerId` and are reporting different marks. In an unlikely case, this could happen if a Writer got partitioned out and the application's cluster manager started a replacement writer with the same Id.

### In-memory State

Leader Controller will maintain a collection of **known Writers** that ought to be included for computation of next watermark. A subset of this collection of known Writers are then taken into what is called a **generation**. The collection of known Writers is a Map of `writerIds` to their most recent mark ({time, position} tuple). A "generation" is a subset of Writers and their marks that have been selected for next watermark generation. With every new watermark, since our objective is to keep progressing time in a monotonically increasing fashion, we will include only those Writers in the current generation. For this purpose, leader keeps track of last watermark time and only includes writers in current "generation" if their last marks are greater than it.

### Writer Marks
There are two types of Writers (Event Writers and Transaction Writers) each with their own characteristics with respect to time to position tracking. So Event Writers and Transaction Writers will have their corresponding marks generated differently. However, their marks will be reported at the same place in the shared metadata store described above.

- **Event Writer Marks**: Event Writers are expected to supply their positions and corresponding times explicitly to any arbitrary Controller instance. This Controller instance will durably record the received mark in the store after checking the mark for sanity.

- **Transaction Writer Marks**: Transaction writers, on the other hand, are only aware of Transaction ids and the Transaction commits happen asynchronously and are orchestrated by Controller. Since Controller knows when a Transaction is committed, it is best suited to compute the "position" where it was committed.

As Transaction Writers request Controller to commit a Transaction, the protocol between Controller and client for  `commitTransaction` will need to include additional information about `writerId` and `time`. Using this time and the computed position, Controller can generate mark on behalf of the Writer as the Transaction gets committed. However, since Transaction commits happen asynchronously and a Writer could have issued multiple outstanding Transaction commit requests, it becomes imperative to provide some form of ordering guarantee on Controller. Controller will provide strict ordering guarantees for non-overlapping Transactions from the same Writer. A non-overlapping Transactions is a pair of Transactions where second Transaction is created only after commit request for first Transaction is submitted. With this guarantee, a Transaction Writer's "mark" can be tracked on Controller and each subsequent mark will guarantee increasing order of time and position (as long as Writers provide increasing order of time).

Details of how Controller will provide strong ordering guarantees for non overlapping Transactions is not in the scope of this document. The Controller will compute "position" as part of execution of commit workflow for the said Transaction. The Controller is aware of Segments where Transcation's Segments are merged and as part of merge request, it can get merged offset from the Segment Store. Since Segment Store does not persist the merged offsets perpetually, so any failure in retrieving this value from first commit request will make it impossible for Controller to get the "exact" offset at which Transaction was committed. However, since our purposes require us to merely compute a "position" which is an upper bound of merged Transaction's position, Controller can get the tailing offset of the parent Segment if it failed to get the exact offset.

### Periodic Watermark Workflow
The leader Controller maintains an in memory watermarking related state for each Stream and schedules an unconditional loop for execution of watermarking workflow. At the end of each execution cycle, next iteration is scheduled after a fixed periodic delay. During bootstrap, we could choose to have a delayed start to workflow in order to give time for all Writers to be started. At the start of each new cycle, Controller fetches marks from the metadata store for all the Writers. This forms the "known set" of Writers for the next cycle. It then starts a new generation with a time barrier equal to `timestamp` of previous watermark (during bootstrap this will be defaulted to **0**). Only those Writers from the "known" set with marks that have times greater than generation's barrier will be included in the generation. Excluded Writers from the known set are those whose most recent marks are older than the previously emitted watermark's time. Presence of such Writers could mean one of two things:
- Case a: These Writers have not reported any marks in at least last cycle; or
- Case b: These Writers were not present in previous generation and have been recently added, yet they are reporting older `timestamps`. **Case a** may be caused by intermittent failure of a Writer and warrants us to wait until these Writers either recover or timed out and removed from the set. **Case b** is out of our control and including such Writers in watermark set will break the watermark's time order and hence they will be ignored for any computation until they catch up with remainder of the Writers that participated in emission of previous watermarks. Irrespective, Controller cannot emit a watermark until it has either received marks from all such Writers or it declares them "dead". This will depend on a "timeout" mechanism and if, after starting a new generation, if Writers have not been included in it for at least "timeout" period, they will be removed from known Writer set. This algorithm is bounded to run and produce a "watermark" and progress the generation within one "timeout" period.

### Watermark Computation
A watermark is composed of a time **T** and `streamCut` **S**. A watermark effectively indicates that "until `streamCut` **S**, all data corresponding to time **T** has been written across all Writers. To compose such a watermark from the given positions of Writers, we need to take the **lowest** time from marks across Writers and compute a greatest upper bound on all positions from across Writers marks. To compute greatest upper bound on, we will loop over each Writer's mark and loop over each Segment in Writer's position and include it in the set conditionally - a Segment is included in upper bound only if none of its successors already exist in the upper bound. An included Segment will replace all its predecessor Segments in the result set. A Segment included in the upper bound will include the highest offset across all Writer marks that contain that Segment.

It is important to understand that the Writer's position is not same as a `streamCut` because a Writer could be writing concurrently to a predecessor and successor Segment but for different Routing Keys.
This means any composition of Writer positions may not yield a well formed and complete `streamCut`. So the above computed greatest upper bound may have missing key ranges whereby we will need to augment above algorithm to compute a complete `streamCut`.

To illustrate, with an example:
Say we have segments _0_, _1_ with even range splits _[0.0, 0.5]_, _[0.5-1.0]_.

Now lets say we start a scale to Segments _2 [0-0.6], 3 [0.6-1.0]_.
We could have a writer that has a position which is comprised of Segments _1_ and _2_ `-> (1/off1, 2/off2)`. This is possible in either of the following cases:

- While this scale is ongoing, we can have a situation such that Segment _2_, _3_ are created and _0_ is sealed but _1_ may still be open.

- Alternatively, Writer may not have attempted to write any data in the range covered by _1_ and hence is never replaced while _0_ is replaced by _2_.

Computing greatest upper bound on such a position will yield `(2/off2)`. This is clearly not a well formed `streamCut` as it has missing ranges. So we need to augment the algorithm to fill up missing ranges with Segments such that entire range is covered without breaking the greatest upper bound criteria. We can use the epoch of the highest Segment in the upper bound and include its sibling Segments from this epoch such that cover the missing range. All such included Segments can be assigned `offset 0` without breaking any guarantees. Inclusion of any such sibling Segment may also result in exclusion of predecessor Segments from upper bound and we should run this loop until there are no missing ranges. Worst case, we will end up with upper bound comprising of all Segments from this epoch. However, this will still be an upper bound on all positions of all Writers of the Stream. We can have a Segment _x_, which replaced Segment _y_, that may only cover its partial range. Which means in _x's_ epoch there was some set of segment _z1, z2, ..., zk_ that covered the remainder range covered by _y_. It is possible that union of _z1, z2, ..., zk_ covered more than the range covered by _y_ and may end up replacing other Segments from the upper bound set. We can include all of _z1, z2, ..., zk_ without breaking the upper bound promise. And then repeat this until all missing ranges are accounted for.

```
class Mark {
    long timestamp;
	Map<Segment, Long> position; // segment to offset map
}

class Generations {
	long previousWatermarkTime;
	Map<String, Mark> writerMarks;

	Pair<StreamCut, Long> generateWaterMark() {
		upperBound = computeGreatestUpperBound();
		StreamCut streamCut = computeStreamCut(upperbound);
		int lowerBoundOnTime = writerMarks.values().stream().map(x -> x.timestamp).min();
		return new Pair(streamCut, lowerBoundOnTime);
	}

	Map<Segment, Long> computeGreatestUpperBound() {
		Map<Segment, Long> upperBound;
		for (Position pos : writerPositions) {
			addToUpperBound(pos.segmentOffsetMap, upperBound);
		} 		
	}

	void addToUpperBound(Map<Segment, Long> segmentOffsetMap, Map<Segment, Long> upperBound) {
		for (Entry<Segment, Long> entry : segmentOffsetMap) {
		    Segment s = entry.key;
			Long offset = entry.value;
		    if (upperBound.containsKey(s)) { // update offset if the segment is already present.
			    long newOffset = Math.max(p.getOffsetFor(s), upperBound.get(s));
				upperBound.put(s, newOffset);
			} else if (!hasSuccessors(s, upperBound.keys)) { // only include segment if it doesnt have a successor already included in the set.
    			list<Segment> toReplace = findPredecessors(s, upperBound.keys()); // replace any predecessor segments from the result set.
		    	upperBound.removeAllKeys(toReplace);
				currentOffsets.put(s, offset);
			}
		}
	}

	StreamCut computeStreamCut(Map<Segment, Long> upperbound) {
		Pair<Double, Double> missingRange = findMissingRange(upperBound);
		Map<Segment, Long> streamCut = upperbound;
		if (missingRange != null) {
			int highestEpoch = upperbound.segments.stream().map(segment -> segment.epoch).max();
			EpochRecord highestEpochRecord = metadataStore.getEpoch(highestEpoch);

			do {
				List<Segment> replacement = findSegmentsForMissingRange(highestEpochRecord, missingRange);
				Map<Segment, Long> replacementSegmentOffsetMap = replacement.stream().collect(Collectors.toMap(x -> x, x -> 0);
				addToUpperBound(replacementSegmentOffsetMap, upperBound);
				missingRange = findMissingRange(streamCut);
			} while (missingRange != null);
		}

		return new StreamCut(streamCut);
	}

}

```
# Compatibility and Migration
Use of the API for marking is optional. In the event that the Writer does not provide us with any information, nothing will be written to the mark Segment and the Reader will not be able to receive `timestamps`.

The only backward incompatible change it the addition of the `writerId` to the call to create a new Writer.

# Future Possibilities
One natural extension to this idea is to allow the user to request a `streamCut` for given time. We could implement this if we had an index of the marks Segment. This could be done by maintaining an ‘Index Segment’ which contained records that consist of:

- An offset (a pointer to the mark segment)
- A `timestamp`, Which are a fixed 16 bytes

Because the Index Segment has fixed size records we could easily perform a binary search on it to obtain the closest `streamCut` in the marks Segment to any given `timestamp`.

Another possible improvement is to use the time information to help with reader re-balancing. If a Stream is marked, when Readers re-balance Segments instead of re-balancing by summing the number of bytes remaining, they can instead use their time location in the Stream to compare who is ahead or behind.

# Disadvantages

## Ties time/progress to a long
Because it becomes part of our API, time is necessarily defined in terms of a long. For some applications this may not be desirable as it does not represent the application's notion of progress.

## Applications reading from multiple streams
This proposal focuses on a single Stream, there are applications that process multiple Streams. Enrichment joins are a good example for the case in which the time is aligned for the multiple Streams.

For the case in which time is not aligned, it does not make sense to talk about watermarking. The contract of watermarks is that once the operator receives a watermark for time _T_, all events with   `timestamp` _T_ or earlier have been received. With Streams with data corresponding to time epochs that might not even overlap, such a contract is meaningless.

## Time window bounds may not be tight

The upper bound provided for time may be quite far behind if one Writer had not noted the time for a while when the data was being written. Similarly if a connection is dropped after a Segment is merged the offsets for that Segment may be behind.

The lower bound is also slack in the event that clocks are not well synchronized. Similarly if committing Transactions has a very high latency, it will add to the uncertainty in the lower bound.

# Discarded Approaches

## Keeping a time index on a per segment basis
This could provide a very similar API to the one proposed, and eliminate much of the work done on the Controller. However it requires significant changes on the server side and does not work with Transactional writers.

## Only supporting ingestion time
Ingestion time has a lot of nice properties, like you don’t actually need to know or hear from all the Writers to advance time. However it just doesn’t work for all use cases. Particularly historical data could be a problem.

## Using extended attributes instead of a index segment
If the Segment Store could provide API like `findNextAttributeFollowing(Key)` and could scale to very large numbers of attributes, this could be an alternative to the Index Segment. This requires a lot of work server side and would be less compact in terms of storage. The advantage it would have is that creating a new Reader Group based on a `timestamp` would be faster. However this is a rarely performed operation that is not performance critical.

# References
Watermarking in other systems:
- [Millwheel](https://ai.google/research/pubs/pub41378.pdf)
- [Dataflow](https://ai.google/research/pubs/pub43864)
- [Gigascope](www.vldb.org/conf/2005/papers/p1079-johnson.pdf)
