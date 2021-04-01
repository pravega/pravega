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
# Reader Groups Design

## Motivation
A set of Readers can be grouped together in order that the set of Events in a Stream can be read in parallel. This grouping of Readers is called a Reader Group. Pravega guarantees that each Event in the Stream is read by exactly one Reader in the Reader Group.

Each Reader in a Reader Group is assigned zero or more Stream Segments.
The Reader assigned to a Stream Segment is the only Reader within the Reader Group that reads Events from that Stream Segment. This is the fundamental mechanism by which Pravega makes ordering guarantees of Event delivery to a Reader – a Reader will receive Events in the order they were written into a Stream Segment.
There are several challenges associated with this mechanism:

 -  How to maintain the mapping of which Reader within a Reader Group is assigned which Stream Segment?
 -  How to manage the above mapping when Stream Segments split and merge?
 -  How to manage the above mapping when Readers are added to the Reader Group?
 -  How to manage the above mapping when Readers leave the Reader Group either by an explicit operation or the Reader becoming unavailable due to network outage or the Reader process failing?

To address these challenges, we use [State Synchronizer](state-synchronizer-design.md) to enable coordination among Readers.

### Consistent Replicated State
A consistent replicated state object representing the Reader Group metadata will be created in each Reader. This Reader Group metadata consists of:

 - A map of online Readers to the Stream Segments they own.
 - A list of positions in Stream Segments that can be taken over.

Every time the Readers in a Reader Group change, the state can be updated. The replicated state is updated every time a Reader starts reading a new Stream Segment. Thus every Reader is aware of the Stream Segments owned by all the Readers in the their Reader Group.

Given this information:

 - A new Reader can infer which Stream Segments are available to read from. (By virtue of it being absent from the state).
 - Dealing with a Stream Segment being merged becomes easy, because the last reader to reach the end of its pre-merge Stream Segment knows it can freely take ownership of the new Stream Segment.
 - Readers can see their relative load and how they are progressing relative to the other Readers in their group and can decide to transfer Stream Segments if things are out of balance.
 - This allows Readers to take action directly to ensure all the events are read without the need for some external tracker.

## Reader Group APIs

The external APIs to manage Reader Groups could be added to the `ReaderGroupManager` object. It consist of:

```java
ReaderGroup createReaderGroup(String name, Stream stream, ReaderGroupConfig config);
ReaderGroup getReaderGroup(String name, Stream stream);
void deleteReaderGroup(ReaderGroup group);
```
When a Reader Group is created, it creates a [State Synchronizer](state-synchronizer-design.md) shared by the Readers. To join a Reader Group, Readers would just specify it in their configuration:

```java
ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
EventStreamReader< T > reader = clientFactory.createReader(readerId, READER_GROUP_NAME, serializer, readerConfig);

```
The Readers, while joining the group, access the information stored on the state to determine which Stream Segments to read from. Once when they shut down, they update the state so that other Readers can take over their Stream Segments.

# Detecting Pravega Reader Failure

We still need some effective mechanism to identify, whether Readers are alive or not. The problem is greatly simplified because it need not produce a view of the cluster or manage any state. The component would just need to detect a Reader failure and invoke the `void ReaderOffline(String ReaderId, Position lastPosition);` API on the Reader Group.

For consistency, the "Failure detector" should not declare a host as dead that is still processing events. Doing so could violate exactly once processing guarantees.

## New Reader Added
1. When a Reader joins a group its online status is added to the shared state.
1. Other Readers receive updates to the shared state.
1. When a Reader with more than average number of Stream Segments sees the new Reader, it may give up a Stream Segment by writing its position for that Stream Segment to the shared state.
1. The new Reader can take over a Stream Segment by recording that it is doing so in the shared state.
1. The new Reader can start reading from the position it read from the shared state for the Stream Segment it picked up.

There are no races between multiple Readers coming online concurrently because only one of them can successfully claim ownership of any given Stream Segment.

## Stream Segments Get Merged
1. When a Reader comes to the end of its Stream Segment it records this information in the shared state.
1. When all of the Stream Segments that are getting merged together are completed, a Reader may claim ownership of the following Stream Segment.

There is no ambiguity as to who the owner is, because it is stored in the shared state. There is no risk of a Stream Segment being ignored because every Reader can see the available Stream Segments by looking at the shared state and claim them.

## Reader Offline
1. When a Reader dies, the `void ReaderOffline(String ReaderId, Position lastPosition);` API method will be invoked either by the Reader itself in a graceful shutdown (internally to the close method) or via a "liveness detector". In either case the Reader's last position is written to the state.

1. If a null `Position` is sent then the last checkpointed position will be written to the state.
1. This is used by the newer Readers when they take ownership of the Stream Segment(s) that were read by the older/offline reader.
1. Any Reader can decide to take over one or more of the Stream Segments owned by the old Reader from where it left off by recording their intention to do so in the state object.
1. Once the state has been updated by the new Reader, it is considered the owner of the Stream Segment and can read from it.


# Other Considerations on Reader Groups

## What happens if a Reader does not keep up to date?
A Reader with out-of-date state can read from their existing Stream Segments without interference. The only disadvantage to this is that they will not shed load to another Reader should one become available. However, because they have to write to the shared state to start reading from any Stream Segment which they don't already own, they must fetch up-to-date information before moving on to a new Stream Segment.

## Impact of availability and latency
Reading and updating the state object can occur in parallel to reading, so there would likely be no visible latency impact.
A stream would be unavailable for reading if Pravega failed in such a way that the Stream Segment containing the Reader Group information went down and remained offline for long enough for the Readers to exhaust all the events in their existing Stream Segments. Of course, if Pravega failed in this way, odds are at least some portion of the stream would also be directly impacted and not be able to read any events. This sort of failure mode would manifest as latency for the Reader, similar to what would happen if they had reached the tail of the stream.

This is preferable to using an external system to manage this coordination, as that would require adding new components that can fail in different ways, as opposed to further relying on a small set that we need to make highly available anyway. This is particularly notable in the case of a network partition. If the network is split any Readers that are on the same side of the partition as the Pravega servers can continue working. If we were to utilize an external service, that service could be cut off and Readers might not be able to make progress even if they could talk to Pravega.
