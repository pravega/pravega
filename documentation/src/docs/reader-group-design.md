<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Reader Groups Design

_To be updated to reflect recent implementation details (05/07/2017)_

## Motivation 
A set of Readers can be grouped together in order that the set of Events in a Stream can be read in parallel.  The grouping of Readers is called a Readers Group.  Pravega guarantees that each Event in the Stream is read by exactly one Reader in the Readers Group. 

Each Reader in a ReaderGroup is assigned zero or more Segments.
The Reader assigned to a Segment is the only Reader within the ReaderGroup that reads Events from that Segment.  This is the fundamental mechanism by which Pravega makes ordering guarantees of Event delivery to a Reader – a Reader will receive Events in the order they were published into a Segment.
There are several challenges associated with this mechanism:

- How to maintain the mapping of which Reader within a ReaderGroup is assigned which Segment
- How to manage the above mapping when Segments split and merge
- How to manage the above mapping when Readers are added to the ReaderGroup 
- How to manage the above mapping when Readers leave the ReaderGroup either by an explicit operation or the Reader becoming unavailable due to network outage or the Reader process failing

To solve these problems we can use [StateSynchronizer](state-synchronizer-design.md) to enable readers to coordinate among themselves.

### How Consistent replicated state can be used to solve the problem
A consistent replicated state object representing the ReaderGroup metadata will be created in each reader.  This ReaderGroup metadata consists of:

- a map of online readers to the segments they own
- a list of positions in segments that can be taken over.

Every time the Readers in a ReaderGroup change, the state can be updated. Similarly each time one of the readers is going to start reading from a new segment, it can update the replicated state. 
This allows all readers to know about all the other Readers in their ReaderGroup and which segments they own.

Given this information:

- A new reader can infer which segments are available to read from. (By virtue of it being absent from the state)
- Dealing with a segment being merged becomes easy, because the last reader to reach the end of its pre-merge segment knows it can freely take ownership of the new segment.
- Readers can see their relative load and how they are progressing relative to the other readers in their group and can decide to transfer segments if things are out of balance.
- This allows readers to take action directly to ensure all the events are read without the need for some external tracker.

## ReaderGroup APIs
The external APIs to manage ReaderGroups could be added to the StreamManager object. They consist of:
```
    ReaderGroup createReaderGroup(String name, Stream stream, ReaderGroupConfig config);
    ReaderGroup getReaderGroup(String name, Stream stream);
    void deleteReaderGroup(ReaderGroup group);
```
When a ReaderGroup is created, it creates a [StateSynchronizer](state-synchronizer-design.md) shared by the readers. To join a ReaderGroup readers would just specify it in their configuration:
```
    ReaderConfig cc = new ReaderConfig(props);
    Reader<T> reader = a_stream.createReader("my_reader_id", "my_reader_group", cc);
```
When readers join the group they use the state to determine which segments to read from. When they shut down they update the state so that other readers can take over their segments.

# Failure detector
We still need some sort of heartbeating mechanism to tell if Readers are alive. The problem is greatly simplified because it need not produce a view of the cluster or manage any state. The component would just need to detect failures and invoke the <code>void readerOffline(String readerId, Position lastPosition);</code> api on the ReaderGroup

For consistency, the Failure detector should not declare a host as dead that is still processing events. Doing so could violate exactly once processing guarantees.

# Examples
## New reader
1. When a reader joins a group its online status is added to the shared state
1. Other readers receive updates to the shared state.
1. When a reader with more than average number of segments sees the new reader, it may give up a segment by writing its position for that segment to the shared state.
1. The new reader can take over a segment by recording that it is doing so in the shared state.
1. The new reader can start reading from the position it read from the shared state for the segment it picked up.

There are no races between multiple readers coming online concurrently because only one of them can successfully claim ownership of any given segment.

## Segments merging
1. When a reader comes to the end of its segment it records this information in the shared state.
1. When all of the segments that are getting merged together are completed a reader may claim ownership of the following segment.

There is no ambiguity as to who the owner is, because it is stored in the shared state. There is no risk of a segment being forgotten because any reader can see which segments are available by looking at the shared state and claim them.

## Reader going offline.
1. When a reader dies the readerOffline() method will be invoked either by the reader itself in a graceful shutdown (internally to the close method) or via a liveness detector. In either case the reader's last position is passed in.
1. The last position is written to the state.
1. Other readers will see this when they update their local state.
1. Any of them can decide to take over one or more of the segments owned by the old reader from where it left off by recording their intention to do so in the state object.
1. Once the state has been updated the new reader is considered the owner of the segment and can read from it at will.

## What happens if a reader does not keep up to date
A Reader with out-of-date state can read from their existing segments without interference. The only disadvantage to this is that they will not shed load to another reader should one become available. However because they have to write to the shared state to start reading from any segment they don't already own, they must fetch up-to-date information before moving on to a new segment. 

## Impact of availability and latency
Reading and updating the state object can occur in parallel to reading so there would likely be no visible latency impact. 
A stream would be unavailable for reading if Pravega failed in such a way that the segment containing the ReaderGroup information went down and remained offline for long enough for the readers to exhaust all the events in their existing segments. Of course if Pravega failed in this way, odds are at least some portion of the stream would also be directly impacted and not be able to read any events. This sort of failure mode would manifest as latency for the reader, similar to what would happen if they had reached the tail of the stream. 

This is preferable to using an external system to manage this coordination, as that would require adding new components that can fail in different ways, as opposed to further relying on a small set that we need to make highly available anyway. This is particularly notable in the case of a network partition. If the network is split any readers that are on the same side of the partition as the Pravega servers can continue working. If we were to utilize an external service, that service could be cut off and readers might not be able to make progress even if they could talk to Pravega. 
