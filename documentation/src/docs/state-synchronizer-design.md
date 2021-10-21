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
# State Synchronizer Design

In a State Synchronizer data can be written and read by multiple processes, and the consistency is guaranteed using optimistic checks. State Synchronizer provides the abstraction of a user defined `Java Object` which is kept in-sync consistently across the multiple machines. All the hosts would see the same object even as it is modified.

The [State Synchronizer API](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/state/StateSynchronizer.java) can be used to perform updates to the state `Object`. This can be used to implement replicated state machines, schema distribution, and leader election.

State Synchronizer works by storing a consistent history of updates to the state `Object`. The updates are stored in a Pravega Stream. Pravega ensures that every process that is performing an update on the latest version of that `Object`. Thus the `Object` is coordinated across a fleet and everyone sees the same sequence of updates on the same `Object`.

The idea is to use a Stream to persist a sequence of changes for a shared state. And allow various applications to use the [Pravega Java Client Library](http://pravega.io/docs/latest/state-synchronizer/#shared-state-and-pravega) to concurrently read and write the shared state in a consistent fashion.

This works by having each process keep a copy of the data. All the updates are written through the State Synchronizer which appends them to the Pravega Stream Segment. Latest updates can be incorporated to the data by consuming from the Stream Segment. To provide consistency a conditional append is used. This ensures that the updates can only proceed if the process performing them has the most recent data. To avoid the unbounded data in the Stream Segment, a compact operation is involved which re-writes the latest data and truncates the old data.

In Pravega Stream, a Segment is always owned by a single server. This allows it to provide atomic compare-and-set operation on the Segments. This primitive is used to build a higher level abstraction at the application layer while maintaining strong consistency.

This model works well when most of the updates are small in comparison to the total data size being stored, as they can be written as small deltas. As with any optimistic concurrency system it would work worst when many processes contend and try to update the same information at the same time.

# Example

A concrete [example](https://github.com/pravega/pravega/blob/master/client/src/test/java/io/pravega/client/state/examples/SetSynchronizer.java) of synchronizing the contents of a Set is provided. We also have an example that is synchronizing [membership of a set of hosts](https://github.com/pravega/pravega/blob/master/client/src/test/java/io/pravega/client/state/examples/MembershipSynchronizer.java).

Imagine you want many processes to share a Map. This can be done by creating the State Synchronizer, it will aid in coordinating the changes to the Map. Each client has its own copy of the Map in memory and can apply updates by passing a generator to the State Synchronizer. Every time an update is made, the update is recorded to the Stream Segment. Updates are successful when the Map passed into the update method is consistent with all of the updates that have been recorded to the Stream Segment. If this occurs the generator is called with the latest state to try again. Thus the order of updates is defined by the order in which they are written to the Stream Segment.

# Implementation

For the implementation, two features of the Pravega Segment Store Service are used.

## Conditional Append

The conditional append call in the Pravega Segment Store is the cornerstone for the implementation of the State Synchronizer semantics. That is, when a client updates a piece of data via State Synchronizer, a conditional append is internally used against the Segment Store. In a conditional append, the client specifies the `Offset` in which the append is expected to be located. If the `Offset` provided by the client does match the actual `Offset` of the append in the Stream Segment, the operation is aborted and an error is returned to the client. This mechanism is used in the State Synchronizer to provide optimistic locks on data updates.

## Truncate Segment
Truncate Segment deletes all data before a given `Offset`. This operation does not affect the existing `Offset`s. Any reads for the `Offset`s lower than this value will fail. Any data stored below this `Offset` can be removed. Truncation is performed following compaction, so that the Segment does not need to hold onto old data.
