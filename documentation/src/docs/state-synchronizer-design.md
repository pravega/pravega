<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# State Synchronizer Design

A State Synchronizer provides a means for data to be written and read by multiple processes, while consistency is guaranteed using optimistic checks. For more information, check the [API](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/state/StateSynchronizer.java)

This works by having each process keep a copy of the data. All the updates are written through the State Synchronizer which appends them to the Pravega Stream Segment. Latest updates can be incorporated to the data by consuming from the Stream Segment. To provide consistency a conditional append is used. This ensures that the updates can only proceed if the process performing them has the most recent data. To avoid the unbounded data in the Stream Segment, a compact operation is involved which re-writes the latest data and truncates the old data.

This model works well when most of the updates are small in comparison to the total data size being stored, as they can be written as small deltas. As with any optimistic concurrency system it would work worst when many processes contend and try to update the same information at the same time.

# Example

A concrete [example](https://github.com/pravega/pravega/blob/master/client/src/test/java/io/pravega/client/state/examples/SetSynchronizer.java) of synchronizing the contents of a set is provided. We also have an example that is synchronizing [membership of a set of hosts](https://github.com/pravega/pravega/blob/master/client/src/test/java/io/pravega/client/state/examples/MembershipSynchronizer.java).

Imagine you want many processes to share a Map. This can be done by creating the State Synchronizer, it will aid in coordinating the changes to the Map. Each client has their own copy of the Map in memory and can apply updates by passing a generator to the State Synchronizer. Every time an update is attempted the updates are recorded to the Stream Segment. Updates are successful when the Map passed into the update method is consistent with all of the updates that have been recorded to the Stream Segment. If this occurs the generator is called with the latest state to try again. Thus the order of updates is defined by the order in which they are written to the Stream Segment.

# Implementation

For the implementation, two features of the Pravega Segment Store Service is used.

## Conditional Append

The append method can specify what offset the append is expected to be at. If the append will not go in, a fail message should be returned to the client.

## Truncate Segment
Truncate Segment deletes all data before a given offset. (This operation does not affect the existing offsets)
Any reads for the offsets lower than this value will fail. Any data stored below this offset can be removed.
