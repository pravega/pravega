<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# StateSynchronizer Design

A StateSynchronizer provides a means for data to be written and read by multiple processes, while consistency is guaranteed using optimistic checks. A rough version of this API is [checked in here.](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/state/StateSynchronizer.java)

This works by having each process keep a copy of the data. All updates are written through the StateSynchronizer which appends them to a Pravega segment. They can keep up to date of changes to the data by consuming from the segment. To provide consistency a conditional append is used. This ensures that the updates can only proceed if the process performing them has the most recent data. Finally to prevent the segment from growing without bound, we use have a compact operation that re-writes the latest data, and truncates the old data so it can be dropped.

This model works well when most updates are small in comparison to the total data size being stored, as they can be written a small deltas. As with any optimistic concurrency system it would work worst when many processes are all contending to try to update the same information at the same time.

# Example
A concrete example synchroning the contents of a set is [checked in here](https://github.com/pravega/pravega/blob/master/client/src/test/java/io/pravega/client/state/examples/SetSynchronizer.java). We also have an example that is synchronizing [membership of a set of hosts](https://github.com/pravega/pravega/blob/master/client/src/test/java/io/pravega/client/state/examples/MembershipSynchronizer.java).

Imagine you want many processes to share a Map. This can be done by creating by a StateSynchronizer, which will help coordinate changes to the map. Each client has their own copy of the map in memory and can apply updates by passing a generator to the StateSynchronizer. Every time an update is attempted the updated is recorded to a segment. Updates will fail unless the Map passed into the update method is consistent with all of the updates that have been recorded to the segment. If this occurs the generator is called with the latest state to try again. Thus the order of updates is defined by the order in which they are written to the segment.

# How this is implemented
For this to work we use two features of the Pravega Segment Store Service.
## Conditional append
The append method can specify what offset the append is expected to be at. If the append will not go in, nothing should be done and a failure should be returned to the client.

## Truncate segment
Truncate segment deletes all data before a given offset. (This operation does not affect the existing offsets)
Any reads for offsets lower than this value will fail. Any data stored below this offset can be removed.
