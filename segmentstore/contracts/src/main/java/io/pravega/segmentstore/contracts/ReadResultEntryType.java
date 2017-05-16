/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

/**
 * Defines various types of Read Result Entries, based on where their data is located.
 */
public enum ReadResultEntryType {
    /**
     * The ReadResultEntry points to a location in the Cache, and data is readily available.
     */
    Cache,

    /**
     * The ReadResultEntry points to a location in Storage, and data will need to be retrieved from there in order
     * to make use of it.
     */
    Storage,

    /**
     * The ReadResultEntry points to a location beyond the end offset of the StreamSegment. It will not be able to return
     * anything until such data is appended to the StreamSegment.
     */
    Future,

    /**
     * The ReadResultEntry indicates that the End of the StreamSegment has been reached. No data can be consumed
     * from it and no further reading can be done on this StreamSegment from its position.
     */
    EndOfStreamSegment
}
