/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.contracts;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Defines various types of Read Result Entries, based on where their data is located.
 */
@RequiredArgsConstructor
public enum ReadResultEntryType {
    /**
     * The ReadResultEntry points to a location in the Cache, and data is readily available.
     */
    Cache(false),

    /**
     * The ReadResultEntry points to a location in Storage, and data will need to be retrieved from there in order
     * to make use of it.
     */
    Storage(false),

    /**
     * The ReadResultEntry points to a location beyond the end offset of the StreamSegment. It will not be able to return
     * anything until such data is appended to the StreamSegment.
     */
    Future(false),

    /**
     * The ReadResultEntry indicates that the End of the StreamSegment has been reached. No data can be consumed
     * from it and no further reading can be done on this StreamSegment from its position.
     */
    EndOfStreamSegment(true),

    /**
     * The ReadResultEntry points to a location in the StreamSegment that has been truncated out and is no longer available.
     */
    Truncated(true);

    /**
     * If true, this indicates that the ReadResult cannot progress from this point anymore and should be closed.
     */
    @Getter
    private final boolean terminal;
}
