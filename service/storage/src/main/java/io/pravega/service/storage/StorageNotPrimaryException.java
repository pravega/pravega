/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.service.storage;

import io.pravega.service.contracts.StreamSegmentException;

/**
 * Indicates that a particular Storage Instance is no longer the Primary Writer for a Segment.
 */
public class StorageNotPrimaryException extends StreamSegmentException {
    /**
     * Creates a new instance of the StorageNotPrimaryException class.
     *
     * @param streamSegmentName The name of the segment for which the Storage is no longer primary.
     */
    public StorageNotPrimaryException(String streamSegmentName) {
        this(streamSegmentName, null);
    }

    public StorageNotPrimaryException(String streamSegmentName, String message) {
        super(streamSegmentName, "The current instance is no longer the primary writer for this StreamSegment." + (message == null ? "" : " ") + message);
    }
}
