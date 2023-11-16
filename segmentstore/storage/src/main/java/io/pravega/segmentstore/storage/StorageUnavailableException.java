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
package io.pravega.segmentstore.storage;

import io.pravega.segmentstore.contracts.StreamSegmentException;

/**
 * Indicates that a particular Storage Instance is unavailable.
 */
public class StorageUnavailableException extends StreamSegmentException {
    private static final long serialVersionUID = 1L;
    /**
     * Creates a new instance of the StorageUnstableException class.
     *
     * @param streamSegmentName The name of the segment for which the Storage is unavailable.
     */
    public StorageUnavailableException(String streamSegmentName) {
        this(streamSegmentName, null, null);
    }

    /**
     * Creates a new instance of the StorageUnstableException class.
     *
     * @param streamSegmentName The name of the segment for which operation was called.
     * @param cause             The causing exception.
     */
    public StorageUnavailableException(String streamSegmentName, Throwable cause) {
        this(streamSegmentName, null, cause);
    }

    /**
     * Creates a new instance of the StorageUnstableException class.
     *
     * @param streamSegmentName The name of the segment for which operation was called.
     * @param message           Message.
     * @param cause             The causing exception.
     */
    public StorageUnavailableException(String streamSegmentName, String message, Throwable cause) {
        super(streamSegmentName, "The current storage instance is unavailable." + (message == null ? "" : " " + message),
                cause);
    }
}
