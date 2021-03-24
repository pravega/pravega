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
package io.pravega.segmentstore.storage.cache;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents the state of a {@link CacheStorage} instance at a point in time.
 */
@RequiredArgsConstructor
@Getter
public class CacheState {
    /**
     * The total number of bytes inserted ({@link CacheStorage#insert} or appended ({@link CacheStorage#append}).
     */
    private final long storedBytes;
    /**
     * The total number of bytes that are occupied by the Cache Blocks holding stored bytes. These are bytes that are
     * allocated, but may not be used for any other purpose. {@link #getUsedBytes()} will always be greater than or equal
     * to {@link #getStoredBytes()}.
     */
    private final long usedBytes;
    /**
     * The total number of bytes used for metadata purposes. These are bytes that the {@link CacheStorage} uses to maintain
     * its data structures and are not directly accessible via public APIs.
     */
    private final long reservedBytes;
    /**
     * The total number of bytes allocated. This includes {@link #getUsedBytes()}, {@link #getReservedBytes()} and "free"
     * bytes (which have not yet been used for anything or that have been deleted).
     */
    private final long allocatedBytes;
    /**
     * The maximum size, in bytes, that can be allocated in this {@link CacheStorage}.
     */
    private final long maxBytes;

    @Override
    public String toString() {
        return String.format("Stored = %d, Used = %d, Reserved = %d, Allocated = %d, Max = %d",
                this.storedBytes, this.usedBytes, this.reservedBytes, this.allocatedBytes, this.maxBytes);
    }
}