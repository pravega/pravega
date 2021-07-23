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
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;

/**
 * Represents a ReadIndexEntry that points to an entry in the Cache.
 */
public class CacheIndexEntry extends ReadIndexEntry {
    @Getter
    private final int cacheAddress;
    @GuardedBy("this")
    private int length;

    /**
     * Creates a new instance of the ReadIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @param length              The Length of this entry.
     * @param cacheAddress        The address of this Index Entry in the CacheStorage.
     * @throws IllegalArgumentException if the offset is a negative number.
     * @throws IllegalArgumentException if the length is a negative number.
     */
    CacheIndexEntry(long streamSegmentOffset, int length, int cacheAddress) {
        super(streamSegmentOffset);
        Preconditions.checkArgument(length >= 0, "length", "length must be a non-negative number.");
        this.length = length;
        this.cacheAddress = cacheAddress;
    }

    @Override
    synchronized long getLength() {
        return this.length;
    }

    /**
     * Increases the length by the given amount.
     *
     * @param delta The amount to increase by.
     */
    synchronized void increaseLength(int delta) {
        Preconditions.checkArgument(delta >= 0, "delta must be a non-negative number.");
        this.length += delta;
    }

    @Override
    boolean isDataEntry() {
        return true;
    }

    @Override
    public synchronized String toString() {
        return String.format("%s, Address = %d", super.toString(), this.cacheAddress);
    }
}
