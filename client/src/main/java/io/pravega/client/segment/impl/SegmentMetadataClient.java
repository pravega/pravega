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
package io.pravega.client.segment.impl;

/**
 * A client for looking at and editing the metadata related to a specific segment.
 */
public interface SegmentMetadataClient extends AutoCloseable {
    
    /**
     * Returns info for the current segment.
     *
     * @return Metadata about the segment.
     */
    abstract SegmentInfo getSegmentInfo();
    
    /**
     * Returns the length of the current segment. i.e. the total length of all data written to the segment.
     *
     * @return The length of the current segment.
     */
    abstract long fetchCurrentSegmentLength();

    /**
     * Gets the current value of the provided attribute.
     * @param attribute The attribute to get the value of.
     * @return The value of the attribute or {@link SegmentAttribute#NULL_VALUE} if it is not set.
     */
    abstract long fetchProperty(SegmentAttribute attribute);

    /**
     * Atomically replaces the value of attribute with newValue if it is expectedValue.
     * 
     * @param attribute The attribute to set
     * @param expectedValue The value the attribute is expected to be
     * @param newValue The new value for the attribute
     * @return If the replacement occurred. (False if the attribute was not expectedValue)
     */
    abstract boolean compareAndSetAttribute(SegmentAttribute attribute, long expectedValue, long newValue);
    
    /**
     * Deletes all data before the offset of the provided segment.
     * This data will no longer be readable. Existing offsets are not affected by this operations. 
     * The new startingOffset will be reflected in {@link SegmentMetadataClient#getSegmentInfo()}.{@link SegmentInfo#getStartingOffset()}.
     * @param offset The offset the segment should be truncated at.
     */
    abstract void truncateSegment(long offset);
    
    /**
     * Seals the segment so that no more writes can go to it.
     */
    abstract void sealSegment();
    
    @Override
    abstract void close();
    
}
