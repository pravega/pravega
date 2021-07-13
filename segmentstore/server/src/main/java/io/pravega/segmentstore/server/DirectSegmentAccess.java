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
package io.pravega.segmentstore.server;

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.ReadResult;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an API that can be used to get direct access to a Segment. This can be used instead of the SegmentContainer API
 * for short periods of time if a rapid sequence of operations is desired (since it caches the locations of the Segment and
 * it does not need to call the usual SegmentContainer and StreamSegment lookups on every invocation).
 */
public interface DirectSegmentAccess {
    /**
     * Gets the internal Id of the Segment that this API provides direct access to.
     *
     * @return The internal ID of the Segment.
     */
    long getSegmentId();

    /**
     * Appends a range of bytes at the end of the Segment and atomically updates the given attributes. The byte range
     * will be appended as a contiguous block, however there is no guarantee of ordering between different calls to this
     * method.
     * @see io.pravega.segmentstore.contracts.StreamSegmentStore#append(String, BufferView, AttributeUpdateCollection, Duration)
     *
     * @param data             The data to add.
     * @param attributeUpdates A Collection of Attribute-Values to set or update. May be null (which indicates no updates).
     *                         See Notes about AttributeUpdates in the interface Javadoc.
     * @param timeout          Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the offset at which the data were added. If the
     * operation failed, the future will be failed with the causing exception.
     * @throws NullPointerException     If any of the arguments are null, except attributeUpdates.
     * @throws IllegalArgumentException If the Segment Name is invalid (NOTE: this doesn't
     *                                  check if the Segment does not exist - that exception will be set in the
     *                                  returned CompletableFuture).
     */
    CompletableFuture<Long> append(BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout);

    /**
     * Appends a range of bytes at the end of the Segment and atomically updates the given attributes. The byte range
     * will be appended as a contiguous block, however there is no guarantee of ordering between different calls to this
     * method.
     * @see io.pravega.segmentstore.contracts.StreamSegmentStore#append(String, BufferView, AttributeUpdateCollection, Duration)
     *
     * @param data             The data to add.
     * @param attributeUpdates A Collection of Attribute-Values to set or update. May be null (which indicates no updates).
     *                         See Notes about AttributeUpdates in the interface Javadoc.
     * @param offset           The expected length of the segment. If the current length of the Segment does not equal this
     *                         value, the operation will fail with a {@link io.pravega.segmentstore.contracts.BadOffsetException}.
     * @param timeout          Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the offset at which the data were added. If the
     * operation failed, the future will be failed with the causing exception.
     * @throws NullPointerException     If any of the arguments are null, except attributeUpdates.
     * @throws IllegalArgumentException If the Segment Name is invalid (NOTE: this doesn't
     *                                  check if the Segment does not exist - that exception will be set in the
     *                                  returned CompletableFuture).
     */
    CompletableFuture<Long> append(BufferView data, AttributeUpdateCollection attributeUpdates, long offset, Duration timeout);

    /**
     * Performs an attribute update operation on the Segment.
     *
     *  @see io.pravega.segmentstore.contracts.StreamSegmentStore#append(String, BufferView, AttributeUpdateCollection, Duration)
     *
     * @param attributeUpdates A Collection of Attribute-Values to set or update. May be null (which indicates no updates).
     *                         See Notes about AttributeUpdates in the interface Javadoc.
     * @param timeout          Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the update completed successfully.
     * If the operation failed, the future will be failed with the causing exception.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the Segment Name is invalid (NOTE: this doesn't check if the Segment
     *                                  does not exist - that exception will be set in the returned CompletableFuture).
     */
    CompletableFuture<Void> updateAttributes(AttributeUpdateCollection attributeUpdates, Duration timeout);

    /**
     * Gets the values of the given Attributes (Core or Extended).
     * @see io.pravega.segmentstore.contracts.StreamSegmentStore#getAttributes(String, Collection, boolean, Duration)
     *
     * @param attributeIds A Collection of Attribute Ids to fetch. These may be Core or Extended Attributes.
     * @param cache        If set, then any Extended Attribute values that are not already in the in-memory Segment
     *                     Metadata cache will be atomically added using a conditional update (comparing against a missing value).
     *                     This argument will be ignored if the Segment is currently Sealed.
     * @param timeout      Timeout for the operation.
     * @return A Completable future that, when completed, will contain a Map of Attribute Ids to their latest values. Any
     * Attribute that is not set will also be returned (with a value equal to Attributes.NULL_ATTRIBUTE_VALUE). If the operation
     * failed, the future will be failed with the causing exception.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the Segment Name is invalid (NOTE: this doesn't check if the Segment
     *                                  does not exist - that exception will be set in the returned CompletableFuture).
     */
    CompletableFuture<Map<AttributeId, Long>> getAttributes(Collection<AttributeId> attributeIds, boolean cache, Duration timeout);

    /**
     * Initiates a Read operation on the Segment and returns a ReadResult which can be used to consume the read data.
     * @see io.pravega.segmentstore.contracts.StreamSegmentStore#read(String, long, int, Duration)
     *
     * @param offset    The offset within the Segment to start reading at.
     * @param maxLength The maximum number of bytes to read.
     * @param timeout   Timeout for the operation.
     * @return A ReadResult instance that can be used to consume the read data.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    ReadResult read(long offset, int maxLength, Duration timeout);

    /**
     * Gets information about the Segment.
     *
     * @return The requested Segment Info. Note that this result will only contain those attributes that
     * are loaded in memory (if any) or Core Attributes. To ensure that Extended Attributes are also included, you must use
     * getAttributes(), which will fetch all attributes, regardless of where they are currently located.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     * @see io.pravega.segmentstore.contracts.StreamSegmentStore#getStreamSegmentInfo(String, Duration)
     */
    SegmentMetadata getInfo();

    /**
     * Seals the Segment.
     * @see io.pravega.segmentstore.contracts.StreamSegmentStore#sealStreamSegment(String, Duration)
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the final length of the Segment.
     * If the operation failed, the future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<Long> seal(Duration timeout);

    /**
     * Truncates the Segment at a given offset.
     * @see io.pravega.segmentstore.contracts.StreamSegmentStore#truncateStreamSegment(String, long, Duration)
     *
     * @param offset  The offset at which to truncate. This must be at least equal to the existing truncation
     *                offset and no larger than the Segment's length. After the operation is complete,
     *                no offsets below this one will be accessible anymore.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception.
     */
    CompletableFuture<Void> truncate(long offset, Duration timeout);

    /**
     * Gets an iterator for the Segment's Attributes in the given range (using natural ordering based on {@link AttributeId#compareTo}.
     *
     * @param fromId  An AttributeId representing the first Attribute Id to include.
     * @param toId    An AttributeId representing the last Attribute Id to include.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will return an {@link AttributeIterator} that can be used to iterate
     * through the Segment's Attributes.
     */
    CompletableFuture<AttributeIterator> attributeIterator(AttributeId fromId, AttributeId toId, Duration timeout);

    /**
     * Gets the number of Extended (non-Core) Attributes for this segment that have been persisted into the index.
     * This may not include recently updated or removed Attributes (only those that are stored in the index).
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will return the number of Extended Attributes for this Segment.
     * If such statistics are not maintained on the Segment (for example, a legacy segment), this will be completed with -1.
     */
    CompletableFuture<Long> getExtendedAttributeCount(Duration timeout);
}
