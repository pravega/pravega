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
package io.pravega.segmentstore.server.attributes;

import io.pravega.segmentstore.server.AttributeIndex;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Collection of Attribute Index objects.
 */
public interface ContainerAttributeIndex extends AutoCloseable {
    /**
     * Gets or creates an AttributeIndex instance and initializes it.
     *
     * @param streamSegmentId The Id of the Segment to create the AttributeIndex for.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the requested AttributeIndex.
     */
    CompletableFuture<AttributeIndex> forSegment(long streamSegmentId, Duration timeout);

    /**
     * Deletes any existing attribute data pertaining to the given Segment.
     *
     * @param segmentName The name of the Segment whose attribute data should be deleted.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation finished.
     */
    CompletableFuture<Void> delete(String segmentName, Duration timeout);

    /**
     * Removes all internal indices that point to the given StreamSegments from memory. This does not delete the data itself.
     *
     * @param segmentIds A Collection of SegmentIds for the Segments to clean up. If this is null, then all the Segment Ids
     *                   registered in this ContainerAttributeIndex are eligible for removal.
     */
    void cleanup(Collection<Long> segmentIds);

    @Override
    void close();
}
