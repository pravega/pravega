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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines the StreamSegmentStore which is responsible for delegating the various
 * operations possible on a StreamSegment to their respective Container.
 */
public interface StreamSegmentStore extends SegmentApi {

    /**
     * Applies all outstanding operations in a particular SegmentContainer from the DurableLog into the underlying Storage.
     *
     * @param containerId The Id of the container that needs to persisted to storage.
     * @param timeout     Timeout for the operation.
     * @return A Completable future that when completed, will indicate that the operation has been successfully completed.
     * If the operation fails, it will be completed with the appropriate exception.
     */
    CompletableFuture<Void> flushToStorage(int containerId, Duration timeout);

    /**
     * Performs sanity operations on chunk like create chunk, write to the chunk, check if the chunk exists, read back contents to the chunk and delete the chunk.
     *
     * @param containerId The Id of the container on which sanity operations are to be performed.
     * @param chunkName Name of the chunk on which sanity operations are to be performed.
     * @param dataSize dataSize of the bytes to read.
     * @param timeout Timeout for the operation.
     * @return A Completable future that when completed, will indicate that the operation has been successfully completed.
     */
    CompletableFuture<Void> checkChunkStorageSanity(int containerId, String chunkName, int dataSize, Duration timeout);

    /**
     * Evicts all eligible entries from buffer cache and all entries from guava cache.
     *
     * @param containerId The Id of the container for which meta data cache eviction is performed.
     * @param timeout Timeout for the operation.
     * @return A Completable future that when completed, will indicate that the operation has been successfully completed.
     */
    CompletableFuture<Void> evictMetaDataCache(int containerId, Duration timeout);

    /**
     * Evict entire read index cache.
     *
     * @param containerId The Id of the container for which read index cache eviction is performed.
     * @param timeout Timeout for the operation.
     * @return A Completable future that when completed, will indicate that the operation has been successfully completed.
     */
    CompletableFuture<Void> evictReadIndexCache(int containerId, Duration timeout);

    /**
     * Evict entire read index cache for given segment.
     *
     * @param containerId The Id of the container for which read index cache eviction is performed.
     * @param segmentName Name of the segment for which read index cache eviction is performed.
     * @param timeout Timeout for the operation.
     * @return A Completable future that when completed, will indicate that the operation has been successfully completed.
     */
    CompletableFuture<Void> evictReadIndexCacheForSegment(int containerId, String segmentName, Duration timeout);
}
