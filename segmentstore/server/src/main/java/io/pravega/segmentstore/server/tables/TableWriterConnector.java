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
package io.pravega.segmentstore.server.tables;

import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMetadata;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * A connector between the {@link ContainerTableExtension} implementation and the {@link WriterTableProcessor} responsible
 * for handling a particular Segment.
 */
interface TableWriterConnector extends AutoCloseable {
    /**
     * Gets the {@link SegmentMetadata} for the Table Segment this connector refers to.
     *
     * @return The {@link SegmentMetadata}.
     */
    SegmentMetadata getMetadata();

    /**
     * Gets the {@link EntrySerializer} used for this Table Segment.
     *
     * @return The {@link EntrySerializer}.
     */
    EntrySerializer getSerializer();

    /**
     * Gets the {@link KeyHasher} used for this Table Segment.
     *
     * @return The {@link KeyHasher}.
     */
    KeyHasher getKeyHasher();

    /**
     * Gets a {@link DirectSegmentAccess} that can be used to operate directly on the Table Segment.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the desired result.
     */
    CompletableFuture<DirectSegmentAccess> getSegment(Duration timeout);

    /**
     * This method will be invoked by the {@link WriterTableProcessor} after every successful call to
     * {@link WriterTableProcessor#flush} which advanced the value of the {@link TableAttributes#INDEX_OFFSET} attribute
     * on the Table Segment this connector refers to.
     *
     * @param lastIndexedOffset  The current value of the {@link TableAttributes#INDEX_OFFSET} attribute.
     * @param processedSizeBytes The number of bytes processed during this update (including duplicates).
     */
    void notifyIndexOffsetChanged(long lastIndexedOffset, int processedSizeBytes);

    /**
     * Gets a value representing the maximum length that a Table Segment compaction can process at once.
     *
     * @return The maximum compaction length.
     */
    int getMaxCompactionSize();

    /**
     * Gets a value representing the maximum number of bytes to attempt to index (flush) at once.
     * @return The maximum flush size.
     */
    default int getMaxFlushSize() {
        return 134217728; // 128MB
    }

    /**
     * This method will be invoked by the {@link WriterTableProcessor} when it is closed.
     */
    @Override
    void close();
}
