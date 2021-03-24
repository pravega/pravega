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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration for {@link ContainerTableExtensionImpl} and sub-components.
 */
@Data
@Builder
class TableExtensionConfig {
    /**
     * The maximum unindexed length ({@link SegmentProperties#getLength() - {@link TableAttributes#INDEX_OFFSET}}) of a
     * Segment for which {@link ContainerKeyIndex} {@code triggerCacheTailIndex} can be invoked.
     */
    @Builder.Default
    private int maxTailCachePreIndexLength = EntrySerializer.MAX_BATCH_SIZE * 4;

    /**
     * The maximum allowed unindexed length ({@link SegmentProperties#getLength() - {@link TableAttributes#INDEX_OFFSET}})
     * for a Segment. As long as a Segment's unindexed length is below this value, any new update that would not cause
     * its unindexed length to exceed it will be allowed. Any updates that do not meet this criteria will be blocked until
     * {@link ContainerKeyIndex#notifyIndexOffsetChanged} indicates that this value has been reduced sufficiently in order
     * to allow it to proceed.
     */
    @Builder.Default
    private final int maxUnindexedLength = EntrySerializer.MAX_BATCH_SIZE * 4;

    /**
     * The default value to supply to a {@link WriterTableProcessor} to indicate how big compactions need to be.
     * We need to return a value that is large enough to encompass the largest possible Table Entry (otherwise
     * compaction will stall), but not too big, as that will introduce larger indexing pauses when compaction is running.
     */
    @Builder.Default
    private final int maxCompactionSize = EntrySerializer.MAX_SERIALIZATION_LENGTH * 4;

    /**
     * Default value to set for the {@link TableAttributes#MIN_UTILIZATION} for every new Table Segment.
     */
    @Builder.Default
    private final long defaultMinUtilization = 75L;

    /**
     * Default value to set for the {@link Attributes#ROLLOVER_SIZE} for every new Table Segment.
     */
    @Builder.Default
    private final long defaultRolloverSize = EntrySerializer.MAX_SERIALIZATION_LENGTH * 4 * 4;

    /**
     * The maximum size of a single update batch. For unit test purposes only. Do not tinker with in production code.
     */
    @Builder.Default
    @VisibleForTesting
    private final int maxBatchSize = EntrySerializer.MAX_BATCH_SIZE;

    /**
     * The maximum amount of time to wait for a Table Segment Recovery. If any recovery takes more than this amount of time,
     * all registered calls will be failed with a {@link TimeoutException}.
     */
    @Builder.Default
    private Duration recoveryTimeout = Duration.ofSeconds(60);

    /**
     * The default Segment Attributes to set for every new Table Segment. These values will override the corresponding
     * defaults from {@link TableAttributes#DEFAULT_VALUES}.
     */
    Map<UUID, Long> getDefaultCompactionAttributes() {
        return ImmutableMap.of(TableAttributes.MIN_UTILIZATION, getDefaultMinUtilization(),
                Attributes.ROLLOVER_SIZE, getDefaultRolloverSize());
    }
}
