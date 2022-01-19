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
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import lombok.Getter;

/**
 * Configuration for {@link ContainerTableExtensionImpl} and sub-components.
 * NOTE: This should only be used for testing or cluster repair purposes. Even though these settings can be set externally,
 * it is not recommended to expose them to users. The defaults are chosen conservatively to ensure proper system functioning
 * under most circumstances.
 */
@Getter
public class TableExtensionConfig {
    public static final Property<Long> MAX_TAIL_CACHE_PREINDEX_LENGTH = Property.named("preindex.bytes.max", (long) EntrySerializer.MAX_BATCH_SIZE * 4);
    public static final Property<Integer> MAX_TAIL_CACHE_PREINDEX_BATCH_SIZE = Property.named("preindex.batch.bytes.max", EntrySerializer.MAX_BATCH_SIZE * 4);
    public static final Property<Integer> RECOVERY_TIMEOUT = Property.named("recovery.timeout.millis", 60000);
    public static final Property<Integer> MAX_UNINDEXED_LENGTH = Property.named("unindexed.bytes.max", EntrySerializer.MAX_BATCH_SIZE * 4);
    public static final Property<Integer> SYSTEM_CRITICAL_MAX_UNINDEXED_LENGTH = Property.named("systemcritical.unindexed.bytes.max", EntrySerializer.MAX_BATCH_SIZE * 8);
    public static final Property<Integer> MAX_COMPACTION_SIZE = Property.named("compaction.bytes.max", EntrySerializer.MAX_SERIALIZATION_LENGTH * 4);
    public static final Property<Integer> COMPACTION_FREQUENCY = Property.named("compaction.frequency.millis", 30000);
    public static final Property<Integer> DEFAULT_MIN_UTILIZATION = Property.named("utilization.min", 75);
    public static final Property<Long> DEFAULT_ROLLOVER_SIZE = Property.named("rollover.size.bytes", (long) EntrySerializer.MAX_SERIALIZATION_LENGTH * 4 * 4);
    public static final Property<Integer> MAX_BATCH_SIZE = Property.named("batch.size.bytes", EntrySerializer.MAX_BATCH_SIZE);
    private static final String COMPONENT_CODE = "tables";

    /**
     * The maximum unindexed length ({@link SegmentProperties#getLength() - {@link TableAttributes#INDEX_OFFSET}}) of a
     * Segment for which {@link ContainerKeyIndex} {@code triggerCacheTailIndex} can be invoked.
     */
    private final long maxTailCachePreIndexLength;

    /**
     * The maximum number of bytes to read and process at once from the segment while performing preindexing.
     * See {@link #getMaxTailCachePreIndexLength()}.
     */
    private final int maxTailCachePreIndexBatchLength;

    /**
     * The maximum allowed unindexed length ({@link SegmentProperties#getLength() - {@link TableAttributes#INDEX_OFFSET}})
     * for a Segment. As long as a Segment's unindexed length is below this value, any new update that would not cause
     * its unindexed length to exceed it will be allowed. Any updates that do not meet this criteria will be blocked until
     * {@link ContainerKeyIndex#notifyIndexOffsetChanged} indicates that this value has been reduced sufficiently in order
     * to allow it to proceed.
     */
    private final int maxUnindexedLength;

    /**
     * Same as {@link #maxUnindexedLength}, but it applies specifically to system-critical Segments. This allows us to
     * tune with more precision the amount of unindexed data for system-critical Segments, which are key to the
     * operation of the system.
     */
    private final int systemCriticalMaxUnindexedLength;

    /**
     * The default value to supply to a {@link WriterTableProcessor} to indicate how big compactions need to be.
     * We need to return a value that is large enough to encompass the largest possible Table Entry (otherwise
     * compaction will stall), but not too big, as that will introduce larger indexing pauses when compaction is running.
     */
    private final int maxCompactionSize;

    /**
     * The amount of time to wait between successive compaction attempts on the same Table Segment. This may not apply
     * to all Table Segment Layouts (i.e., it only applies to Fixed-Key-Length Table Segments).
     */
    private final Duration compactionFrequency;

    /**
     * Default value to set for the {@link TableAttributes#MIN_UTILIZATION} for every new Table Segment.
     */
    private final long defaultMinUtilization;

    /**
     * Default value to set for the {@link Attributes#ROLLOVER_SIZE} for every new Table Segment.
     */
    private final long defaultRolloverSize;

    /**
     * The maximum size of a single update batch. For unit test purposes only.
     * IMPORTANT: Do not tinker with in production code. Enforced to be at most {@link EntrySerializer#MAX_BATCH_SIZE}.
     */
    @VisibleForTesting
    private final int maxBatchSize;

    /**
     * The maximum amount of time to wait for a Table Segment Recovery. If any recovery takes more than this amount of time,
     * all registered calls will be failed with a {@link TimeoutException}.
     */
    private final Duration recoveryTimeout;

    private TableExtensionConfig(TypedProperties properties) throws ConfigurationException {
        this.maxTailCachePreIndexLength = properties.getPositiveLong(MAX_TAIL_CACHE_PREINDEX_LENGTH);
        this.maxTailCachePreIndexBatchLength = properties.getPositiveInt(MAX_TAIL_CACHE_PREINDEX_BATCH_SIZE);
        this.maxUnindexedLength = properties.getPositiveInt(MAX_UNINDEXED_LENGTH);
        this.systemCriticalMaxUnindexedLength = properties.getPositiveInt(SYSTEM_CRITICAL_MAX_UNINDEXED_LENGTH);
        this.maxCompactionSize = properties.getPositiveInt(MAX_COMPACTION_SIZE);
        this.compactionFrequency = properties.getDuration(COMPACTION_FREQUENCY, ChronoUnit.MILLIS);
        this.defaultMinUtilization = properties.getNonNegativeInt(DEFAULT_MIN_UTILIZATION);
        if (this.defaultMinUtilization > 100) {
            throw new ConfigurationException(String.format("Property '%s' must be a value within [0, 100].", DEFAULT_MIN_UTILIZATION));
        }
        this.defaultRolloverSize = properties.getPositiveLong(DEFAULT_ROLLOVER_SIZE);
        this.maxBatchSize = properties.getPositiveInt(MAX_BATCH_SIZE);
        if (this.maxBatchSize > EntrySerializer.MAX_BATCH_SIZE) {
            throw new ConfigurationException(String.format("Property '%s' must be a value within [0, %s].", DEFAULT_MIN_UTILIZATION, EntrySerializer.MAX_BATCH_SIZE));
        }
        this.recoveryTimeout = properties.getDuration(RECOVERY_TIMEOUT, ChronoUnit.MILLIS);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<TableExtensionConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, TableExtensionConfig::new);
    }

    /**
     * The default Segment Attributes to set for every new Table Segment. These values will override the corresponding
     * defaults from {@link TableAttributes#DEFAULT_VALUES}.
     */
    public Map<AttributeId, Long> getDefaultCompactionAttributes() {
        return ImmutableMap.of(TableAttributes.MIN_UTILIZATION, getDefaultMinUtilization(),
                Attributes.ROLLOVER_SIZE, getDefaultRolloverSize());
    }
}
