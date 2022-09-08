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
package io.pravega.segmentstore.server.containers;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import lombok.Getter;

/**
 * Segment Container Configuration.
 */
public class ContainerConfig {
    //region Members

    public static final int MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS = 60; // Minimum possible value for segmentExpiration
    public static final Property<Integer> SEGMENT_METADATA_EXPIRATION_SECONDS = Property.named("segment.metadata.expiry.seconds",
            MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS, "segmentMetadataExpirationSeconds");
    public static final Property<Integer> STORAGE_SNAPSHOT_TIMEOUT_SECONDS = Property.named("storage.snapshot.timeout.seconds", MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS);
    public static final Property<Integer> METADATA_STORE_INIT_TIMEOUT_SECONDS = Property.named("metadataStore.init.timeout.seconds", 30, "metadataStoreInitTimeoutSeconds");
    public static final Property<Integer> MAX_ACTIVE_SEGMENT_COUNT = Property.named("segment.active.count.max", 25000, "maxActiveSegmentCount");
    public static final Property<Integer> MAX_CONCURRENT_SEGMENT_EVICTION_COUNT = Property.named("segment.eviction.concurrent.count.max", 2500, "maxConcurrentSegmentEvictionCount");
    public static final Property<Integer> MAX_CACHED_EXTENDED_ATTRIBUTE_COUNT = Property.named("extended.attribute.cached.count.max", 4096, "maxCachedExtendedAttributeCount");
    public static final Property<Integer> EVENT_PROCESSOR_ITERATION_DELAY_MS = Property.named("eventprocessor.iteration.delay.ms", 1000);
    public static final Property<Integer> EVENT_PROCESSOR_OPERATION_TIMEOUT_MS = Property.named("eventprocessor.operation.timeout.ms", 5000);
    public static final Property<Integer> TRANSIENT_SEGMENT_DELETE_TIMEOUT_MS = Property.named("segment.transient.delete.timeout.ms", 30000);
    public static final Property<Boolean> DATA_INTEGRITY_CHECKS_ENABLED = Property.named("data.integrity.checks.enabled", false);
    private static final String COMPONENT_CODE = "containers";

    /**
     * The amount of time after which Segments are eligible for eviction from the metadata.
     */
    @Getter
    private final Duration segmentMetadataExpiration;

    /**
     * The amount of time to wait for the Metadata Store to initialize.
     */
    @Getter
    private final Duration metadataStoreInitTimeout;

    /**
     * The maximum number of segments that can be active at any given time in a container.
     */
    @Getter
    private final int maxActiveSegmentCount;

    /**
     * The maximum number of segments to evict at once.
     */
    @Getter
    private final int maxConcurrentSegmentEvictionCount;

    /**
     * The maximum number of cached Extended Attributes that are allowed.
     */
    @Getter
    private final int maxCachedExtendedAttributeCount;

    /**
     * Default timeout for StorageSnapshot operations.
     */
    @Getter
    private final Duration storageSnapshotTimeout;

    /**
     * Default delay between consecutive processing iterations of ContainerEventProcessor.
     */
    @Getter
    private final Duration eventProcessorIterationDelay;

    /**
     * Default timeout for EventProcessor operations against SegmentContainer.
     */
    @Getter
    private final Duration eventProcessorOperationTimeout;

    /**
     * Default timeout for the deletion of Transient Segments from Metadata.
     */
    @Getter
    private final Duration transientSegmentDeleteTimeout;

    /**
     * Whether to enable data integrity checks in the ingestion pipeline (i.e., hash data to validate integrity).
     * Note that this feature is mainly devised for testing or development environments, as it may induce additional
     * computation cost that may be detrimental to performance.
     */
    @Getter
    private final boolean dataIntegrityChecksEnabled;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     */
    ContainerConfig(TypedProperties properties) {
        int segmentMetadataExpirationSeconds = properties.getInt(SEGMENT_METADATA_EXPIRATION_SECONDS);
        if (segmentMetadataExpirationSeconds < MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS) {
            throw new ConfigurationException(String.format("Property '%s' must be at least %s.",
                    SEGMENT_METADATA_EXPIRATION_SECONDS, MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS));
        }
        this.segmentMetadataExpiration = Duration.ofSeconds(segmentMetadataExpirationSeconds);
        this.metadataStoreInitTimeout = properties.getDuration(METADATA_STORE_INIT_TIMEOUT_SECONDS, ChronoUnit.SECONDS);
        this.storageSnapshotTimeout = properties.getDuration(STORAGE_SNAPSHOT_TIMEOUT_SECONDS, ChronoUnit.SECONDS);
        this.maxActiveSegmentCount = properties.getPositiveInt(MAX_ACTIVE_SEGMENT_COUNT);
        this.maxConcurrentSegmentEvictionCount = properties.getPositiveInt(MAX_CONCURRENT_SEGMENT_EVICTION_COUNT);
        this.maxCachedExtendedAttributeCount = properties.getPositiveInt(MAX_CACHED_EXTENDED_ATTRIBUTE_COUNT);
        this.eventProcessorIterationDelay = properties.getDuration(EVENT_PROCESSOR_ITERATION_DELAY_MS, ChronoUnit.MILLIS);
        this.eventProcessorOperationTimeout = properties.getDuration(EVENT_PROCESSOR_OPERATION_TIMEOUT_MS, ChronoUnit.MILLIS);
        this.transientSegmentDeleteTimeout = properties.getDuration(TRANSIENT_SEGMENT_DELETE_TIMEOUT_MS, ChronoUnit.MILLIS);
        this.dataIntegrityChecksEnabled = properties.getBoolean(DATA_INTEGRITY_CHECKS_ENABLED);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ContainerConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ContainerConfig::new);
    }

    //endregion
}
