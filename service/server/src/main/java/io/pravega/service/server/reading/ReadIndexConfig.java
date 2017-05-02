/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.service.server.reading;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.time.Duration;

import lombok.Getter;

/**
 * Configuration for Read Index.
 */
public class ReadIndexConfig {
    //region Config Names
    public static final Property<Integer> STORAGE_READ_ALIGNMENT = Property.named("storageReadAlignment", 1024 * 1024);
    public static final Property<Integer> MEMORY_READ_MIN_LENGTH = Property.named("memoryReadMinLength", 4 * 1024);
    public static final Property<Integer> STORAGE_READ_DEFAULT_TIMEOUT = Property.named("storageReadDefaultTimeoutMillis", 30 * 1000);
    public static final Property<Long> CACHE_POLICY_MAX_SIZE = Property.named("cacheMaxSize", 16L * 1024 * 1024 * 1024);
    public static final Property<Integer> CACHE_POLICY_MAX_TIME = Property.named("cacheMaxTimeMillis", 30 * 60 * 1000);
    public static final Property<Integer> CACHE_POLICY_GENERATION_TIME = Property.named("cacheGenerationTimeMillis", 5 * 1000);
    private static final String COMPONENT_CODE = "readindex";

    //endregion

    //region Members

    /**
     * A value to align all Storage Reads to. When a Storage Read is issued, the read length is adjusted (if possible)
     * to end on a multiple of this value.
     */
    @Getter
    private final int storageReadAlignment;

    /**
     * The minimum number of bytes to serve from memory during reads. The ReadIndex will try to coalesce data from multiple
     * contiguous index entries, as long as they are all referring to cached data, when serving individual ReadResultEntries
     * from the ReadResult coming from a call to read(). There is no guarantee that all entries will be at least of this
     * size, nor that they will be exactly of this size (it is just an attempt at optimization).
     * <p>
     * This is most effective in cases when there is a large number of very small appends, which would otherwise be
     * returned as individual ReadResultEntries - this setting allows coalescing multiple such items into a single call.
     * <p>
     * Setting this to 0 will effectively disable this feature.
     */
    @Getter
    private final int memoryReadMinLength;

    /**
     * The CachePolicy, as defined in this configuration.
     */
    @Getter
    private final CachePolicy cachePolicy;

    /**
     * The Default Timeout (should no other value be provided) for Storage reads.
     */
    @Getter
    private final Duration storageReadDefaultTimeout;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DurableLogConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ReadIndexConfig(TypedProperties properties) throws ConfigurationException {
        this.storageReadAlignment = properties.getInt(STORAGE_READ_ALIGNMENT);
        this.memoryReadMinLength = properties.getInt(MEMORY_READ_MIN_LENGTH);
        this.storageReadDefaultTimeout = Duration.ofMillis(properties.getInt(STORAGE_READ_DEFAULT_TIMEOUT));
        long cachePolicyMaxSize = properties.getLong(CACHE_POLICY_MAX_SIZE);
        int cachePolicyMaxTime = properties.getInt(CACHE_POLICY_MAX_TIME);
        int cachePolicyGenerationTime = properties.getInt(CACHE_POLICY_GENERATION_TIME);
        this.cachePolicy = new CachePolicy(cachePolicyMaxSize, Duration.ofMillis(cachePolicyMaxTime), Duration.ofMillis(cachePolicyGenerationTime));
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ReadIndexConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ReadIndexConfig::new);
    }

    //endregion
}
