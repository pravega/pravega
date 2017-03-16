/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.TypedProperties;
import java.time.Duration;
import lombok.Getter;

/**
 * Configuration for Read Index.
 */
public class ReadIndexConfig {
    //region Config Names
    public static final String PROPERTY_STORAGE_READ_ALIGNMENT = "storageReadAlignment";
    public static final String PROPERTY_MEMORY_READ_MIN_LENGTH = "memoryReadMinLength";
    public static final String PROPERTY_CACHE_POLICY_MAX_SIZE = "cacheMaxSize";
    public static final String PROPERTY_CACHE_POLICY_MAX_TIME = "cacheMaxTimeMillis";
    public static final String PROPERTY_CACHE_POLICY_GENERATION_TIME = "cacheGenerationTimeMillis";
    private static final String COMPONENT_CODE = "readindex";

    private final static int DEFAULT_STORAGE_READ_ALIGNMENT = 1024 * 1024;
    private final static int DEFAULT_MEMORY_READ_MIN_LENGTH = 4 * 1024;
    private final static long DEFAULT_CACHE_POLICY_MAX_SIZE = 4L * 1024 * 1024 * 1024; // 4GB
    private final static int DEFAULT_CACHE_POLICY_MAX_TIME = 30 * 60 * 1000; // 30 mins
    private final static int DEFAULT_CACHE_POLICY_GENERATION_TIME = 5 * 1000; // 5 seconds

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

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DurableLogConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ReadIndexConfig(TypedProperties properties) throws ConfigurationException {
        this.storageReadAlignment = properties.getInt32(PROPERTY_STORAGE_READ_ALIGNMENT, DEFAULT_STORAGE_READ_ALIGNMENT);
        this.memoryReadMinLength = properties.getInt32(PROPERTY_MEMORY_READ_MIN_LENGTH, DEFAULT_MEMORY_READ_MIN_LENGTH);
        long cachePolicyMaxSize = properties.getInt64(PROPERTY_CACHE_POLICY_MAX_SIZE, DEFAULT_CACHE_POLICY_MAX_SIZE);
        int cachePolicyMaxTime = properties.getInt32(PROPERTY_CACHE_POLICY_MAX_TIME, DEFAULT_CACHE_POLICY_MAX_TIME);
        int cachePolicyGenerationTime = properties.getInt32(PROPERTY_CACHE_POLICY_GENERATION_TIME, DEFAULT_CACHE_POLICY_GENERATION_TIME);
        this.cachePolicy = new CachePolicy(cachePolicyMaxSize, Duration.ofMillis(cachePolicyMaxTime), Duration.ofMillis(cachePolicyGenerationTime));
    }

    /**
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ReadIndexConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ReadIndexConfig::new);
    }

    //endregion
}
