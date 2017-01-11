/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.InvalidPropertyValueException;
import lombok.Getter;

import java.time.Duration;
import java.util.Properties;

/**
 * Configuration for Read Index.
 */
public class ReadIndexConfig extends ComponentConfig {
    //region Members
    public final static String COMPONENT_CODE = "readindex";
    public static final String PROPERTY_STORAGE_READ_MIN_LENGTH = "storageReadMinLength";
    public static final String PROPERTY_STORAGE_READ_MAX_LENGTH = "storageReadMaxLength";
    public static final String PROPERTY_MEMORY_READ_MIN_LENGTH = "memoryReadMinLength";
    public static final String PROPERTY_CACHE_POLICY_MAX_SIZE = "cacheMaxSize";
    public static final String PROPERTY_CACHE_POLICY_MAX_TIME = "cacheMaxTimeMillis";
    public static final String PROPERTY_CACHE_POLICY_GENERATION_TIME = "cacheGenerationTimeMillis";

    private final static int DEFAULT_STORAGE_READ_MIN_LENGTH = 100 * 1024;
    private final static int DEFAULT_STORAGE_READ_MAX_LENGTH = 1024 * 1024;
    private final static int DEFAULT_MEMORY_READ_MIN_LENGTH = 4 * 1024;
    private final static long DEFAULT_CACHE_POLICY_MAX_SIZE = 4L * 1024 * 1024 * 1024; // 4GB
    private final static int DEFAULT_CACHE_POLICY_MAX_TIME = 30 * 60 * 1000; // 30 mins
    private final static int DEFAULT_CACHE_POLICY_GENERATION_TIME = 5 * 1000; // 5 seconds

    /**
     * The minimum number of bytes that should be read with any given operation from Storage.
     */
    @Getter
    private int storageReadMinLength;

    /**
     * The maximum number of bytes that should be read with any given operation from Storage.
     * This also sets the read alignment for Storage Reads. Cache entries that are aligned to some offset multiplier are
     * easier to manage and have a lower chance of collisions (attempting to insert entries that overlap).
     */
    @Getter
    private int storageReadMaxLength;

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
    private int memoryReadMinLength;

    /**
     * The CachePolicy, as defined in this configuration.
     */
    @Getter
    private CachePolicy cachePolicy;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DurableLogConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws ConfigurationException   When a configuration issue has been detected. This can be:
     *                                  MissingPropertyException (a required Property is missing from the given properties collection),
     *                                  NumberFormatException (a Property has a value that is invalid for it).
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public ReadIndexConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    @Override
    protected void refresh() throws ConfigurationException {
        this.storageReadMinLength = getInt32Property(PROPERTY_STORAGE_READ_MIN_LENGTH, DEFAULT_STORAGE_READ_MIN_LENGTH);
        this.storageReadMaxLength = getInt32Property(PROPERTY_STORAGE_READ_MAX_LENGTH, DEFAULT_STORAGE_READ_MAX_LENGTH);
        if (this.storageReadMinLength > this.storageReadMaxLength) {
            throw new InvalidPropertyValueException(String.format("Property '%s' (%d) cannot be larger than Property '%s' (%d).", PROPERTY_STORAGE_READ_MIN_LENGTH, this.storageReadMinLength, PROPERTY_STORAGE_READ_MAX_LENGTH, this.storageReadMaxLength));
        }

        this.memoryReadMinLength = getInt32Property(PROPERTY_MEMORY_READ_MIN_LENGTH, DEFAULT_MEMORY_READ_MIN_LENGTH);

        long cachePolicyMaxSize = getInt64Property(PROPERTY_CACHE_POLICY_MAX_SIZE, DEFAULT_CACHE_POLICY_MAX_SIZE);
        int cachePolicyMaxTime = getInt32Property(PROPERTY_CACHE_POLICY_MAX_TIME, DEFAULT_CACHE_POLICY_MAX_TIME);
        int cachePolicyGenerationTime = getInt32Property(PROPERTY_CACHE_POLICY_GENERATION_TIME, DEFAULT_CACHE_POLICY_GENERATION_TIME);
        this.cachePolicy = new CachePolicy(cachePolicyMaxSize, Duration.ofMillis(cachePolicyMaxTime), Duration.ofMillis(cachePolicyGenerationTime));
    }
}
