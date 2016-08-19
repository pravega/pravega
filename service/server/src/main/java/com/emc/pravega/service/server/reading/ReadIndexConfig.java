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
import com.emc.pravega.common.util.InvalidPropertyValueException;
import com.emc.pravega.common.util.MissingPropertyException;

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
    public static final String PROPERTY_CACHE_POLICY_MAX_SIZE = "cacheMaxSize";
    public static final String PROPERTY_CACHE_POLICY_MAX_TIME = "cacheMaxTimeMillis";
    public static final String PROPERTY_CACHE_POLICY_GENERATION_TIME = "cacheGenerationTimeMillis";

    private final static int DEFAULT_STORAGE_READ_MIN_LENGTH = 100 * 1024;
    private final static int DEFAULT_STORAGE_READ_MAX_LENGTH = 1024 * 1024;
    private final static long DEFAULT_CACHE_POLICY_MAX_SIZE = 4L * 1024 * 1024 * 1024; // 4GB
    private final static int DEFAULT_CACHE_POLICY_MAX_TIME = 30 * 60 * 1000; // 30 mins
    private final static int DEFAULT_CACHE_POLICY_GENERATION_TIME = 5 * 1000; // 5 seconds

    private int storageReadMinLength;
    private int storageReadMaxLength;
    private CachePolicy cachePolicy;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DurableLogConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws MissingPropertyException Whenever a required Property is missing from the given properties collection.
     * @throws NumberFormatException    Whenever a Property has a value that is invalid for it.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public ReadIndexConfig(Properties properties) throws MissingPropertyException, InvalidPropertyValueException {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    /**
     * Gets a value indicating the minimum number of bytes that should be read with any given operation from Storage.
     *
     * @return The value.
     */
    public int getStorageReadMinLength() {
        return this.storageReadMinLength;
    }

    /**
     * Gets a value indicating the maximum number of bytes that should be read with any given operation from Storage.
     * This also sets the read alignment for Storage Reads. Cache entries that are aligned to some offset multiplier are
     * easier to manage and have a lower chance of collisions (attempting to insert entries that overlap).
     *
     * @return The value.
     */
    public int getStorageReadMaxLength() {
        return this.storageReadMaxLength;
    }

    /**
     * Gets a pointer to the CachePolicy, as defined in this configuration.
     *
     * @return The CachePolicy.
     */
    public CachePolicy getCachePolicy() {
        return this.cachePolicy;
    }

    @Override
    protected void refresh() throws MissingPropertyException, InvalidPropertyValueException {
        this.storageReadMinLength = getInt32Property(PROPERTY_STORAGE_READ_MIN_LENGTH, DEFAULT_STORAGE_READ_MIN_LENGTH);
        this.storageReadMaxLength = getInt32Property(PROPERTY_STORAGE_READ_MAX_LENGTH, DEFAULT_STORAGE_READ_MAX_LENGTH);
        if (this.storageReadMinLength > this.storageReadMaxLength) {
            throw new InvalidPropertyValueException(String.format("Property '%s' (%d) cannot be larger than Property '%s' (%d).", PROPERTY_STORAGE_READ_MIN_LENGTH, this.storageReadMinLength, PROPERTY_STORAGE_READ_MAX_LENGTH, this.storageReadMaxLength));
        }

        long cachePolicyMaxSize = getInt64Property(PROPERTY_CACHE_POLICY_MAX_SIZE, DEFAULT_CACHE_POLICY_MAX_SIZE);
        int cachePolicyMaxTime = getInt32Property(PROPERTY_CACHE_POLICY_MAX_TIME, DEFAULT_CACHE_POLICY_MAX_TIME);
        int cachePolicyGenerationTime = getInt32Property(PROPERTY_CACHE_POLICY_GENERATION_TIME, DEFAULT_CACHE_POLICY_GENERATION_TIME);
        this.cachePolicy = new CachePolicy(cachePolicyMaxSize, Duration.ofMillis(cachePolicyMaxTime), Duration.ofMillis(cachePolicyGenerationTime));
    }
}
