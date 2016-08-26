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

package com.emc.pravega.service.server.writer;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.InvalidPropertyValueException;
import com.emc.pravega.common.util.MissingPropertyException;

import java.time.Duration;
import java.util.Properties;

/**
 * Writer Configuration
 */
public class WriterConfig extends ComponentConfig {
    //region Members

    public final static String COMPONENT_CODE = "writer";
    public static final String PROPERTY_FLUSH_THRESHOLD_BYTES = "flushThresholdBytes";
    public static final String PROPERTY_FLUSH_THRESHOLD_MILLIS = "flushThresholdMillis";
    public static final String PROPERTY_MAX_ITEMS_TO_READ_AT_ONCE = "maxItemsToReadAtOnce";

    private static final int DEFAULT_FLUSH_THRESHOLD_BYTES = 4 * 1024 * 1024; // 4MB
    private static final int DEFAULT_FLUSH_THRESHOLD_MILLIS = 30 * 1000; // 30s
    private static final int DEFAULT_MAX_ITEMS_TO_READ_AT_ONCE = 100;

    private int flushThresholdBytes;
    private Duration flushThresholdTime;
    private int maxItemsToReadAtOnce;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriterConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws MissingPropertyException Whenever a required Property is missing from the given properties collection.
     * @throws NumberFormatException    Whenever a Property has a value that is invalid for it.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public WriterConfig(Properties properties) throws MissingPropertyException, InvalidPropertyValueException {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the minimum number of bytes to wait for before flushing aggregated data for a Segment to Storage.
     *
     * @return The result.
     */
    public int getFlushThresholdBytes() {
        return this.flushThresholdBytes;
    }

    /**
     * Gets a value indicating the minimum amount of time to wait for before flushing aggregated data for a Segment to Storage.
     *
     * @return The result.
     */
    public Duration getFlushThresholdTime() {
        return this.flushThresholdTime;
    }

    /**
     * Gets a value indicating the maximum number of items to read every time a read is issued to the OperationLog.
     *
     * @return The result.
     */
    public int getMaxItemsToReadAtOnce() {
        return this.maxItemsToReadAtOnce;
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        this.flushThresholdBytes = getInt32Property(PROPERTY_FLUSH_THRESHOLD_BYTES, DEFAULT_FLUSH_THRESHOLD_BYTES);
        if (this.flushThresholdBytes < 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a non-negative integer.", PROPERTY_FLUSH_THRESHOLD_BYTES));
        }

        long flushMillis = getInt64Property(PROPERTY_FLUSH_THRESHOLD_MILLIS, DEFAULT_FLUSH_THRESHOLD_MILLIS);
        this.flushThresholdTime = Duration.ofMillis(flushMillis);

        this.maxItemsToReadAtOnce = getInt32Property(PROPERTY_MAX_ITEMS_TO_READ_AT_ONCE, DEFAULT_MAX_ITEMS_TO_READ_AT_ONCE);
        if (this.maxItemsToReadAtOnce <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer.", PROPERTY_MAX_ITEMS_TO_READ_AT_ONCE));
        }
    }

    //endregion
}
