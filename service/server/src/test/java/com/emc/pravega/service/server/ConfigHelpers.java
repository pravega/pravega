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

package com.emc.pravega.service.server;

import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.CachePolicy;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.writer.WriterConfig;

import java.time.Duration;
import java.util.Properties;

/**
 * Helper class that can be used to quickly create Configurations.
 */
public class ConfigHelpers {
    public static final CachePolicy INFINITE_CACHE_POLICY = new CachePolicy(Long.MAX_VALUE, Duration.ofMillis(Integer.MAX_VALUE), Duration.ofMillis(Integer.MAX_VALUE));

    /**
     * Creates a new instance of the DurableLogConfig class with given arguments.
     *
     * @param checkpointMinCommitCount
     * @param checkpointCommitCount
     * @param checkpointTotalCommitLength
     * @return
     */
    public static DurableLogConfig createDurableLogConfig(int checkpointMinCommitCount, int checkpointCommitCount, int checkpointTotalCommitLength) {
        Properties p = new Properties();
        ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, Integer.toString(checkpointMinCommitCount));
        ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_COMMIT_COUNT, Integer.toString(checkpointCommitCount));
        ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, Integer.toString(checkpointTotalCommitLength));
        return new DurableLogConfig(p);
    }

    /**
     * Creates a new instance of the ReadIndexConfig class with given arguments.
     *
     * @param minReadSize
     * @param maxReadSize
     * @return
     */
    public static ReadIndexConfig createReadIndexConfig(int minReadSize, int maxReadSize) {
        return createReadIndexConfig(minReadSize, maxReadSize, INFINITE_CACHE_POLICY);
    }

    /**
     * Creates a new instance of the ReadIndexConfig class with given arguments.
     *
     * @param minReadSize
     * @param maxReadSize
     * @param cachePolicy
     * @return
     */
    public static ReadIndexConfig createReadIndexConfig(int minReadSize, int maxReadSize, CachePolicy cachePolicy) {
        Properties p = new Properties();
        ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_STORAGE_READ_MIN_LENGTH, Integer.toString(minReadSize));
        ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_STORAGE_READ_MAX_LENGTH, Integer.toString(maxReadSize));

        int generationDuration = (int) cachePolicy.getGenerationDuration().toMillis();
        ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_CACHE_POLICY_MAX_SIZE, Long.toString(cachePolicy.getMaxSize()));
        ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_CACHE_POLICY_MAX_TIME, Integer.toString(cachePolicy.getMaxGenerations() * generationDuration));
        ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_CACHE_POLICY_GENERATION_TIME, Integer.toString(generationDuration));

        return new ReadIndexConfig(p);
    }

    /**
     * Creates a new instance of the WriterConfig class with given arguments.
     *
     * @param flushThresholdBytes
     * @param flushThresholdMillis
     * @return
     */
    public static WriterConfig createWriterConfig(int flushThresholdBytes, long flushThresholdMillis) {
        Properties p = new Properties();
        ServiceBuilderConfig.set(p, WriterConfig.COMPONENT_CODE, WriterConfig.PROPERTY_FLUSH_THRESHOLD_BYTES, Integer.toString(flushThresholdBytes));
        ServiceBuilderConfig.set(p, WriterConfig.COMPONENT_CODE, WriterConfig.PROPERTY_FLUSH_THRESHOLD_MILLIS, Long.toString(flushThresholdMillis));

        return new WriterConfig(p);
    }
}
