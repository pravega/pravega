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

import com.google.common.base.Preconditions;

import java.time.Duration;

/**
 * Represents a Policy for a CacheManager.
 */
public class CachePolicy {
    //region Members

    private final long maxSize;
    private final int maxGenerations;
    private final Duration generationDuration;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CachePolicy class.
     *
     * @param maxSize            The maximum size of the cache.
     * @param maxTime            The maximum amount of time a cache entry can live in the cache.
     * @param generationDuration The amount of time one Cache generation spans.
     */
    public CachePolicy(long maxSize, Duration maxTime, Duration generationDuration) {
        Preconditions.checkArgument(maxSize > 0, "maxSize must be a positive integer");
        this.maxSize = maxSize;
        this.generationDuration = generationDuration;
        this.maxGenerations = Math.max(1, (int) ((double) maxTime.toMillis() / generationDuration.toMillis()));
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the maximum size of the cache.
     *
     * @return The value.
     */
    public long getMaxSize() {
        return this.maxSize;
    }

    /**
     * Gets a value indicating the maximum number of generations a cache entry can be inactive in the cache for, before
     * being eligible for eviction.
     *
     * @return The value.
     */
    public int getMaxGenerations() {
        return this.maxGenerations;
    }

    /**
     * Gets the amount of time a particular Cache generation lasts.
     *
     * @return The value.
     */
    public Duration getGenerationDuration() {
        return this.generationDuration;
    }

    @Override
    public String toString() {
        return String.format("MaxSize = %d, MaxGen = %d, Generation = %s", this.maxSize, this.maxGenerations, this
                .generationDuration);
    }

    //endregion
}
