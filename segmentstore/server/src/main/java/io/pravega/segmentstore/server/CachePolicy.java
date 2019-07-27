/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import com.google.common.base.Preconditions;
import java.time.Duration;
import lombok.Getter;

/**
 * Represents a Policy for a CacheManager.
 */
public class CachePolicy {
    //region Members
    public static final CachePolicy INFINITE = new CachePolicy(Long.MAX_VALUE, 1.0, Duration.ofSeconds(Integer.MAX_VALUE), Duration.ofSeconds(Integer.MAX_VALUE));
    /**
     * Unless specified via the constructor, defines the default value for {@link #getMaxUtilization()}.
     */
    public static final double DEFAULT_MAX_CACHE_UTILIZATION = 0.98;
    /**
     * Adjusts the {@link #getMaxUtilization()} in order to calculate the maximum usable size. Since both the Cache Manager
     * and the Operation Processor's Throttler are pegged to this value, we want to instruct the Cache Manager to attempt
     * evicting the cache before the Throttler reaches its maximum limit.
     *
     * For example, if MaxUtilization = 0.98 and the adjustment is -0.08, the Cache Manager will attempt to keep the Cache
     * at or below 0.9 of its maximum allowed size ({@link #getMaxSize()}). However the Throttler will only apply maximum
     * throttling when the cache reaches 0.98 (so if the Cache Manager is successful, it should not never reach that spot).
     */
    private static final double USABLE_RATIO_ADJUSTMENT = -0.08;
    /**
     * The maximum size of the cache.
     */
    @Getter
    private final long maxSize;
    /**
     * The maximum utilization of the cache, expressed as a number between 0.0 and 1.0, representing the ratio of the
     * used cache to {@Link #getMaxSize()}.
     */
    @Getter
    private final double maxUtilization;
    /**
     * The maximum usable size of the cache. When the cache reaches or exceeds this threshold, cache eviction will kick in.
     * This is a pre-calculated value of {@link #getMaxSize()} * ({@link #getMaxUtilization()} - {@link #USABLE_RATIO_ADJUSTMENT}).
     */
    @Getter
    private final long maxUsableSize;
    /**
     * The maximum number of generations a cache entry can be inactive in the cache for, before being eligible for eviction.
     */
    @Getter
    private final int maxGenerations;
    /**
     * The amount of time a particular Cache generation lasts.
     */
    @Getter
    private final Duration generationDuration;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CachePolicy class with MaxUtilization set to {@link #DEFAULT_MAX_CACHE_UTILIZATION}.
     *
     * @param maxSize            The maximum size of the cache.
     * @param maxTime            The maximum amount of time a cache entry can live in the cache.
     * @param generationDuration The amount of time one Cache generation spans.
     */
    public CachePolicy(long maxSize, Duration maxTime, Duration generationDuration) {
        this(maxSize, DEFAULT_MAX_CACHE_UTILIZATION, maxTime, generationDuration);
    }

    /**
     * Creates a new instance of the CachePolicy class.
     *
     * @param maxSize            The maximum size of the cache.
     * @param maxUtilization     The maximum cache utilization to set. See {@link #getMaxUtilization()}.
     * @param maxTime            The maximum amount of time a cache entry can live in the cache.
     * @param generationDuration The amount of time one Cache generation spans.
     */
    public CachePolicy(long maxSize, double maxUtilization, Duration maxTime, Duration generationDuration) {
        Preconditions.checkArgument(maxSize > 0, "maxSize must be a positive integer");
        Preconditions.checkArgument(maxUtilization > 0 && maxUtilization <= 1.0, "maxUtilization must be a number in the range (0.0, 1.0].");
        this.maxSize = maxSize;
        this.maxUtilization = maxUtilization;
        this.maxUsableSize = (long) Math.floor(this.maxSize * this.maxUtilization);
        this.generationDuration = generationDuration;
        this.maxGenerations = Math.max(1, (int) ((double) maxTime.toMillis() / generationDuration.toMillis()));
    }

    //endregion

    @Override
    public String toString() {
        return String.format("MaxSize = %d, UsableSize = %d, MaxGen = %d, Generation = %s",
                this.maxSize, this.maxUsableSize, this.maxGenerations, this.generationDuration);
    }
}
