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
    public static final CachePolicy INFINITE = new CachePolicy(Long.MAX_VALUE, 1.0, 1.0, Duration.ofSeconds(Integer.MAX_VALUE), Duration.ofSeconds(Integer.MAX_VALUE));
    /**
     * Unless specified via the constructor, defines the default value for {@link #getMaxUtilization()}.
     */
    public static final double DEFAULT_TARGET_UTILIZATION = 0.85;
    /**
     * Unless specified via the constructor, defines the default value for {@link #getMaxUtilization()}.
     */
    public static final double DEFAULT_MAX_UTILIZATION = 0.98;
    /**
     * The maximum size of the cache.
     */
    @Getter
    private final long maxSize;
    /**
     * The target utilization of the cache, expressed as a number between 0.0 and 1.0, representing the ratio of the
     * used cache to {@link #getMaxSize()}. The CacheManager will attempt to keep the cache at or below this target,
     * and incoming requests should be expected to be throttled after reaching this limit.
     */
    @Getter
    private final double targetUtilization;
    /**
     * The maximum utilization of the cache, expressed as a number between 0.0 and 1.0, representing the ratio of the
     * used cache to {@link #getMaxSize()}. It is expected that full throttling will apply after reaching this limit.
     */
    @Getter
    private final double maxUtilization;
    /**
     * The maximum usable size of the cache. When the cache reaches or exceeds this threshold, cache eviction will kick in.
     * This is a pre-calculated value of {@link #getMaxSize()} * {@link #getTargetUtilization()} ()}.
     */
    @Getter
    private final long evictionThreshold;
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
     * Creates a new instance of the CachePolicy class with TargetUtilization set to {@link #DEFAULT_TARGET_UTILIZATION}
     * and MaxUtilization set to {@link #DEFAULT_MAX_UTILIZATION}.
     *
     * @param maxSize            The maximum size of the cache.
     * @param maxTime            The maximum amount of time a cache entry can live in the cache.
     * @param generationDuration The amount of time one Cache generation spans.
     */
    public CachePolicy(long maxSize, Duration maxTime, Duration generationDuration) {
        this(maxSize, DEFAULT_TARGET_UTILIZATION, DEFAULT_MAX_UTILIZATION, maxTime, generationDuration);
    }

    /**
     * Creates a new instance of the CachePolicy class.
     *
     * @param maxSize            The maximum size of the cache.
     * @param targetUtilization  The target cache utilization to set. See {@link #getTargetUtilization()} ()}.
     * @param maxUtilization     The maximum cache utilization to set. See {@link #getMaxUtilization()}.
     * @param maxTime            The maximum amount of time a cache entry can live in the cache.
     * @param generationDuration The amount of time one Cache generation spans.
     */
    public CachePolicy(long maxSize, double targetUtilization, double maxUtilization, Duration maxTime, Duration generationDuration) {
        Preconditions.checkArgument(maxSize > 0, "maxSize must be a positive integer");
        Preconditions.checkArgument(targetUtilization > 0 && targetUtilization <= 1.0,
                "maxUtilization must be a number in the range (0.0, 1.0].");
        Preconditions.checkArgument(maxUtilization >= targetUtilization && maxUtilization <= 1.0,
                "maxUtilization must be a number in the range (0.0, 1.0], at least equal to targetUtilization(%s).", targetUtilization);
        this.maxSize = maxSize;
        this.targetUtilization = targetUtilization;
        this.maxUtilization = maxUtilization;
        this.evictionThreshold = (long) Math.floor(this.maxSize * this.targetUtilization);
        this.generationDuration = generationDuration;
        this.maxGenerations = Math.max(1, (int) ((double) maxTime.toMillis() / generationDuration.toMillis()));
    }

    //endregion

    @Override
    public String toString() {
        return String.format("MaxSize = %d, UsableSize = %d, MaxGen = %d, Generation = %s",
                this.maxSize, this.evictionThreshold, this.maxGenerations, this.generationDuration);
    }
}
