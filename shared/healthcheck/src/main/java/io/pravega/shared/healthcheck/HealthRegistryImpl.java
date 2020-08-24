/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.healthcheck;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * HealthRegistry implementation that holds weak references to registered HealthUnits.
 * The registry also trigger the HealthInfoSupplier of all the registered HealthUnit, and then
 * apply aggregation on aspect level and global level before returns the final result.
 *
 */
@Slf4j
public class HealthRegistryImpl implements HealthRegistry {

    public static final int HEALTH_CHECK_TIMEOUT_SECONDS = 1;
    private static final AtomicReference<HealthRegistryImpl> INSTANCE = new AtomicReference<>();

    Map<HealthAspect, WeakHashMap<HealthUnit, HealthUnit>> aspectMap = new java.util.concurrent.ConcurrentHashMap<>();

    private HealthRegistryImpl() {
        for (HealthAspect aspect: HealthAspect.values()) {
            aspectMap.put(aspect, new java.util.WeakHashMap<>());
        }
    }

    /**
     * Initialization to construct an HealthRegistryImpl instance.
     */
    public static synchronized void init() {
        if (INSTANCE.get() == null) {
            INSTANCE.set(new HealthRegistryImpl());
            log.info("HealthRegistryImpl instance has been initialized: " + INSTANCE);
        }
    }

    /**
     * Get the initialized instance of HealthRegistryImpl.
     *
     * @return the instance of HealthRegistryImpl
     */
    public static HealthRegistryImpl getInstance() {
        Preconditions.checkState(INSTANCE.get() != null, "HealthRegistryImpl has not been instanced.");
        return INSTANCE.get();
    }

    /**
     * Only for testing purpose - create a new instance of HealthRegistryImpl in case test isolation is needed
     */
    @VisibleForTesting
    static synchronized void resetInstance() {
        INSTANCE.set(new HealthRegistryImpl());
        log.info("HealthRegistryImpl instance has been reset (WARNING: this can only be done by test code): " + INSTANCE);
    }

    @Override
    public void registerHealthUnit(HealthUnit unit) {
        Preconditions.checkArgument(unit != null, "HealthUnit cannot be null");
        synchronized (unit.getHealthAspect()) {
            aspectMap.get(unit.getHealthAspect()).put(unit, unit);
            log.info("Registered HealthUnit: " + unit);
        }
    }

    @Override
    public void unregisterHealthUnit(HealthUnit unit) {
        Preconditions.checkArgument(unit != null, "HealthUnit cannot be null");
        synchronized (unit.getHealthAspect()) {
            aspectMap.get(unit.getHealthAspect()).remove(unit);
            log.info("Unregistered HealthUnit: " + unit);
        }
    }

    /*
     * Conduct the actual health-check using the HealthInfo supplier provided by HealthUnit.
     * (Note no feature beyond Java 8)
     */
    private HealthInfo doHealthCheck(CompletableFuture<HealthInfo> future) {
        try {
            return future.get(HEALTH_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            return new HealthInfo(HealthInfo.Status.UNKNOWN, "Health-check timed-out");
        } catch (InterruptedException interruptedException) {
            return new HealthInfo(HealthInfo.Status.UNKNOWN, "Health-check interrupted: " + interruptedException);
        } catch (ExecutionException executionException) {
            return new HealthInfo(HealthInfo.Status.UNKNOWN, "Health-check exception: " + executionException);
        }
    }

    /**
     * Check Health for a specific HealthAspect.
     *
     * @param aspect - the HealthAspect to be checked
     * @return - the HealthInfo of the aspect
     */
    @VisibleForTesting
    Optional<HealthInfo> checkAspectHealth(HealthAspect aspect) {

        List<CompletableFuture<HealthInfo>> infoFutures = aspectMap.get(aspect).values()
                .stream()
                .map(unit -> CompletableFuture.supplyAsync(unit.getHealthInfoSupplier()))
                .collect(Collectors.toList());

        List<HealthInfo> infos = infoFutures.parallelStream()
                .map(future -> doHealthCheck(future))
                .collect(Collectors.toList());

        Optional<HealthInfo> aspectHealthInfo = aspect.getAspectAggregationRule().apply(infos);
        if (aspectHealthInfo.isPresent()) {
            StringBuilder details = new StringBuilder("\nHealth Aspect: " + aspect.getName() + "\n");
            details.append(aspectHealthInfo.get().getDetails());
            return Optional.of(new HealthInfo(aspectHealthInfo.get().getStatus(), details.toString()));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Public interface for HealthCheck.
     *
     * @return optional HealthInfo of the instance being health-checked.
     */
    public synchronized Optional<HealthInfo> checkHealth() {

        List<HealthInfo> aspectInfos = Arrays.stream(HealthAspect.values())
                .parallel()
                .map(aspect -> checkAspectHealth(aspect))
                .filter(optionalAspectInfo -> optionalAspectInfo.isPresent())
                .map(optionalAspectInfo -> optionalAspectInfo.get())
                .collect(Collectors.toList());

        return HealthInfoAggregationRules.oneVeto(aspectInfos);
    }
}
