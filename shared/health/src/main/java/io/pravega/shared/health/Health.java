/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.shared.health.impl.CompositeHealthContributor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The {@link Health} class represents the data gathered by a {@link HealthIndicator} after performing a health check.
 */
@Builder
@ThreadSafe
@AllArgsConstructor
public class Health {

    /**
     * Any sort of identifying string that describes from which component this measurement
     * was taken from.
     */
    @Getter
    @Builder.Default
    private final String name = "unknown";

    @Getter
    @Builder.Default
    private final Status status = Status.UNKNOWN;

    /**
     * {@link #ready} and {@link #alive} should not (for the moment) take a default value, as the distinction
     * between unset and false (not ready/alive) is important.
     */
    private final Boolean ready;

    private final Boolean alive;

    @Getter
    @Builder.Default
    private final Map<String, Object> details = ImmutableMap.of();

    /**
     * A {@link CompositeHealthContributor} may be composed of any number of child {@link HealthContributor}.
     */
    @Getter
    @Builder.Default
    private final List<Health> children = ImmutableList.of();

    /**
     * Used to perform readiness checks. It determines if the {@link Health} object holds a {@link Status} that is considered 'ready'.
     * A component is considered 'ready' if it has completed it's initialization step(s) and is ready to execute.
     *
     * Checks should be made to make sure {@link #isReady()} will not report a result if it is in a logically invalid state. Those include:
     * - ready is *true* if {@link Health#isAlive()} returns false.
     * - ready is *true* with a {@link Status} representing a 'weaker' {@link Status} than {@link Status#UNKNOWN}.

     * @return The readiness result.
     */
    public boolean isReady() {
        boolean isReady = Objects.isNull(ready) ? Status.isAlive(status) : ready;
        Preconditions.checkState(!(isReady && status.getCode() <= 0), "A Health object should not have a DOWN Status and a ready state.");
        Preconditions.checkState(!(isReady && !isAlive()), "A Health object can not be ready and not alive.");

        return isReady;
    }

    /**
     * Used to perform liveness checks. It determines if the {@link Health} object holds a {@link Status} that is considered 'alive'.
     * A component is considered 'alive' if it is able to perform it's expected duties.
     *
     * The distinction is not as useful within a single process, but could be used to show the state of some number of threads
     * backing the service. Otherwise, if the {@link Health} object describes the entire process, 'alive' describes whether
     * or not it is running.
     *
     * A component that is 'ready' implies that it is 'alive', but not vice versa.
     *
     * Checks should be made to make sure {@link #isAlive} will not report a result if it is in a logically invalid state. Those include:
     * - alive is *false* if {@link Health#isReady()} returns *true*.
     * - alive is *true* with a {@link Status} representing a 'weaker/equivalent' {@link Status} than {@link Status#UNKNOWN}.
     * - alive is *false* with a {@link Status} representing a 'strictly stronger' {@link Status} than {@link Status#UNKNOWN}.
     *
     * @return The liveness result.
     */
    public boolean isAlive() {
        boolean isAlive = Objects.isNull(alive) ? Status.isAlive(status) : alive;
        Preconditions.checkState(!(isAlive && status.getCode() <= 0), "A Health object should not have a DOWN Status and be marked alive.");
        Preconditions.checkState(!(!isAlive && status.getCode() > 0), "A Health object should not have an UP Status and be marked non-alive.");
        Preconditions.checkState(!(!isAlive && isReady()), "A Health object can not be ready but not alive.");

        return isAlive;
    }
}
