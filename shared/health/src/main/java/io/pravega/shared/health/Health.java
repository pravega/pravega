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

import io.pravega.shared.health.impl.CompositeHealthContributor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Collection;
import java.util.Collections;
/**
 * The {@link Health} interface represents the data gathered by a {@link HealthIndicator} after performing a health check.
 */
@Builder
@AllArgsConstructor
public class Health {

    /**
     * Any sort of identifying string that describes from which component this measurement
     * was taken from.
     */
    @Getter
    public final String name;

    @Getter
    @Builder.Default
    private Status status = Status.UNKNOWN;

    /**
     * {@link Health#ready} and {@link Health#alive} should not (for the moment) take a default value, as the distinction
     * between unset and false (not ready/alive) is important.
     */
    private Boolean ready;

    private Boolean alive;

    @Getter
    @Builder.Default
    private Details details = new Details();

    /**
     * A {@link CompositeHealthContributor} may be composed of any number of child {@link HealthContributor}.
     */
    @Getter
    @Builder.Default
    private Collection<Health> children = Collections.emptyList();

    /**
     * Used to perform readiness checks. It determines if the {@link Health} object holds a {@link Status} that is considered 'ready'.
     * A component is considered 'ready' if it has completed it's initialization step(s) and is ready to execute.
     *
     * Checks should be made to make sure {@link Health#isReady()} will not report a result if it is in a logically invalid state. Those include:
     * - ready is *true* if {@link Health#isAlive()} returns false.
     * - ready is *true* with a {@link Status} representing a 'weaker' {@link Status} than {@link Status#UNKNOWN}.

     * @return
     */
    public boolean isReady() {
        if (ready == null) {
            ready = Status.alive(status);
        }
        if (ready && status.getCode() <= 0) {
            throw new RuntimeException("A Health object should not logically be in an UNKNOWN/DOWN (Status) and a ready state.");
        }
        if (ready && !isAlive()) {
            throw new RuntimeException("A Health object can not be ready and not alive.");
        }
        return ready;
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
     * Checks should be made to make sure {@link Health#isAlive} will not report a result if it is in a logically invalid state. Those include:
     * - alive is *false* if {@link Health#isReady()} returns *true*.
     * - alive is *true* with a {@link Status} representing a 'weaker/equivalent' {@link Status} than {@link Status#UNKNOWN}.
     * - alive is *false* with a {@link Status} representing a 'strictly stronger' {@link Status} than {@link Status#UNKNOWN}.
     *
     * @return
     */
    public boolean isAlive() {
        if (alive == null) {
            alive = Status.alive(status);
        }
        // Can't (logically) be marked both in an 'Alive' state and in a 'DOWN/UNKNOWN' state.
        if (alive && status.getCode() <= 0) {
            throw new RuntimeException("A Health object should not logically be in an UNKNOWN/DOWN (Status) and an alive state.");
        }
        // Can't (logically) be marked both in a non-alive state and in a 'UP/WARNING' state.
        if (!alive && status.getCode() > 0) {
            throw new RuntimeException("A Health object should not logically be in an UP/WARNING(Status) and a non-alive state.");
        }
        if (!alive && isReady()) {
            throw new RuntimeException("A Health object can not be ready but not alive.");
        }
        return alive;
    }
}
