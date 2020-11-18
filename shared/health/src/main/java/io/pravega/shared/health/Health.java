/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

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

    private Boolean ready;

    private Boolean alive;

    @Getter
    @Builder.Default
    private Collection<Details.Result> details = Collections.emptyList();

    /**
     * A {@link CompositeHealthContributor} may be composed of any number of child {@link HealthContributor}.
     */
    @Getter
    @Builder.Default
    private Collection<Health> children = Collections.emptyList();

    Health(HealthBuilder builder) {
        this.name = builder.name;
        this.ready = builder.ready;
        this.alive = builder.alive;
    }

    /**
     * Used to perform readiness checks. It determines if the {@link Health} object holds a {@link Status} that is considered 'ready'.
     * A component is considered 'ready' if it has completed it's initialization step(s) and is ready to execute.
     *
     * @return
     */
    public boolean isReady() {
        if (ready == null) {
            return Status.alive(status);
        }
        return this.ready;
    }

    /**
     * Used to perform liveness checks. It determines if the {@link Health} object holds a {@link Status} that is considered 'alive'.
     * A component is considered 'alive' if it is able to perform it's expected duties.
     *
     * The distinction is not as useful within a single process, but might mean that there exists some number of threads
     * backing the service. Otherwise, if the {@link Health} object describes the entire process, 'alive' describes whether
     * or not it is running.
     *
     * A component that is 'ready' implies that it is 'alive', but not vice versa.
     *
     * @return
     */
    public boolean isAlive() {
        if (alive == null) {
            return Status.alive(status);
        }
        return this.alive;
    }
}
