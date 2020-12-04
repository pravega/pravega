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


import lombok.Getter;
import lombok.NonNull;

/**
 * Enumerates the list of potential states that a service or component may be in.
 */
public enum Status {
    /**
     * Describes a {@link Status} that is considered in a healthy and operational state.
     */
    UP(3),

    /**
     * Describes a {@link Status} that is considered to be in the process of starting, but not yet 'RUNNING'.
     */
    STARTING(2),

    /**
     * Describes the scenario where a {@link HealthContributor} has very recently been created, but not entered
     * a 'STARTING' phase.
     */
    NEW(1),

    /**
     * Describes a {@link Status} that is in an unknown state.
     */
    UNKNOWN(0),

    /**
     * Describes a {@link Status} that is considered in an unhealthy and non-operational state.
     */
    DOWN(-1);

    @Getter
    private final int code;

    Status(int code) {
        this.code = code;
    }

    /**
     * A {@link Status} represents an *alive* component if it is neither {@link Status#DOWN} nor {@link Status#UNKNOWN}.
     * @param status The reference {@link Status} object.
     * @return Whether this {@link Status} can be considered 'alive' or not.
     */
    @NonNull
    static boolean isAlive(Status status) {
        return status != DOWN && status != UNKNOWN;
    }

}
