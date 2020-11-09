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


import lombok.Getter;

/**
 * Enumerates the list of potential states that a service or component may be in.
 */
public enum Status {
    /**
     * Describes a {@link Status} that is considered in a healthy and operational state.
     */
    UP(2),

    /**
     * Describes a {@link Status} that is considered to be in a (potentially) deteriorating state, but still
     * remains operational.
     */
    WARNING(1),

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
    static boolean alive(Status status) {
        return status != null && status != DOWN && status != UNKNOWN;
    }

}
