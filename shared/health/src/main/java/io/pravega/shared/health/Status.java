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


import lombok.NonNull;

/**
 * Enumerates the list of potential states that a service or component may be in.
 */
public final class Status {

    /**
     * Describes a {@link Status} that is considered in a healthy and operational state.
     */
    public static final Status UP = new Status("UP");

    /**
     * Describes a {@link Status} that is considered in an unhealthy and non-operational state.
     */
    public static final Status DOWN = new Status("DOWN");

    /**
     * Describes a {@link Status} that is considered to be in a (potentially) deteriorating state, but still
     * remains operational.
     */
    public static final Status WARNING = new Status("WARNING");

    /**
     * Describes a {@link Status} that is in an unknown state.
     */
    public static final Status UNKNOWN = new Status("UNKNOWN");

    private final String code;

    /**
     * Generates a {@link Status} object with the given code.
     *
     * @param code The string representation of the code.
     */
    @NonNull
    public Status(String code) {
        this.code = code;
    }

    /**
     * Returns the code for this {@link Status}.
     * @return The code.
     */
    public String getCode() {
        return this.code;
    }

}
