/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
     * Describes a {@link Status} that is in a down and non-operational state.
     */
    DOWN(-1),

    /**
     * Describes a {@link Status} that is unhealthy and has unexpectedly failed.
     */
    FAILED(-2);

    @Getter
    private final int code;

    private Status(int code) {
        this.code = code;
    }

    /**
     * A {@link Status} represents an *alive* component if it is neither {@link Status#DOWN} nor {@link Status#UNKNOWN}.
     * @return Whether this {@link Status} represents an 'alive' state.
     */
    public boolean isAlive() {
        return this.code >= NEW.getCode();
    }

    /**
     * A {@link Status} can be considered 'ready' if it is {@link Status#UP}.
     * @return Whether this {@link Status} represents a 'ready' state.
     */
    public boolean isReady() {
        return this.code >= UP.getCode();
    }

    public static Status min(Status one, Status two) {
        return one.getCode() < two.getCode() ? one : two;
    }
}
