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

import java.util.Collection;

/**
 * Any component that implements {@link HealthIndicator} may depend on one or more contributors to determine
 * its own {@link Status}. An aggregation rule gives components flexibility in determining it's overall health,
 * given the health of itself and its dependencies.
 *
 * Particularly in the case where a component has many instances of a particular service, it may tolerate a certain
 * number of failures before entering an unhealthy state.
 *
 * A {@link Status} will only reduce to an {@link Status#UNKNOWN} state *if* it has no dependencies. A {@link HealthContributor}
 * that has dependencies signifies that it is now provided with something that (in the abstract) should provide information
 * about it's {@link Health}.
 */
public class StatusAggregationRule {
    /**
     * Returns a healthy {@link Status} if a majority of the statuses are healthy.
     *
     * @param statuses The statuses to aggregate.
     * @return A {@link Status} object describing the aggregate status.
     */
    public static Status majority(Collection<Status> statuses) {
        if (statuses.isEmpty()) {
            return Status.UNKNOWN;
        }
        int isAliveStatus = 0;
        // Should be a strict majority.
        for (Status status : statuses) {
            isAliveStatus = Status.isAlive(status) ? isAliveStatus + 1 : isAliveStatus - 1;
        }
        return isAliveStatus > 0 ? Status.UP : Status.DOWN;
    }

    /**
     * Returns a healthy {@link Status} if and only if *all* of the statuses are healthy.
     *
     * @param statuses The statuses to aggregate.
     * @return A {@link Status} object describing the aggregate status.
     */
    public static Status unanimous(Collection<Status> statuses) {
        if (statuses.isEmpty()) {
            return Status.UNKNOWN;
        }
        if (statuses.stream().allMatch(Status::isAlive)) {
            return Status.UP;
        }
        return Status.DOWN;
    }

    /**
     * Returns a healthy {@link Status} as long as any of the statuses are healthy.
     *
     * @param statuses The statuses to aggregate.
     * @return A {@link Status} object describing the aggregate status.
     */
    public static Status any(Collection<Status> statuses) {
        if (statuses.isEmpty()) {
            return Status.UNKNOWN;
        }
        if (statuses.stream().anyMatch(Status::isAlive)) {
            return Status.UP;
        }
        return Status.DOWN;
    }
}
