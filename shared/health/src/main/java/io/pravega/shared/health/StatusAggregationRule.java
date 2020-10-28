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

import java.util.Collection;

/**
 * Any component that implements {@link HealthContributor} may depend on one or more contributors to determine
 * the true {@link Status} of itself. An aggregation rule gives components flexibility in determining it's overall health,
 * given the health of itself and its dependencies.
 *
 * Particularly in the case where a component has many instances of a particular service, it may tolerate a certain
 * number of failures before entering an unhealthy state.
 */
public interface StatusAggregationRule {
    /**
     * Returns a healthy {@link Status} if a majority of the statuses are healthy.
     *
     * @param statuses The statuses to aggregate.
     * @return A {@link Status} object describing the aggregate status.
     */
    Status majority(Collection<Status> statuses);

    /**
     * Returns a healthy {@link Status} if and only if *all* of the statuses are healthy.
     *
     * @param statuses The statuses to aggregate.
     * @return A {@link Status} object describing the aggregate status.
     */
    Status unanimous(Collection<Status> statuses);

    /**
     * Returns a healthy {@link Status} as long as any of the statuses are healthy.
     *
     * @param statuses The statuses to aggregate.
     * @return A {@link Status} object describing the aggregate status.
     */
    Status any(Collection<Status> statuses);
}
