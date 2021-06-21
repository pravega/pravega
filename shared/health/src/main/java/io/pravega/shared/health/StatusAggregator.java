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

import java.util.Collection;
import java.util.function.Function;

public enum StatusAggregator {

    /**
     * Publicizes a {@link StatusAggregator} using the '{@link StatusAggregator#majority(Collection)}' rule.
     */
    MAJORITY(StatusAggregator::majority),

    /**
     * Publicizes a {@link StatusAggregator} using the '{@link StatusAggregator#unanimous(Collection)}' rule.
     */
    UNANIMOUS(StatusAggregator::unanimous),

    /**
     * Publicizes a {@link StatusAggregator} using the '{@link StatusAggregator#any(Collection)}' rule.
     */
    ANY(StatusAggregator::any);

    /**
     * The underlying aggregation rule.
     */
    private final Function<Collection<Status>, Status> rule;

    /**
     * In the case where a custom aggregation rule is required, any lambda function reducing a {@link Collection} of
     * {@link Status} to a single {@link Status} may be supplied.
     * @param rule The rule used to perform the aggregation.
     */
    StatusAggregator(Function<Collection<Status>, Status> rule) {
        this.rule = rule;
    }

    /**
     * Returns a healthy {@link Status} if a strict majority of the statuses are healthy. For a majority to occur,
     * more than half of the {@link Status} objects must report a healthy state.
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
            isAliveStatus += status.isAlive() ? 1 : -1;
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

    public static Status aggregate(StatusAggregator aggregator, Collection<Status> statuses) {
        return aggregator.rule.apply(statuses);
    }
}
