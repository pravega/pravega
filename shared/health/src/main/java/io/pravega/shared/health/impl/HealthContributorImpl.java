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
package io.pravega.shared.health.impl;

import io.pravega.common.Exceptions;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.StatusAggregator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.NonNull;
import lombok.val;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link HealthContributorImpl} class defines the base logic required to build {@link HealthContributor} objects
 * that depend on (or are composed of) other {@link HealthContributor} objects. To do this we need to due two things:
 * reducing/reconciling the (potentially) many different {@link Status} states and a way to persist these relationships.
 *
 * {@link HealthContributorImpl} objects are used *strictly* for grouping, meaning that they do not define their own
 * health checking logic and therefore does not export any details ({@link Health#getDetails}).
 */
@Slf4j
@ThreadSafe
public abstract class HealthContributorImpl implements HealthContributor {

    /**
     * The {@link StatusAggregator} used to perform the aggregation of all the {@link HealthContributor} dependencies.
     */
    @Getter
    @NonNull
    private final StatusAggregator aggregator;

    /**
     * The identifier for this {@link HealthContributor}.
     */
    @Getter
    @NonNull
    private final String name;

    /**
     * Flag indicating this contributor and all its children are no longer accessible.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    private final Map<String, HealthContributor> contributors = new ConcurrentHashMap<>();

    public HealthContributorImpl(@NonNull String name) {
        this(name, StatusAggregatorImpl.UNANIMOUS);
    }

    public HealthContributorImpl(@NonNull String name, @NonNull StatusAggregator aggregator) {
        this.name = name;
        this.aggregator = aggregator;
    }

    /**
     * Recursively build (in post-order fashion) the {@link Health} result for a given {@link HealthContributor}.
     *
     * @return The {@link Health} result of the {@link HealthContributor}.
     */
    @Override
    synchronized public Health getHealthSnapshot() {
        Exceptions.checkNotClosed(closed.get(), this);

        Health.HealthBuilder builder = Health.builder().name(getName());
        Collection<Status> statuses = new ArrayList<>();
        Collection<Health> children = new ArrayList<>();

        for (val entry : contributors.entrySet()) {
            if (!entry.getValue().isClosed()) {
                Health health = entry.getValue().getHealthSnapshot();
                children.add(health);
                statuses.add(health.getStatus());
            } else {
                contributors.remove(name);
            }
        }

        Status status = Status.DOWN;
        // Perform own health check logic.
        try {
            status = doHealthCheck(builder);
        } catch (Exception ex) {
            log.warn("HealthCheck for {} has failed.", this.name, ex);
            builder.status(Status.DOWN);
        }
        // If there are no child statuses, return the Status from its own health check, else
        // return the least 'healthy' status between the child aggregate and its own.
        status = statuses.isEmpty() ? status : Status.min(aggregator.aggregate(statuses), status);

        return builder.name(name).status(status).children(children).build();
    }

    @Override
    synchronized public void register(HealthContributor... children) {
        Exceptions.checkNotClosed(isClosed(), this);
        for (HealthContributor child : children) {
            contributors.put(child.getName(), child);
        }
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            contributors.clear();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * The doHealthCheck(Health.HealthBuilder) method is the primary interface used by some client
     * to define the logic which determines the health status of a component.
     *
     * This method *must* define logic to assign the {@link Status} that best reflects the current state of the component.
     *
     * @param builder The {@link Health.HealthBuilder} object.
     * @throws Exception An exception to be thrown if the underlying health check fails.
     */
    public abstract Status doHealthCheck(Health.HealthBuilder builder) throws Exception;
}
