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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.NonNull;
import lombok.val;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link AbstractHealthContributor} class defines the base logic required to build {@link HealthContributor} objects
 * that depend on (or are composed of) other {@link HealthContributor} objects. To do this we need to due two things:
 * reducing/reconciling the (potentially) many different {@link Status} states and a way to persist these relationships.
 *
 * {@link AbstractHealthContributor} objects are used *strictly* for grouping, meaning that they do not define their own
 * health checking logic and therefore does not export any details ({@link Health#getDetails}).
 */
@Slf4j
@ThreadSafe
public abstract class AbstractHealthContributor implements HealthContributor {

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
     * The most recent reported status, either by the {@link #doHealthCheck(Health.HealthBuilder)} or an explicit mutation.
     * Useful when there is a sudden failure and can provide a distinction between an expected closure/shutdown
     * and an abrupt 'failing' closure.
     */
    @Getter
    @Setter
    private Status status;
    /**
     * Flag indicating this contributor and all its children are no longer accessible.
     */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Holds the references to all child {@link HealthContributor}.
     */
    private final Map<String, HealthContributor> contributors = new ConcurrentHashMap<>();

    public AbstractHealthContributor(@NonNull String name) {
        this(name, StatusAggregator.UNANIMOUS);
    }

    public AbstractHealthContributor(@NonNull String name, @NonNull StatusAggregator aggregator) {
        this.name = name;
        this.aggregator = aggregator;
    }

    /**
     * Recursively build (in post-order fashion) the {@link Health} result for a given {@link HealthContributor}.
     *
     * @return The {@link Health} result of the {@link HealthContributor}.
     */
    @Override
    synchronized final public Health getHealthSnapshot() {
        Exceptions.checkNotClosed(isClosed(), this);

        Health.HealthBuilder builder = Health.builder().name(getName());
        Collection<Status> statuses = new ArrayList<>();
        Map<String, Health> children = new HashMap<>();

        for (val entry : contributors.entrySet()) {
            HealthContributor contributor = entry.getValue();
            synchronized (contributor) {
                if (!contributor.isClosed()) {
                    Health health = contributor.getHealthSnapshot();
                    children.put(entry.getKey(), health);
                    statuses.add(health.getStatus());
                } else {
                    contributors.remove(name);
                }
            }
        }

        Status status = Status.DOWN;
        // Perform own health check logic.
        try {
            status = doHealthCheck(builder);
        } catch (Exception ex) {
            log.warn("HealthCheck for {} has failed.", this.name, ex);
            builder.status(Status.FAILED);
        }
        // If there are no child statuses, return the Status from its own health check, else
        // return the least 'healthy' status between the child aggregate and its own.
        status = statuses.isEmpty() ? status : Status.min(StatusAggregator.aggregate(aggregator, statuses), status);
        this.status = status;

        return builder.name(name).status(status).children(children).build();
    }

    @Override
    synchronized final public void register(HealthContributor... children) {
        Exceptions.checkNotClosed(isClosed(), this);
        for (HealthContributor child : children) {
            if (child.getName().contains(DELIMITER)) {
                log.warn("The supplied HealthContributor contains the lookup delimiter ('{}') -- skipping.", DELIMITER);
                continue;
            }
            contributors.put(child.getName(), child);
        }
    }

    @Override
    synchronized public final void close() {
        if (!closed.getAndSet(true)) {
            for (val contributor : contributors.entrySet()) {
                contributor.getValue().close();
            }
            contributors.clear();
        }
    }

    @Override
    public final boolean isClosed() {
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
