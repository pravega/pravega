/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health.impl;

import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.StatusAggregator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public abstract class CompositeHealthContributor implements HealthContributor {

    /**
     * The {@link StatusAggregator} used to perform the aggregation of all the {@link HealthContributor} dependencies.
     */
    @Getter
    private final StatusAggregator aggregator;

    private final ContributorRegistry registry;

    private final Collection<HealthContributor> contributors = new HashSet<>();

    CompositeHealthContributor(StatusAggregator aggregator, ContributorRegistry registry) {
        this.aggregator = aggregator;
        this.registry = registry;
    }

    public Health getHealthSnapshot() {
        return getHealthSnapshot(false);
    }

    /**
     * Performs a {link Health} check on *all* descendant {@link HealthContributor}. The 'includeDetails' flag
     * has different semantics depending on the type of sub-class.
     *
     * {@link io.pravega.shared.health.HealthIndicator} object will be requested to fetch their {@link io.pravega.shared.health.Details},
     * while {@link HealthComponent} will provide the {@link Health} results for all it's descendants.
     *
     * @param includeDetails Whether or not to request {@link io.pravega.shared.health.Details} from each of the {@link HealthContributor}.
     * @return The {@link Health} result of the {@link CompositeHealthContributor}.
     */
    public Health getHealthSnapshot(boolean includeDetails) {
        Health.HealthBuilder builder = Health.builder().name(getName());
        Collection<Status> statuses = new ArrayList<>();
        // Fetch the Health Status of all dependencies.
        val children =  contributors().stream()
                .filter(Objects::nonNull)
                .map(contributor -> {
                    Health health = contributor.getHealthSnapshot(includeDetails);
                    statuses.add(health.getStatus());
                    if (health.getStatus() == Status.UNKNOWN) {
                        log.warn("{} has a Status of 'UNKNOWN'. This indicates `doHealthCheck` does not set a status" +
                                " or is an empty HealthComponent.", health.getName());
                    }
                    return health;
                })
                .collect(Collectors.toList());
        // Get the aggregate health status.
        Status status = aggregator.aggregate(statuses);

        return builder.status(status).children(includeDetails ? children : Collections.emptyList()).build();
    }

    /**
     * A method which supplies the {@link CompositeHealthContributor#getHealthSnapshot(boolean)} method with the collection of
     * {@link HealthContributor} objects to perform the aggregate health check on. This is helpful because it gives us
     * flexibility in defining where the contributors may be but also avoids the requirement of being bound to a
     * {@link ContributorRegistry} instance.
     *
     * @return The {@link Collection} of {@link HealthContributor}.
     */
    public Collection<HealthContributor> contributors() {
        if (registry != null) {
            return registry.dependencies(getName());
        }
        return this.contributors;
    }

    abstract public String getName();

}
