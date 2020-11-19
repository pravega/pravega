/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health.impl;

import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.Registry;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.StatusAggregator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

@Slf4j
public abstract class CompositeHealthContributor implements HealthContributor, Registry<HealthContributor> {

    /**
     * The {@link StatusAggregator} used to perform the aggregation of all the {@link HealthContributor} dependencies.
     */
    private final StatusAggregator aggregator;

    private ContributorRegistry registry = null;

    private Collection<HealthContributor> contributors = new HashSet<>();

    CompositeHealthContributor() {
        this(StatusAggregatorImpl.DEFAULT, new HashSet<>());
    }

    CompositeHealthContributor(StatusAggregator aggregator, ContributorRegistry registry) {
        this.aggregator = aggregator;
        this.registry = registry;
    }

    CompositeHealthContributor(StatusAggregator aggregator, Collection<HealthContributor> contributors) {
        this.aggregator = aggregator;
        this.contributors = contributors;
    }

    public Health health() {
        return health(false);
    }

    public Health health(boolean includeDetails) {
        // Fetch the Health Status of all dependencies.
        val children =  contributors().stream()
                .filter(contributor -> contributor != null)
                .map(contributor -> {
                    Health health = contributor.health(includeDetails);
                    if (health.getStatus() == Status.UNKNOWN) {
                        log.warn("{} has a Status of 'UNKNOWN', your `doHealthCheck` method may not define the Status logic.", health.getName());
                    }
                    return health;
                })
                .collect(Collectors.toList());
        // Get the aggregate health status.
        Status status = aggregator.aggregate(children
                .stream()
                .map(contributor -> contributor.getStatus())
                .collect(Collectors.toList()));

        return Health.builder().status(status).children(includeDetails ? children : null).build();
    }

    public Collection<HealthContributor> contributors() {
        if (registry != null) {
            return registry.dependencies(getName());
        }
        return this.contributors;
    }

    abstract public String getName();

}
