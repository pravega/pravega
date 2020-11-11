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

import io.pravega.shared.health.impl.StatusAggregatorImpl;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public abstract class CompositeHealthContributor implements HealthContributor, Registry<HealthContributor> {

    /**
     * The {@link StatusAggregator} used to perform the aggregation of all the {@link HealthContributor} dependencies.
     */
    private final StatusAggregator aggregator;

    @Getter
    private final Set<HealthContributor> contributors;

    CompositeHealthContributor() {
        this(StatusAggregatorImpl.UNANIMOUS);
    }

    CompositeHealthContributor(StatusAggregator aggregator) {
        this.contributors = new HashSet<>();
        this.aggregator = aggregator;
    }

    public Health health() {
        return health(false);
    }

    public Health health(boolean includeDetails) {
        // Fetch the Health Status of all dependencies.
        val children = contributors.stream()
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

    abstract public String getName();

    /**
     * Removes a {@link HealthContributor} as a dependency from this {@link HealthComponent}.
     *
     * @param contributor The {@link HealthContributor} to remove.
     */
    public void unregister(HealthContributor contributor) {
        if (!contributors.contains(contributor)) {
            log.warn("A request to remove {} failed. {} is not listed as a dependency.", contributor, contributor);
            return;
        }
        contributors.remove(contributor);
    }

    /**
     * Adds a {@link HealthContributor} as a dependency to this {@link HealthComponent}.
     *
     * @param contributor The {@link HealthContributor} to add.
     */
    public void register(HealthContributor contributor) {
        contributors.add(contributor);
    }

    public Optional<HealthContributor> get(String name) {
        return contributors.stream()
                .filter(val -> val.getName() == name)
                .findFirst();
    }

    public void clear() {
        contributors.clear();
    }
}
