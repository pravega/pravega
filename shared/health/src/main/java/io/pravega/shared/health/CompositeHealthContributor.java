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
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public abstract class CompositeHealthContributor implements HealthContributor, Registry<HealthContributor>  {

    /**
     * The {@link StatusAggregator} used to perform the aggregation of all the {@link HealthContributor} dependencies.
     */
    private final StatusAggregator aggregator;

    private final Set<HealthContributor> contributors;

    CompositeHealthContributor() {
        this(StatusAggregatorImpl.UNANIMOUS);
    }

    CompositeHealthContributor(StatusAggregator aggregator) {
        this.aggregator = aggregator;
        this.contributors = new HashSet<>();
    }

    /**
     * Adds a {@link HealthContributor} as a dependency to this {@link HealthComponent}.
     *
     * @param contributor The {@link HealthContributor} to add.
     */
    public void register(HealthContributor contributor) {
        contributors.add(contributor);
    }

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

    /*
     Reminder: Fix risk of StackOverflow.
     */
    public Health health() {
        return health(false);
    }

    public Health health(boolean includeDetails) {
        // Fetch the Health Status of all dependencies.
        val children = contributors.stream()
                .map(contributor -> contributor.health(includeDetails))
                .collect(Collectors.toList());
        // Get the aggregate health status.
        Status status = aggregator.aggregate(children
                .stream()
                .map(contributor -> contributor.getStatus())
                .collect(Collectors.toList()));
        // Normalize the details object(s).
        ArrayList<Map.Entry<String, Object>> details = new ArrayList<>();
        if (includeDetails) {
            for (Health child : children) {
                details.add(new AbstractMap.SimpleImmutableEntry<>(child.getName(), child.getDetails()));
            }
        }
        return Health.builder().status(status).details(details).build();
    }

    public void clear() {
        contributors.clear();
    }

    public Optional<HealthContributor> get(String name) {
        return contributors.stream()
                .filter(val -> val.getName() == name)
                .findFirst();
    }


    abstract public String getName();
}
