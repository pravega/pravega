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

import java.util.stream.Collectors;

@Slf4j
public abstract class CompositeHealthContributor implements HealthContributor {

    /**
     * The {@link StatusAggregator} used to perform the aggregation of all the {@link HealthContributor} dependencies.
     */
    public final StatusAggregator aggregator;

    CompositeHealthContributor() {
        this(StatusAggregatorImpl.DEFAULT);
    }

    CompositeHealthContributor(StatusAggregator aggregator) {
        this.aggregator = aggregator;
    }

    public Health health() {
        return health(false);
    }

    public Health health(boolean includeDetails) {
        // Fetch the Health Status of all dependencies.
        val children =  registry()
                .get(getName())
                .stream()
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

    abstract public ContributorRegistry registry();

}
