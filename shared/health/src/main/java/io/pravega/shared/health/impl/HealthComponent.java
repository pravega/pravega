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
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.StatusAggregationRule;
import io.pravega.shared.health.StatusAggregator;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * The {@link HealthComponent} class is used to provide a logical grouping of components. Each registered {@link  HealthComponent}
 * will then be exportable via a {@link HealthEndpoint}.
 *
 * The children/dependencies of a {@link HealthComponent} are used to determine the {@link Status} of this component, based
 * on some {@link StatusAggregationRule}.
 *
 * If a {@link HealthComponent} is registered under a {@link ContributorRegistry}, the {@link ContributorRegistry} should
 * (but is not currently a strict requirement) treat said {@link HealthComponent} as immutable, meaning that it should not
 * be removed (unregistered) once registered. *However* a {@link HealthComponent} can still be used to dynamically aggregate
 * various {@link HealthContributor}, so long as the component is registered as a {@link HealthContributor} and not its
 * native {@link HealthComponent} type.
 */
@Slf4j
public class HealthComponent extends CompositeHealthContributor {

    @Getter
    @NonNull
    private final String name;

    /**
     * Creates a new instance of the {@link HealthComponent} class.
     * @param name The name which this object is addressable by.
     * @param aggregator The {@link StatusAggregator} which defines the aggregation logic.
     * @param registry The {@link ContributorRegistry} which to register this component under.
     */
    @NonNull
    protected HealthComponent(String name, StatusAggregator aggregator, ContributorRegistry registry) {
        super(aggregator, registry);
        this.name = name;
    }

    @Override
    public String toString() {
        return String.format("HealthComponent::%s", this.name);
    }

}
