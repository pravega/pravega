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
import io.pravega.shared.health.HealthComponent;
import io.pravega.shared.health.HealthContributor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class ContributorRegistryImpl implements ContributorRegistry {

    /**
     * A {@link Map} that maintains the 'logical' relationships between a particular {@link HealthComponent} and all
     * of the required dependencies used to determine it's overall health.
     */
    Map<HealthComponent, Set<HealthContributor>> contributors;

    ContributorRegistryImpl() {
        contributors = new HashMap<>();
    }

    @Override
    @NonNull
    public void register(HealthContributor contributor) {
        register(contributor, HealthComponent.ROOT);
    }

    @NonNull
    @Override
    public void register(HealthContributor contributor, HealthComponent parent) {
        contributors.get(parent).add(contributor);
    }

    @Override
    @NonNull
    public void unregister(HealthContributor contributor) {
        if (contributors.get(contributor).isEmpty()) {
            log.warn("A request to unregister {} failed -- not found in the registry.", contributor);
        }
    }

    public void clear() {
        contributors.clear();
    }
}
