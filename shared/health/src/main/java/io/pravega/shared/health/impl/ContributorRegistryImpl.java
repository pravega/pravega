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
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class ContributorRegistryImpl implements ContributorRegistry {

    /**
     * A {@link Map} that maintains the 'logical' relationships between a particular {@link HealthComponent} and all
     * of the required dependencies used to determine it's overall health.
     */
    Map<String, HealthComponent> components;
    /**
     * A {@link Map} that maintains the 'logical' relationships between a particular {@link HealthContributor} and all
     * of the components that act as a dependee to this {@link HealthContributor}.
     *
     * The {@link HealthComponent} maintains the references to it's dependencies, while the dependencies ({@link HealthContributor}
     * do not maintain references to their dependees.
     */
    Map<HealthContributor, Set<HealthComponent>> contributors;

    ContributorRegistryImpl() {
        components = new HashMap<>();
        contributors = new HashMap<>();
    }

    @Override
    @NonNull
    public void register(HealthContributor contributor) {
        register(contributor, HealthComponent.ROOT);
    }

    @NonNull
    public void register(HealthContributor contributor, HealthComponent parent) {
        log.info("Registering {} to the {}.", contributor, parent);
        // HealthComponent -> Set { HealthContributor }
        if (!components.containsKey(parent.getName())) {
            components.put(parent.getName(), parent);
        }
        parent.register(contributor);
        // HealthContributor -> Set { HealthComponent }
        if (!contributors.containsKey(contributor)) {
            contributors.put(contributor, new HashSet<>());
        }
        contributors.get(contributor).add(parent);
    }

    @Override
    @NonNull
    public void unregister(HealthContributor contributor) {
        if (contributors.get(contributor).isEmpty()) {
            log.warn("A request to unregister {} failed -- not found in the registry.", contributor);
        }
        // Remove from each HealthComponent.
        for (HealthComponent component : contributors.get(contributor)) {
            component.unregister(contributor);
        }
        // Remove HealthContributor -> Set { HealthComponent } mapping.
        contributors.remove(contributor);
    }

    public Optional<HealthContributor> get(String name) {
        return Optional.ofNullable(components.get(name));
    }

    public void clear() {
        components.clear();
    }
}
