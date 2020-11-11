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

import io.pravega.shared.health.HealthComponent;
import io.pravega.shared.health.HealthComponentConfig;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.ContributorRegistry;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class ContributorRegistryImpl implements ContributorRegistry {

    /**
     * Maintains a *flattened* {@link Map} of all {@link HealthComponent} objects to avoid following references.
     */
    @Getter
    final Map<String, HealthComponent> components;

    /**
     * A {@link Set} that maintains references to all {@link HealthContributor} objects. This {@link Set} is kept for
     * de-registration purposes.
     */
    final Set<HealthContributor> contributors = new HashSet<>();

    /**
     * The default {@link HealthComponent} to register any {@link HealthContributor} under.
     */
    private final HealthComponent root;

    @NonNull
    ContributorRegistryImpl(HealthComponent root) {
        this.root = root;
        this.components = new HashMap<>();
        this.components.put(root.getName(), root);
    }

    ContributorRegistryImpl(HealthComponent root, HealthComponentConfig config) {
        this.root = root;
        this.components = config.getComponents();
        // Anchor the parent-less components at this ROOT component.
        this.root.setContributors(components.entrySet()
                .stream()
                .map(entry -> entry.getValue())
                .filter(component -> component.isRoot())
                .collect(Collectors.toSet()));
        // Put the configured ROOT component into the set.
        this.components.put(root.getName(), root);
    }

    @Override
    @NonNull
    public void register(HealthContributor contributor) {
        register(contributor, root);
    }

    @NonNull
    public void register(HealthContributor contributor, HealthComponent parent) {
        // A HealthComponent should only exist if defined during construction, instead of adding it dynamically.
        if (!components.containsKey(parent.getName())) {
            log.warn("Attempting to register {} under an unrecognized HealthComponent {}.", contributor, parent.getName());
            return;
        }
        log.info("Registering {} to the {}.", contributor, parent);
        // HealthComponent -> Set { HealthContributor }
        parent.register(contributor);
        // HealthContributor -> Set { HealthComponent }
        contributors.add(parent);
        contributors.add(contributor);
    }

    @Override
    @NonNull
    public void unregister(HealthContributor contributor) {
        String name = contributor.getName();
        if (!contributors.contains(contributor)) {
            log.warn("A request to unregister {} failed -- not found in the registry.", contributor);
            return;
        }
        if (components.containsKey(name)) {
            log.warn("Attempting to remove a HealthComponent -- this action should not normally be done.");
            components.remove(contributor);
        }
        contributors.remove(contributor);
    }

    public Optional<HealthContributor> get(String name) {
        return Optional.ofNullable(components.get(name));
    }

    public void clear() {
        root.clear();
        components.clear();
        contributors.clear();
    }
}
