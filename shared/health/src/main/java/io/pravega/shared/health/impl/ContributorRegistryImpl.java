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

@Slf4j
public class ContributorRegistryImpl implements ContributorRegistry {

    /**
     * A {@link Map} that maintains references to all {@link HealthContributor} objects.
     */
    @Getter
    final Map<String, HealthComponent> components;

    final Set<HealthContributor> contributors;

    /**
     * The default {@link HealthComponent} to register any {@link HealthContributor} under.
     */
    private final HealthComponent root;

    ContributorRegistryImpl(HealthComponent root) {
        this.root = root;
        this.components = new HashMap<>();
        this.contributors = new HashSet<>();
    }

    @Override
    @NonNull
    public void register(HealthContributor contributor) {
        register(contributor, root);
    }

    @NonNull
    public void register(HealthContributor contributor, HealthComponent parent) {
        // Make sure each HealthContributor/Component exists before attempting to use them.
        this.components.putIfAbsent(parent.getName(), parent);
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
        if (!contributors.contains(name)) {
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
        components.clear();
        contributors.clear();
    }
}
