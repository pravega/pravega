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
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class ContributorRegistryImpl implements ContributorRegistry {

    private static final String ROOT_HEALTH_CONTRIBUTOR = "";

    /**
     * Maintains a *flattened* {@link Map} of all {@link HealthComponent} objects to avoid following references.
     */
    private final Map<String, HealthContributor> contributors = new HashMap<>();

    /**
     * A {@link Set} that maintains references to all {@link HealthContributor} objects. This {@link Set} is kept for
     * de-registration purposes.
     */
    private final Map<String, Set<HealthContributor>> relations = new HashMap<>();

    /**
     * The base under which all other {@link HealthComponent} will be registered.
     */

    @NonNull
    ContributorRegistryImpl() {
        relations.put(ROOT_HEALTH_CONTRIBUTOR, new HashSet<>());
    }

    @NonNull
    @Override
    public void register(HealthContributor contributor) {
        register(contributor, ROOT_HEALTH_CONTRIBUTOR);
    }

    public void register(HealthContributor contributor, String parent) {
        // A null string should map to the default/root HealthContributor.
        parent = defaultContributorName(parent);
        // A HealthComponent should only exist if defined during construction, instead of adding it dynamically.
        if (!relations.containsKey(parent) && parent != ROOT_HEALTH_CONTRIBUTOR) {
            log.warn("Attempting to register {} under an unrecognized HealthComponent {}.", contributor, parent);
            return;
        }
        log.info("Registering {} to the {}.", contributor, parent);
        // HealthContributor -> Set { HealthComponent }
        relations.get(parent).add(contributor);
        // Another HealthComponent may also depend on this HealthContributor.
        relations.putIfAbsent(contributor.getName(), new HashSet<>());
        // If 'contributor' it mapped to by a non-root, remove it from the root references.
        if (parent != ROOT_HEALTH_CONTRIBUTOR) {
            relations.get(ROOT_HEALTH_CONTRIBUTOR).remove(contributor);
        }
    }

    @NonNull
    public void register(HealthContributor contributor, HealthContributor parent) {
        register(contributor, parent.getName());
    }

    @NonNull
    @Override
    public void unregister(HealthContributor contributor) {
        unregister(contributor.getName());
    }

    @NonNull
    @Override
    public void unregister(String name) {
        // Acts as a guard from removing HealthComponents.
        if (!relations.get(name).isEmpty()) {
            log.warn("Attempting to unregister a component with existing dependencies -- aborting.");
            return;
        }
        if (!contributors.containsKey(name)) {
            log.warn("Attempted to remove an unregistered HealthContributor -- {}", name);
            return;
        }
        relations.remove(contributors.get(name));
        contributors.remove(contributors.get(name));

    }

    public Optional<HealthContributor> get(String name) {
        return Optional.ofNullable(contributors.get(defaultContributorName(name)));
    }

    public Collection<HealthContributor> contributors(String name) {
        return this.relations.get(name);
    }

    public Collection<HealthContributor> contributors() {
        return contributors(ROOT_HEALTH_CONTRIBUTOR);
    }

    public void clear() {
        contributors.clear();
        relations.clear();
    }

    private String defaultContributorName(String name)  {
        return name == null ? ROOT_HEALTH_CONTRIBUTOR : name;
    }
}
