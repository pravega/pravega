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
import java.util.stream.Collectors;

@Slf4j
public class ContributorRegistryImpl implements ContributorRegistry {

    /**
     * Maintains a *flattened* {@link Map} of all {@link HealthComponent} objects to avoid following references.
     */
    private final Map<String, HealthContributor> contributors = new HashMap<>();

    /**
     * A {@link Map} that maintains all child (dependencies) relations on a {@link HealthContributor} object.
     * This {@link Map} is kept for registration & de-registration purposes.
     */
    private final Map<String, Collection<HealthContributor>> children = new HashMap<>();

    /**
     * A {@link Map} that maintains all parent (dependees) relations on a {@link HealthContributor} object.
     * This {@link Map} is kept for de-registration purposes.
     */
    private final Map<String, Collection<HealthContributor>> parents = new HashMap<>();

    /**
     * The base under which all other {@link HealthComponent} will be registered.
     */
    @NonNull
    ContributorRegistryImpl() {
        children.put(DEFAULT_CONTRIBUTOR_NAME, new HashSet<>());
        // Be careful of
        contributors.put(DEFAULT_CONTRIBUTOR_NAME, new HealthComponent(DEFAULT_CONTRIBUTOR_NAME, StatusAggregatorImpl.DEFAULT, this));
    }

    @NonNull
    @Override
    public void register(HealthContributor contributor) {
        register(contributor, DEFAULT_CONTRIBUTOR_NAME);
    }

    // The 'parent' component is expected to exist at the time of registration.
    public void register(HealthContributor contributor, String parent) {
        // A null string should map to the default/root HealthContributor.
        parent = defaultIfNull(parent);
        // A HealthComponent should only exist if defined during construction, instead of adding it dynamically.
        if (!children.containsKey(parent) && parent != DEFAULT_CONTRIBUTOR_NAME) {
            log.warn("Attempting to register {} under an unrecognized HealthComponent {}.", contributor, parent);
            return;
        }
        log.info("Registering {} to the {}.", contributor, parent);

        if (contributors.containsKey(contributor.getName())) {
            log.warn("Overwriting existing HealthContributor {}", contributor.getName());
            // Should unregister existing contributor and replace with new contributor.
            unregister(contributors.get(contributor.getName()));
        } else {
            contributors.put(contributor.getName(), contributor);
        }

        // Add the child relation.
        children.get(parent).add(contributor);
        // 'contributor' should not have any existing parents when registered.
        parents.put(contributor.getName(), new HashSet<>());
        // Add the parent relation.
        parents.get(contributor.getName()).add(contributors.get(parent));
    }

    @NonNull
    public void register(HealthContributor contributor, HealthComponent parent) {
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
        if (!children.get(name).isEmpty()) {
            log.warn("Attempting to unregister a component ({}) with existing dependencies -- aborting.", name);
            return;
        }
        if (!contributors.containsKey(name)) {
            log.warn("Attempted to remove an unregistered HealthContributor {} -- aborting.", name);
            return;
        }
        HealthContributor contributor = contributors.get(name);
        log.info("Unregistering {} from ContributorRegistry.", contributor);
        // Remove all parent -> {name} relations.
        for (HealthContributor parent : parents.get(name)) {
            children.get(parent.getName()).remove(contributor);
        }
        // Remove all child -> {name} relations.
        for (HealthContributor child : children.get(name)) {
            parents.get(child.getName()).remove(contributor);
            // Validate that this contributor is still reachable.
            if (parents.get(child.getName()).isEmpty()) {
                log.info("Unregistering {} caused {} to become unreachable.", contributor, child);
                unregister(child);
            }
        }
        // Remove contributor reference from all containers.
        parents.remove(contributors.get(name));
        children.remove(contributors.get(name));
        contributors.remove(contributors.get(name));
    }

    @Override
    public Optional<HealthContributor> get(String name) {
        return Optional.ofNullable(contributors.get(name));
    }

    @Override
    public Optional<HealthContributor> get() {
        return Optional.ofNullable(contributors.get(DEFAULT_CONTRIBUTOR_NAME));
    }

    @Override
    public Collection<HealthContributor> dependencies(String name) {
        return this.children.get(name);
    }

    @Override
    public Collection<HealthContributor> dependencies() {
        return dependencies(DEFAULT_CONTRIBUTOR_NAME);
    }

    @Override
    public Collection<String> contributors() {
        return contributors.entrySet()
                .stream()
                .map(entry -> entry.getKey())
                .collect(Collectors.toList());
    }

    //@Override
    //public Collection<HealthContributor> parents() {
    //    return parents(DEFAULT_CONTRIBUTOR_NAME);
    //}

    //@Override
    //public Collection<HealthContributor> parents(String name) {
    //    return this.parents.get(name);
    //}

    public void clear() {
        log.info("Clearing the ContributorRegistry.");
        contributors.clear();
        children.clear();
        parents.clear();
    }

    private String defaultIfNull(String name)  {
        return name == null ? DEFAULT_CONTRIBUTOR_NAME : name;
    }
}
