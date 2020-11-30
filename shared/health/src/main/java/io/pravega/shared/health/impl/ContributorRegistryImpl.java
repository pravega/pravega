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

import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.StatusAggregator;
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
     * Maintains a *flattened* {@link Map} of all {@link HealthContributor} objects to avoid following references.
     */
    protected final Map<String, HealthContributor> contributors = new HashMap<>();

    /**
     * Maintains a *flattened* {@link Collection} of all {@link HealthComponent} ids to avoid following references.
     */
    protected final Collection<String> components = new HashSet<>();

    /**
     * A {@link Map} that maintains all child (dependencies) relations on a {@link HealthContributor} object.
     * This {@link Map} is kept for registration & de-registration purposes.
     */
    protected final Map<String, Collection<HealthContributor>> children = new HashMap<>();

    /**
     * A {@link Map} that maintains all parent (dependees) relations on a {@link HealthContributor} object.
     * This {@link Map} is kept for de-registration purposes.
     */
    protected final Map<String, Collection<HealthContributor>> parents = new HashMap<>();

    /**
     * The base under which all other {@link HealthComponent} will be registered.
     */
    private final HealthComponent root;

    @NonNull
    public ContributorRegistryImpl(StatusAggregator aggregator) {
        root = new HealthComponent(DEFAULT_CONTRIBUTOR_NAME, aggregator, this);
        initialize(root);
    }

    @NonNull
    public ContributorRegistryImpl() {
        this(StatusAggregatorImpl.DEFAULT);
    }

    private void initialize(HealthComponent root) {
        // Initialize children set of root.
        children.put(DEFAULT_CONTRIBUTOR_NAME, new HashSet<>());
        // Not needed, but useful for validation.
        parents.put(DEFAULT_CONTRIBUTOR_NAME, new HashSet<>());
        // Possible escaped 'this'?
        contributors.put(DEFAULT_CONTRIBUTOR_NAME, root);
        // Separate 'contributors' from 'components' -- useful for an internal vs leaf node distinction.
        components.add(DEFAULT_CONTRIBUTOR_NAME);
    }

    @NonNull
    @Override
    public HealthContributor register(HealthComponent component) {
        components.add(component.getName());
        return register(component, root);
    }

    @NonNull
    @Override
    public HealthContributor register(HealthContributor contributor) {
        return register(contributor, root);
    }

    @NonNull
    @Override
    public HealthContributor register(HealthComponent component, HealthComponent parent) {
        // Use the default (root) component if parent is undefined.
        parent = parent == null ? root : parent;
        // A HealthComponent should only exist if defined during construction, instead of adding it dynamically.
        if (!components.contains(parent.getName()) || !contributors.containsKey(parent.getName())) {
            log.warn("Attempting to register {} under unrecognized {} -- aborting.", component, parent);
            return null;
        }
        components.add(component.getName());
        return register(component, parent.getName());
    }

    @NonNull
    public HealthContributor register(HealthContributor contributor, HealthComponent parent) {
        return register(contributor, parent.getName());
    }

    @NonNull
    @Override
    public HealthContributor register(HealthContributor contributor, String component) {
        String name = contributor.getName();
        // HealthContributor mapped by 'parent' should exist at time of some child registration.
        if (!contributors.containsKey(component)) {
            log.debug("Unrecognized HealthContributor::{} -- aborting registration.", component);
            return null;
        }
        if (contributors.containsKey(contributor)) {
            log.warn("{} has already been registered -- aborting.", contributors.get(contributor));
            return null;
        }
        log.debug("Registering {} to {}.", contributor, contributors.get(component));
        contributors.putIfAbsent(name, contributor);
        // A new contributor should have no child relations to overwrite.
        // Realistically, this should be restricted because it implies non HealthComponent contributors can have children.
        children.put(name, new HashSet<>());
        // Add the child relation.
        children.get(component).add(contributor);
        // 'contributor' should not have any existing parents when registered.
        parents.put(name, new HashSet<>());
        // Add the parent relation.
        parents.get(name).add(contributors.get(component));

        return contributor;
    }

    @NonNull
    @Override
    public HealthContributor unregister(HealthContributor contributor) {
        return unregister(contributor.getName());
    }

    @NonNull
    @Override
    public HealthContributor unregister(String name) {
        HealthContributor contributor = contributors.get(name);
        // Acts as a guard from removing HealthComponents.
        if (components.contains(name)) {
            log.warn("Attempting to unregister {} -- aborting.", contributor);
            return null;
        }
        if (!contributors.containsKey(name)) {
            log.warn("Attempted to remove an unrecognized HealthContributor::{} -- aborting.", name);
            return null;
        }
        log.debug("Unregistering {} from ContributorRegistry.", contributor);
        // Remove all parent -> {name} relations.
        for (HealthContributor parent : parents.get(name)) {
            children.get(parent.getName()).remove(contributor);
        }
        // Remove all child -> {name} relations.
        for (HealthContributor child : children.get(name)) {
            parents.get(child.getName()).remove(contributor);
            // Validate that this contributor is still reachable.
            if (parents.get(child.getName()).isEmpty()) {
                log.debug("> {} removal caused {} to become unreachable.", contributor, child);
                unregister(child);
            }
        }
        // Remove contributor reference from all containers.
        parents.remove(name);
        children.remove(name);
        contributors.remove(name);

        return contributor;
    }

    @Override
    @NonNull
    public Optional<HealthContributor> get(String name) {
        return Optional.ofNullable(contributors.get(name));
    }

    @Override
    public HealthContributor get() {
        return contributors.get(DEFAULT_CONTRIBUTOR_NAME);
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

    public Collection<String> components() {
        return this.components;
    }

    public void reset() {
        components.clear();
        contributors.clear();
        children.clear();
        parents.clear();
        initialize(root);
    }
}
