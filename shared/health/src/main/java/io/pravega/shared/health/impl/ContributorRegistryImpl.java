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

import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.StatusAggregator;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Objects;

// Modifications to the 'contributors', 'children', and 'parents' must happen atomically to ensure valid state is always
// perceived. The 'components' set is not used for program logic so synchronization is not strictly required.
@Slf4j
@ThreadSafe
public class ContributorRegistryImpl implements ContributorRegistry {

    /**
     * Maintains a *flattened* {@link Map} of all {@link HealthContributor} objects to avoid following references.
     */
    protected final Map<String, HealthContributor> contributors = new HashMap<>();

    /**
     * Maintains a *flattened* {@link Collection} of all {@link HealthComponent} ids.
     * Not used for any registration/de-registration logic.
     */
    protected final Collection<String> components = new HashSet<>();

    /**
     * A {@link Map} that maintains all child (dependencies) relations for a given {@link HealthContributor} object.
     * This {@link Map} is kept for registration and de-registration purposes.
     */
    protected final Map<String, Collection<HealthContributor>> children = new HashMap<>();

    /**
     * A {@link Map} that maintains all parent (dependees) relations on a {@link HealthContributor} object.
     * This {@link Map} is kept for de-registration purposes.
     */
    protected final Map<String, Collection<HealthContributor>> parents = new HashMap<>();

    /**
     * The base under which all other {@link HealthComponent} will be registered and where {@link HealthContributor}
     * will be registered when no parent {@link HealthComponent} is specified.
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
        // Initialize children set of the root.
        children.put(DEFAULT_CONTRIBUTOR_NAME, new HashSet<>());
        // Not needed, but useful for validation.
        parents.put(DEFAULT_CONTRIBUTOR_NAME, new HashSet<>());
        // Make it visible to clients.
        contributors.put(DEFAULT_CONTRIBUTOR_NAME, root);
        // Separates 'contributors' from 'components' -- useful for an internal vs leaf node distinction.
        components.add(DEFAULT_CONTRIBUTOR_NAME);
    }

    /**
     * Registers a {@link HealthComponent} under the implicit 'root' {@link HealthComponent}.
     *
     * @param component The {@link HealthComponent} object to add to the registry.
     * @return The {@link HealthContributor} registered under the {@link ContributorRegistry}.
     */
    @NonNull
    @Override
    synchronized public HealthContributor register(HealthComponent component) {
        return register(component, root);
    }

    /**
     * Registers a {@link HealthContributor} under the implicit 'root' {@link HealthComponent}.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry.
     * @return The {@link HealthContributor} registered under the {@link ContributorRegistry}.
     */
    @NonNull
    @Override
    synchronized public HealthContributor register(HealthContributor contributor) {
        return register(contributor, root);
    }

    /**
     * Registers a {@link HealthComponent} under the specified parent {@link HealthComponent}.
     *
     * @param component The {@link HealthComponent} object to add to the registry as a child of 'parent'.
     * @param parent The {@link HealthComponent} to add 'contributor' as a child/dependency.
     * @return The {@link HealthContributor} registered under the {@link ContributorRegistry}.
     */
    @NonNull
    @Override
    synchronized public HealthContributor register(HealthComponent component, HealthComponent parent) {
        parent = Objects.isNull(parent) ? root : parent;
        // A HealthComponent should only exist if defined during construction, instead of adding it dynamically.
        if (!components.contains(parent.getName()) || !contributors.containsKey(parent.getName())) {
            log.warn("Attempting to register {} under unrecognized {} -- aborting.", component, parent);
            return null;
        }
        components.add(component.getName());
        return register(component, parent.getName());
    }

    /**
     * Registers a {@link HealthContributor} under the specified parent {@link HealthComponent}.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry as a child of 'parent'.
     * @param parent The {@link HealthComponent} to add 'contributor' as a child/dependency.
     * @return The {@link HealthContributor} registered under the {@link ContributorRegistry}.
     */
    @NonNull
    synchronized public HealthContributor register(HealthContributor contributor, HealthComponent parent) {
        parent = Objects.isNull(parent) ? root : parent;
        return register(contributor, parent.getName());
    }

    /**
     * Registers a {@link HealthContributor} under the specified parent {@link HealthComponent}.
     *
     * @param contributor The {@link HealthContributor} object to add to the registry as a child of 'parent'.
     * @param componentName The {@link String} id of a {@link HealthComponent} to add 'contributor' as a child/dependency.
     * @return The {@link HealthContributor} registered under the {@link ContributorRegistry}. Null return values are permitted.
     */
    @NonNull
    @Override
    synchronized public HealthContributor register(HealthContributor contributor, String componentName) {
        // HealthContributor mapped by 'parent' should exist at time of some child registration.
        if (!contributors.containsKey(componentName)) {
            log.debug("Unrecognized HealthContributor::{} -- aborting registration.", componentName);
            return null;
        }
        String contributorName = contributor.getName();
        if (contributors.containsKey(contributorName)) {
            log.warn("{} has already been registered.", contributors.get(contributorName));
            return contributors.get(contributorName);
        }
        log.debug("Registering {} to {}.", contributor, contributors.get(componentName));
        contributors.put(contributorName, contributor);
        // A new contributor should have no child relations to overwrite.
        // Realistically, this should be restricted because it implies non HealthComponent contributors can have children.
        children.put(contributorName, new HashSet<>());
        // Add the child relation.
        children.get(componentName).add(contributor);
        // 'contributor' should not have any existing parents when registered.
        parents.put(contributorName, new HashSet<>());
        // Add the parent relation.
        parents.get(contributorName).add(contributors.get(componentName));

        return contributor;
    }

    /**
     * Removes the {@link HealthContributor} from the {@link ContributorRegistry}. Updates all child/parent relations between
     * any other {@link HealthContributor}. Should prevent removal of any {@link HealthComponent} objects.
     * *Must* prevent removal of the root {@link HealthComponent}.
     *
     * @param contributor The {@link HealthContributor} object to remove.
     * @return The now removed {@link HealthContributor}. Null return values are permitted, in the case that removal fails,
     * i.e. the {@link HealthContributor} was not actually registered.
     */
    @NonNull
    @Override
    synchronized public HealthContributor unregister(HealthContributor contributor) {
        return unregister(contributor.getName());
    }

    /**
     * Removes the {@link HealthContributor} from the {@link ContributorRegistry} which is mapped to by 'name'.
     * Updates all child/parent relations between any other {@link HealthContributor}. Should prevent removal of any
     * {@link HealthComponent} objects. *Must* prevent removal of the root {@link HealthComponent}.
     *
     * @param name The {@link String} which maps to some {@link HealthContributor} object to remove.
     * @return The now removed {@link HealthContributor}. Null return values are permitted, in the case that removal fails,
     * i.e. the {@link HealthContributor} was not actually registered.
     */
    @NonNull
    @Override
    synchronized public HealthContributor unregister(String name) {
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
            Collection<HealthContributor> from = parents.get(child.getName());
            from.remove(contributor);
            // Validate that this contributor is still reachable.
            if (from.isEmpty()) {
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

    /**
     * Get the {@link HealthContributor} with id 'name'. A NULL value may be returned.
     *
     * @param name The name of some {@link HealthContributor} to request from this {@link ContributorRegistry}.
     * @return The {@link HealthContributor} mapped to by 'name'.
     */
    @Override
    @NonNull
    synchronized public HealthContributor get(String name) {
        return contributors.get(name);
    }

    /**
     * Provides a reference to the root {@link HealthContributor}.
     * @return The root {@link HealthContributor}.
     */
    @Override
    synchronized public HealthContributor getRootContributor() {
        return contributors.get(DEFAULT_CONTRIBUTOR_NAME);
    }

    /**
     * Provides the set of dependencies that the {@link HealthContributor} mapped to by 'name' relies on.
     *
     * @param name The {@link String} used to query which {@link HealthContributor} to gather it's {@link HealthContributor}
     *             dependencies.
     * @return The {@link Collection} used to determine the {@link io.pravega.shared.health.Health}
     *         of the {@link HealthContributor} mapped to by 'name'.
     */
    @Override
    synchronized public Collection<HealthContributor> dependencies(String name) {
        return this.children.get(name);
    }

    /**
     *
     * Provides the set of dependencies that the root {@link HealthContributor} relies on.
     *
     * @return The {@link Collection} of {@link HealthContributor} used by the root.
     */
    @Override
    synchronized public Collection<HealthContributor> dependencies() {
        return dependencies(DEFAULT_CONTRIBUTOR_NAME);
    }

    /**
     * Supplies a {@link Collection} of *all* {@link HealthContributor} ids (names) registered within
     * this {@link ContributorRegistry}.
     *
     * @return The {@link Collection} of {@link HealthContributor} names.
     */
    @Override
    synchronized public Collection<String> contributors() {
        return new ArrayList<>(contributors.keySet());
    }

    /**
     * Supplies a {@link Collection} of *all* {@link HealthComponent} ids (names) registered within
     * this {@link ContributorRegistry}. This method returns a subset of the {@link Collection} returned by
     * {@link ContributorRegistry#contributors()}.
     *
     * @return The {@link Collection} of {@link HealthComponent} names.
     */
    synchronized public Collection<String> components() {
        return new ArrayList<>(this.components);
    }

    /**
     * Resets the {@link ContributorRegistry} to the state as if it was just instantiated.
     */
    synchronized public void clear() {
        components.clear();
        contributors.clear();
        children.clear();
        parents.clear();
        initialize(root);
    }
}
