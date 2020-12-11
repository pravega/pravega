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

import com.google.common.base.Preconditions;
import io.pravega.shared.health.ContributorRegistry;
import io.pravega.shared.health.HealthConfig;
import io.pravega.shared.health.StatusAggregator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@link HealthConfig} class allows a client to define a (logical) tree-like structure of {@link HealthComponent}
 * to {@link HealthComponent} relations.
 */
@Slf4j
public class HealthConfigImpl implements HealthConfig {

    private final Map<String, HealthComponent> components;

    private final Set<HealthComponent> roots;

    private final Map<String, Set<String>> relations;

    private HealthConfigImpl(Map<String, HealthComponent> components, Set<HealthComponent> roots, Map<String, Set<String>> relations) {
        this.components = components;
        this.roots = roots;
        this.relations = relations;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Map<String, HealthComponent> components = new HashMap<>();

        private final Map<String, Set<String>> relations = new HashMap<>();

        private final Set<HealthComponent> roots = new HashSet<>();

        /**
         * Defines a new {@link HealthComponent} to be created.
         * @param name The name of the to-be {@link HealthComponent}.
         * @param aggregator The aggregation rule to use.
         *
         * @return A reference to the *same* object, with updates to its internal data-structures made.
         */
        public Builder define(String name, StatusAggregator aggregator) {
            if (components.containsKey(name)) {
                log.warn("Overwriting a pre-existing component definition -- aborting.");
            } else {
                HealthComponent component = new HealthComponent(name, aggregator, null);
                roots.add(component);
                components.put(name, component);
                relations.put(name, new HashSet<>());
            }
            return this;
        }

        /**
         * Defines that there should be a parent-child relation between a {@link HealthComponent} called 'child'
         * and a {@link HealthComponent} called 'parent'.
         *
         * @param child The name of the child {@link HealthComponent}.
         * @param parent The name of the parent {@link HealthComponent}.
         * @return A reference to the *same* object, with updates to its internal data-structures made.
         */
        public Builder relation(String child, String parent) {
            if (!components.containsKey(child) || !components.containsKey(parent)) {
                log.warn("At least one of the components in this relation has not been defined -- aborting.");
                return this;
            }
            roots.remove(components.get(child));
            relations.get(parent).add(child);
            return this;
        }

        /**
         * Performs an exhaustive graph traversal to ensure (DFS) that there are no cyclic relations.
         *
         * @return Whether or not the specification is cycle-free.
         * @throws Exception
         */
        private boolean isValid() {
            boolean cycle = false;
            HashMap<String, Boolean> visited = new HashMap<>();
            for (val component : relations.entrySet()) {
                cycle |= validate(component.getKey(), visited);
                if (cycle) {
                    break;
                }
                visited.clear();
            }
            return !cycle;
        }

        /**
         * The main recursive method used for the traversal.
         *
         * @param name The name of the {@link HealthComponent} currently being checked.
         * @param visited The list of {@link HealthComponent} visited so far.
         *
         * @return Whether or not this component has been searched previously and is cycle-free.
         */
        private boolean validate(String name, Map<String, Boolean> visited) {
            if (visited.containsKey(name) && visited.get(name)) {
                return true;
            }
            boolean cycle = false;
            visited.put(name, true);
            for (String child : relations.get(name)) {
                cycle |= validate(child, visited);
                if (cycle) {
                    break;
                }
            }
            return cycle;
        }

        public HealthConfig empty() {
            HealthConfig config = null;
            try {
                config = build();
            } catch (Exception e) {
                log.error("Error building empty HealthConfig.", e);
            }
            return config;
        }

        /**
         * Returns the final constructed {@link HealthConfig} implementation, but validates the configuration to ensure
         * that there are no cyclic relations.
         *
         * @return The resulting {@link HealthConfig}.
         * @throws IllegalStateException Thrown {@link Exception} in the case the validation fails.
         */
        public HealthConfig build() {
            Preconditions.checkState(isValid(), "Invalid HealthConfig definition: Cyclic reference(s) detected.");
            return new HealthConfigImpl(components, roots, relations);
        }
    }

    public boolean isEmpty() {
        return components.isEmpty() && relations.isEmpty() && roots.isEmpty();
    }

    /**
     * Applies the necessary operations to take an empty {@link ContributorRegistry} and transform it to mirror the
     * topology defined by this {@link HealthConfig} implementation.
     *
     * @param registry The {@link ContributorRegistry} to apply this reconciliation on.
     */
    public void reconcile(ContributorRegistry registry) {
        for (val component : this.roots) {
            recurse(registry, component, null);
        }
    }

    // This validation should prevent any unintended StackOverflow errors.
    private void recurse(ContributorRegistry registry, HealthComponent component, HealthComponent parent) {
        // Must supply a valid registry reference.
        registry.register(new HealthComponent(component.getName(), component.getAggregator(), registry), parent);
        // Register all dependencies.
        for (val name : relations.get(component.getName())) {
            recurse(registry, components.get(name), component);
        }
    }
}