/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@link HealthComponentConfig} class allows a client to define a (logical) tree-like structure of {@link HealthComponent}
 * to {@link HealthComponent} relations.
 */
@Slf4j
public class HealthComponentConfig {

    @Getter
    private final Map<String, HealthComponent> components;

    private HealthComponentConfig(Map<String, HealthComponent> components) {
        this.components = components;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Map<String, HealthComponent> components = new HashMap<>();

        private final Map<String, Set<String>> relations = new HashMap<>();

        public Builder define(String name, StatusAggregator aggregator) {
            if (components.containsKey(name)) {
                log.warn("Overwriting a pre-existing component definition -- aborting.");
                return this;
            }
            components.put(name, new HealthComponent(name, aggregator));
            relations.put(name, new HashSet<>());
            return this;
        }

        public Builder relation(String child, String parent) {
            if (child.equals(parent)) {
                log.warn("Attempting to add a reference to itself -- aborting.");
                return this;
            }
            if (!components.containsKey(child) || !components.containsKey(parent)) {
                log.warn("At least one of the components in this relation has not been defined -- aborting.");
                return this;
            }
            relations.get(parent).add(child);
            // Also add the relation in the underlying HealthComponent.
            components.get(parent).register(components.get(child));
            // Is not a root node.
            components.get(child).setRoot(false);
            return this;
        }

        /**
         * Performs an exhaustive graph traversal to ensure (DFS) that there are no cyclic relations.
         *
         * @return Whether or not the specification is cycle free.
         * @throws Exception
         */
        private boolean validate() {
            boolean cycle = false;
            for (val component : relations.entrySet()) {
                cycle |= validate(component.getKey(), new HashMap<>());
            }
            return !cycle;
        }

        private boolean validate(String name, Map<String, Boolean> visited) {
            if (visited.get(name)) {
                return true;
            }
            boolean cycle = false;
            visited.put(name, true);
            for (String child : relations.get(name)) {
                cycle |= validate(child, visited);
            }
            return cycle;
        }

        public HealthComponentConfig build() throws Exception {
            if (validate()) {
                return new HealthComponentConfig(components);
            } else {
                throw new RuntimeException("Invalid HealthComponentConfig definition -- cyclic references.");
            }
        }
    }

    public boolean isEmpty() {
        return components.isEmpty();
    }

    public static HealthComponentConfig empty() {
        return new HealthComponentConfig(new HashMap<>());
    }
}
