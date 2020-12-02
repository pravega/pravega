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
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.TestHealthIndicators.SampleHealthyIndicator;
import io.pravega.shared.health.TestHealthIndicators.SampleFailingIndicator;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

@Slf4j
public class ContributorRegistryTests {

    private static final int BOUND = 100;

    ContributorRegistry registry = new ContributorRegistryImpl();

    @After
    public void after() {
        registry.reset();
        Assert.assertTrue(registry.components().size() == 1);
        Assert.assertTrue(registry.contributors().size() == 1);
    }

    // HealthContributors must be explicitly registered before any children can be added to it, i.e the  parent cotributor
    // is not created on the fly if it does not exist.
    @Test
    public void registerUnderNonExistingParent() {
        int before = registry.contributors().size();
        registry.register(new SampleHealthyIndicator(), "NULL");
        Assert.assertEquals("Expected the registration to fail, contributor count should not increase.",
                before,
                registry.contributors().size());
    }

    @Test
    public void register() {
        simpleRegister();
    }

    @Test
    public void unregister() {
        simpleRegister();
        simpleUnregister();
    }

    @Test
    public void unregisterComponent() {
        // Sanity check.
        int beforeContributors = registry.contributors().size();
        int beforeComponents = registry.components().size();
        Assert.assertEquals("Number of HealthComponents and HealthContributors should be equal.", beforeComponents, beforeContributors);
        // Attempt HealthComponent removal.
        registry.unregister(ContributorRegistry.DEFAULT_CONTRIBUTOR_NAME);
        Assert.assertEquals("Expected the de-registration to fail, HealthComponents should not be allowed to be removed.",
                beforeContributors,
                registry.contributors().size());
        // Ensure same information is conveyed by components list.
        Assert.assertEquals("Expected the de-registration to fail, HealthComponents should not be allowed to be removed.",
                beforeComponents,
                registry.components().size());
    }

    // Validate that removing a HealthContributor (HealthComponent) with dependencies properly updates its references.
    // Meaning that if this internal node causes any children node to become unreachable via traversal from the root,
    // these unreachable nodes are properly removed.
    //
    // Note: HealthComponent objects registered are considered immutable, but by registering a HealthComponent as a HealthContributor,
    // we can create a HealthContributor that aggregates multiple HealthContributors, yet can still be removed dynamically.
    //
    // Let us create the follow 'Health Hierarchy':
    // -----------------------------------------------
    //    ROOT (HealthComponent - Immutable)/ ────────────────────────\
    //        ├─ CONTAINER (HealthComponent/Contributor - Mutable)/   │
    //        │  ├─ ONE - HealthIndicator                             │
    //        │  ├─ TWO - HealthIndicator <───────────────────────────\
    //        ├─ THREE - HealthIndicator
    //
    //  And hope to achieve the following by removing the 'CONTAINER' HealthComponent (registered as a HealthContributor).
    //-------------------------------------------------
    //    ROOT (HealthComponent - Immutable)/
    //        ├─ TWO - HealthIndicator
    //        ├─ THREE - HealthIndicator
    //
    // NOTE: This was constructed to ensure validity, but this should not be done in practice. This loop hole allows one to
    // introduce cycles into this graph, which should be *strictly* forbidden.
    @Test
    public void unregisterInternalContributor() {
        // ContributorRegistry does not expose the API that would allow us to test potentially dangerous inserts.
        ContributorRegistryImpl registry = new ContributorRegistryImpl();
        // Create the contributors to register.
        ArrayList<HealthContributor> contributors = new ArrayList<>(Arrays.asList(
                new HealthComponent("container", StatusAggregatorImpl.DEFAULT, registry),
                new SampleHealthyIndicator("one"),
                new SampleHealthyIndicator("two"),
                new SampleFailingIndicator("three")
        ));

        HealthContributor container = contributors.get(0);
        // Register said contributors.
        registry.register(container);
        registry.register(contributors.get(1), container.getName());
        registry.register(contributors.get(2), container.getName());
        registry.register(contributors.get(3));
        // Validate the registration process.
        Assert.assertEquals("Only one HealthComponent should be recognized (ROOT).",
                1,
                registry.components().size());
        // Four contributors should exist (3 + root).
        Assert.assertEquals("The three contributors and the root should have been recognized.",
                contributors.size() + 1,
                registry.contributors().size());
        // Remove internal node ('container').
        registry.unregister(container);
        // This should removes three contributors: the internal node (container) and the leaf nodes (one, two), leaving
        // the root and 'three'.
        Assert.assertEquals("Three HealthContributors should be remaining.", 2, registry.contributors().size());
        Assert.assertEquals("One HealthComponent should remain.", 1, registry.components().size());
        // The component should be the 'root'.
        Assert.assertEquals("The HealthComponent should be the root.",
                registry.components().stream().findFirst().get(),
                ContributorRegistry.DEFAULT_CONTRIBUTOR_NAME);
        // The contributors should be the 'root' and '
        Assert.assertArrayEquals("The HealthContributors should be the 'root' and 'three'.",
                registry.contributors().toArray(),
                new String[]{ ContributorRegistry.DEFAULT_CONTRIBUTOR_NAME, contributors.get(2).getName(), contributors.get(3).getName() });
    }

    @Test
    // A subset of the above, but valuable to ensure all cases are handled.
    public void unregisterLeafContributor() {
        // Create the contributors to register.
        ArrayList<HealthContributor> contributors = new ArrayList<>(Arrays.asList(
                new SampleHealthyIndicator("one"),
                new SampleHealthyIndicator("two"),
                new SampleFailingIndicator("three")
        ));
        // Register them to the root.
        for (HealthContributor contributor : contributors) {
            registry.register(contributor);
        }
        Assert.assertEquals("Only one HealthComponent should be recognized (ROOT).",
                1,
                registry.components().size());
        // Four contributors should exist (3 + root).
        Assert.assertEquals("The three contributors and the root should have been recognized.",
                contributors.size() + 1,
                registry.contributors().size());
        for (HealthContributor contributor : contributors) {
            registry.unregister(contributor);
        }
        Assert.assertEquals("Only one HealthContributor should be recognized (ROOT).",
                1,
                registry.contributors().size());
    }

    // Should not be able to overwrite existing HealthContributors.
    @Test
    public void registerOverwrite() {
        // Register SampleHealthyIndicator.
        simpleRegister();
        int before = registry.contributors().size();
        // Try to replace it with a SampleFailingIndicator.
        SampleFailingIndicator failing = new SampleFailingIndicator("sample-healthy-indicator");
        registry.register(failing);
        int after = registry.contributors().size();
        Assert.assertEquals("The number of contributors should remain the same.", after, before);
        // The indicator should not have been replaced.
        Optional<HealthContributor> contributor = registry.get(failing.getName());
        // References should be different.
        Assert.assertNotEquals("The HealthContributors should not refer to the same object.", contributor.get(), failing);
        // Can also validate that the 'Status' results are different.
        Assert.assertNotEquals("The HealthContributors should have different 'Status' results.",
                contributor.get().health().getStatus(),
                failing.health().getStatus());
    }

    // Ensure that there are guards against trying to remove a contributor that does not exist.
    @Test
    public void unregisterNonExisting() {
       simpleRegister();
       int before = registry.contributors().size();
       registry.unregister("non-existing-contributor");
       int after = registry.contributors().size();
       Assert.assertEquals("No changes to the contributor list should have happened.", before, after);
    }

    // Define this following HealthComponent Hierarchy (H entries).
    // ----------------------------------------------------------
    // A/
    // ├─ B/
    // │  ├─ C/
    // D/
    // ├─ E/
    // F/
    // G/
    //
    // Unlike 'unregisterInternalContributor', this test follows the assumption that no children contributors will be added
    // to non-component contributors.
    @Test
    public void unregisterNoInvalidReferences() {
        int numComponents = 7;
        // Create the components.
        Map<Character, HealthComponent> components = new HashMap<>();
        for (int i = 0; i < numComponents; i++) {
            char key = (char) ('a' + i);
            components.put(key, new HealthComponent(Character.toString(key), StatusAggregatorImpl.DEFAULT, registry));
        }
        // Registry with the extra 'validation' functionality.
        TestContributorRegistry registry = new TestContributorRegistry();
        // Define the relationships -- Depth 1 relations.
        registry.register(components.get('a'));
        registry.register(components.get('d'));
        registry.register(components.get('f'));
        registry.register(components.get('g'));
        // Depth 2 relations.
        registry.register(components.get('b'), components.get('a'));
        registry.register(components.get('e'), components.get('d'));
        // Depth 3 relations.
        registry.register(components.get('c'), components.get('b'));

       // 1. Create a random number of health indicators *N*.
       int n = new Random().nextInt(BOUND);
       ArrayList<HealthContributor> contributors = new ArrayList<>();
       for (int i = 0; i < n; i++) {
           contributors.add(new SampleFailingIndicator(String.format("sample-health-indicator-%d", i)));
       }
       // 2. *N* times, randomly choose a health indicator, register it, and randomly choose *M <= H* parents to assign
       // this health contributor as a child/dependency.
       for (int i = 0; i < n; i++) {
           int m = new Random().nextInt(numComponents) + 1;
           Set<Character> parents = new HashSet<>();
           for (int j = 0; j < m; j++) {
               parents.add((char) ('a' + new Random().nextInt(numComponents)));
           }
           for (Character parent : parents) {
               registry.register(contributors.get(i), Character.toString(parent));
               registry.validate();
           }
       }
       // 3. Until no indicators are left, randomly choose an indicator to remove.
       Collections.shuffle(contributors);
       for (HealthContributor contributor : contributors) {
           registry.unregister(contributor);
           registry.validate();
       }
       // 4. Validate (call 'validate') after each register/unregister call.
    }

    private void simpleRegister() {
        int before = registry.contributors().size();
        registry.register(new SampleHealthyIndicator());
        Assert.assertEquals("Expected the registration to succeed, contributor count should have increased by one.",
                before + 1,
                registry.contributors().size());
    }

    private void simpleUnregister() {
        HealthContributor sample = new SampleHealthyIndicator();
        int before = registry.contributors().size();
        registry.unregister(sample.getName());
        log.info("Remaining HealthContributors: {}", registry.contributors());
        Assert.assertEquals("Expected the de-registration to succeed, contributor count should have decreased by one.",
                before - 1,
                registry.contributors().size());
    }

    static class TestContributorRegistry extends ContributorRegistryImpl {
        // Validates that all entries in each of the internal containers point to valid references.
        // The 'components' field is not used to determine the validity of the tree-structure.
        public void validate() {
            try {
                // Each container should have the same size.
                assert contributors.size() == children.size() && children.size() == parents.size();
                // Validate that each container contains the same set of keys.
                for (val entry : contributors.entrySet()) {
                    assert children.containsKey(entry.getKey());
                    assert parents.containsKey(entry.getKey());
                }
                // If above holds true, the keySet for each container should be strictly equivalent.
                // Validate that each parent/child reference for a given contributor points to a valid contributor.
                for (val collection : children.entrySet()) {
                    for (val contributor : collection.getValue()) {
                        assert contributor.equals(contributors.get(contributor.getName()));
                    }
                }
                // Do the same for the parent set.
                for (val collection : parents.entrySet()) {
                    for (val contributor : collection.getValue()) {
                        assert contributor.equals(contributors.get(contributor.getName()));
                    }
                }
            } catch (AssertionError e) {
                toString();
            }
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (val entry : contributors.entrySet()) {
                builder = builder.append(String.format("%s: %n", entry.getKey()));
                // Builder the child relations.
                if (children.containsKey(entry.getKey())) {
                    builder = builder.append(String.format("\t (%d) children: ", children.get(entry.getKey()).size()));
                    for (val child : children.get(entry.getKey())) {
                        builder = builder.append(child.getName());
                    }
                    builder = builder.append('\n');
                }
                // Build the parent relations.
                if (parents.containsKey(entry.getKey())) {
                    builder = builder.append(String.format("\t(%d) parents: ", parents.get(entry.getKey()).size()));
                    for (val parent: parents.get(entry.getKey())) {
                        builder = builder.append(parent.getName());
                    }
                    builder = builder.append('\n');
                }
                builder = builder.append('\n');
            }
            return builder.toString();
        }
    }
}
