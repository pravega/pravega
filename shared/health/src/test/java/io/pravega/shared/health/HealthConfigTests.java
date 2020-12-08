/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import io.pravega.shared.health.impl.HealthConfigImpl;
import io.pravega.shared.health.impl.StatusAggregatorImpl;
import io.pravega.test.common.AssertExtensions;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

/**
 * The goal of the {@link HealthConfig} is to ensure that the provided {@link io.pravega.shared.health.impl.HealthComponent}
 * hierarchy is not cyclic. No graphs that contain any cycle should be permitted to exist. Not only does it not logically
 * make sense for some {@link io.pravega.shared.health.impl.HealthComponent} to depend on itself, but by ensuring that
 * only {@link HealthContributor} objects can be registered to a component, we can maintain this cycle-free invariant.
 *
 * Each test will generally follow this format: define a certain class of graph and assert that it succeeds if it is non-cyclic.
 *
 * Note: The {@link HealthConfig} represents a *directed* graph. A particular {@link HealthContributor} however may be reachable
 * via two independent paths, meaning the {@link HealthConfig} need not represent a Tree structure.
 */
@Slf4j
public class HealthConfigTests {

    /**
     * Tests that a {@link HealthConfig} definition cannot have a cycle. The following construction is a 'Complete Graph':
     *
     *    A <--------+
     *    +          |
     *    |          |
     *    |          |
     *    v          +
     *    B +------> C
     */
    @Test
    public void testCyclicComponentRejected() {
        AssertExtensions.assertThrows("A cycle should have been noticed and the HealthConfig rejected.",
                () -> HealthConfigImpl.builder()
                            .define("A", StatusAggregatorImpl.DEFAULT)
                            .define("B", StatusAggregatorImpl.DEFAULT)
                            .define("C", StatusAggregatorImpl.DEFAULT)
                            .relation("A", "B")
                            .relation("B", "C")
                            .relation("C", "A")
                            .build(),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Tests that a {@link HealthConfig} of a single component is valid, i.e. the 'Trivial Graph':
     *
     *     A
     */
    @Test
    public void testSingleComponent() {
        HealthConfigImpl.builder()
                    .define("A", StatusAggregatorImpl.DEFAULT)
                    .build();
    }

    /**
     * Tests that a {@link HealthConfig} that defines only components and *not* relations is valid, i.e. the 'Null Graph'.
     *
     *    A    B    C    D
     */
    @Test
    public void testComponentsWithNoRelations() {
        HealthConfigImpl.builder()
                .define("A", StatusAggregatorImpl.DEFAULT)
                .define("B", StatusAggregatorImpl.DEFAULT)
                .define("C", StatusAggregatorImpl.DEFAULT)
                .define("D", StatusAggregatorImpl.DEFAULT)
                .build();
    }

    /**
     * Tests a {@link HealthConfig} definition such that one parent (a) has many children (b,c,d). Structurally equivalent
     * to a 'Star Graph':
     *
     *           C
     *           |
     *     D --- A --- B
     */
    @Test
    public void testComponentManyChildren() {
        HealthConfigImpl.builder()
                .define("A", StatusAggregatorImpl.DEFAULT)
                .define("B", StatusAggregatorImpl.DEFAULT)
                .define("C", StatusAggregatorImpl.DEFAULT)
                .define("D", StatusAggregatorImpl.DEFAULT)
                .relation("B", "A")
                .relation("C", "A")
                .relation("D", "A")
                .build();
    }

    /**
     * Tests that a {@link HealthConfig} definition which is structurally 'Bipartite' is valid:
     *
     *    A +----> C
     *       \ /
     *        X
     *       / \
     *    B +----> D
     */
    @Test
    public void testComponentSharedDependency() {
        HealthConfigImpl.builder()
                .define("A", StatusAggregatorImpl.DEFAULT)
                .define("B", StatusAggregatorImpl.DEFAULT)
                .define("D", StatusAggregatorImpl.DEFAULT)
                .define("D", StatusAggregatorImpl.DEFAULT)
                .relation("C", "A")
                .relation("D", "A")
                .relation("D", "B")
                .relation("C", "B")
                .build();
    }

    /**
     * Tests an *empty* {@link HealthConfig} definition. This equates to an empty graph, i.e. one with no nodes.
     */
    @Test
    public void testEmptyConfigBuilds() {
        Assert.assertTrue("Config should be listed as 'empty'.", HealthConfigImpl.builder().empty().isEmpty());
    }

    /** Tests that a {@link HealthConfig} restricts definitions with any component that has a self-reference:
     *
     *     +-----+
     *     |     |
     *     |     |
     *     A <---+
     */
    @Test
    public void testSelfReferenceRejected() {
        AssertExtensions.assertThrows("A self-reference was not detected.",
                () -> HealthConfigImpl.builder()
                        .define("A", StatusAggregatorImpl.DEFAULT)
                        .relation("A", "A")
                        .build(),
                ex -> ex instanceof IllegalStateException);
    }

}

