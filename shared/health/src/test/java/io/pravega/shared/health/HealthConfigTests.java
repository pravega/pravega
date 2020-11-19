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

import io.pravega.shared.health.impl.HealthConfigImpl;
import io.pravega.shared.health.impl.StatusAggregatorImpl;
import io.pravega.test.common.AssertExtensions;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

/**
 * The goal of the {@link HealthConfig }is to ensure that the provided {@link io.pravega.shared.health.impl.HealthComponent}
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

    @Test
    // Cyclic Graph.
    public void cyclicComponentRejected() {
        AssertExtensions.assertThrows("A cycle should have been noticed and the HealthConfig rejected.",
                () -> {
                    HealthConfigImpl.builder()
                            .define("a", StatusAggregatorImpl.DEFAULT)
                            .define("b", StatusAggregatorImpl.DEFAULT)
                            .define("c", StatusAggregatorImpl.DEFAULT)
                            .relation("a", "b")
                            .relation("b", "c")
                            .relation("c", "a")
                            .build();
                },
                ex -> ex instanceof RuntimeException);
    }

    @Test
    // Trivial Graph.
    public void singleComponent() {
        HealthConfigImpl.Builder builder = HealthConfigImpl.builder()
                    .define("a", StatusAggregatorImpl.DEFAULT);
        configExceptionHandler(builder, false);
    }

    @Test
    // A Null Graph.
    public void componentsWithNoRelations() {
        HealthConfigImpl.Builder builder = HealthConfigImpl.builder()
                .define("a", StatusAggregatorImpl.DEFAULT)
                .define("b", StatusAggregatorImpl.DEFAULT)
                .define("c", StatusAggregatorImpl.DEFAULT)
                .define("d", StatusAggregatorImpl.DEFAULT);
        configExceptionHandler(builder, false);
    }

    @Test
    // Disconnected Graph (Forest)
    public void disjointComponentHierarchy() {
        HealthConfigImpl.Builder builder = HealthConfigImpl.builder()
                .define("a", StatusAggregatorImpl.DEFAULT)
                .define("b", StatusAggregatorImpl.DEFAULT)
                .define("c", StatusAggregatorImpl.DEFAULT)
                .define("d", StatusAggregatorImpl.DEFAULT)
                .define("e", StatusAggregatorImpl.DEFAULT)
                .define("f", StatusAggregatorImpl.DEFAULT)
                .relation("c", "a")
                .relation("b", "a")
                .relation("e", "d")
                .relation("f", "d");
        configExceptionHandler(builder, false);
    }

    @Test
    // 'Spine' of a 'Caterpillar tree' -- Vertical line.
    public void verticalComponentHierarchy() {
        HealthConfigImpl.Builder builder = HealthConfigImpl.builder()
                .define("a", StatusAggregatorImpl.DEFAULT)
                .define("b", StatusAggregatorImpl.DEFAULT)
                .define("c", StatusAggregatorImpl.DEFAULT)
                .relation("b", "a")
                .relation("c", "b");
        configExceptionHandler(builder, false);
    }

    @Test
    // Star Graph.
    public void componentManyChildren() {
        HealthConfigImpl.Builder builder = HealthConfigImpl.builder()
                .define("a", StatusAggregatorImpl.DEFAULT)
                .define("b", StatusAggregatorImpl.DEFAULT)
                .define("c", StatusAggregatorImpl.DEFAULT)
                .define("d", StatusAggregatorImpl.DEFAULT)
                .relation("b", "a")
                .relation("c", "a")
                .relation("d", "a");
        configExceptionHandler(builder, false);
    }

    @Test
    // Empty Graph.
    public void emptyConfig() {
        Assert.assertTrue("Config should be listed as 'empty'.", HealthConfigImpl.builder().empty().isEmpty());
    }

    @Test
    // Single node with a loop.
    public void selfReferenceRejected() {
        HealthConfigImpl.Builder builder = HealthConfigImpl.builder()
                .define("a", StatusAggregatorImpl.DEFAULT)
                .relation("a", "a");
        configExceptionHandler(builder, true);
    }

    @Test
    // Cycle added at end of graph.
    public void simpleCycle() {
        HealthConfigImpl.Builder builder = HealthConfigImpl.builder()
                .define("a", StatusAggregatorImpl.DEFAULT)
                .define("b", StatusAggregatorImpl.DEFAULT)
                .relation("a", "b")
                .relation("b", "a");
        configExceptionHandler(builder, true);
    }

    private void configExceptionHandler(HealthConfigImpl.Builder builder, boolean shouldThrow)  {
        String message = new StringBuilder()
                .append("An exception was ")
                .append(!shouldThrow ? "not " : "")
                .append("expected to be thrown.")
                .toString();
        try {
            builder.build();
        } catch (Exception e) {
            log.error("{}", e);
            Assert.assertTrue(message, shouldThrow);
        }
    }
}

