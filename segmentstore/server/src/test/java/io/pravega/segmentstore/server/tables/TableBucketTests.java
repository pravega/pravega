/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.UUID;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableBucket} class.
 */
public class TableBucketTests {
    /**
     * Tests the ability to build new Table Buckets.
     */
    @Test
    public void testBuilder() {
        final int nodeCount = 5;
        val empty = TableBucket.builder().build();
        Assert.assertTrue("Expecting empty path.", empty.getNodes().isEmpty());
        Assert.assertNull("Expecting no last node.", empty.getLastNode());

        val builder = TableBucket.builder();
        Assert.assertNull("Expecting builder.getLastNode() to be null.", builder.getLastNode());
        val nodes = new ArrayList<TableBucket.Node>();
        for (int i = 0; i < nodeCount; i++) {
            val node = new TableBucket.Node(i < nodeCount - 1, new UUID(i, i), i + 1);
            nodes.add(node);
            builder.node(node);
            Assert.assertEquals("Unexpected builder.getLastNode.", node, builder.getLastNode());
        }

        val tb = builder.build();
        AssertExtensions.assertListEquals("Unexpected node list.", nodes, tb.getNodes(), Object::equals);
        Assert.assertEquals("Unexpected last node.", nodes.get(nodes.size() - 1), tb.getLastNode());
    }

    /**
     * Tests the equals() and hashCode() methods.
     */
    @Test
    public void testEquals() {
        val c1 = generate(5, true);
        val c2 = generate(5, true);
        val i1 = generate(3, false);
        val i2 = generate(3, false);

        // Empty buckets are always equal.
        val e1 = generate(0, true);
        val e2 = generate(0, false);
        assertEquals("empty", e1, e2);

        // Identical buckets.
        assertCloneEquals("Identical", c1);
        assertCloneEquals("Identical", i1);

        // Non-equal buckets.
        assertNotEquals("Non-equal complete.", c1, c2);
        assertNotEquals("Non-equal incomplete.", i1, i2);
        assertNotEquals("Non-equal.", c1, i1);
    }

    private void assertCloneEquals(String message, TableBucket b) {
        val builder = TableBucket.builder();
        for (val n : b.getNodes()) {
            builder.node(new TableBucket.Node(n.isIndexNode(), n.getKey(), n.getValue()));
        }

        val clone = builder.build();
        assertEquals(message, b, clone);
    }

    private void assertEquals(String message, TableBucket b1, TableBucket b2) {
        int h1 = b1.hashCode();
        int h2 = b2.hashCode();
        Assert.assertEquals("Different hash codes for: " + message, h1, h2);
        Assert.assertTrue("Unexpected equals() for: " + message, b1.equals(b2));
        Assert.assertTrue("Unexpected equals() for: " + message, b2.equals(b1));
    }

    private void assertNotEquals(String message, TableBucket b1, TableBucket b2) {
        Assert.assertFalse(message, b1.equals(b2));
        Assert.assertFalse(message, b2.equals(b1));
    }


    private TableBucket generate(int nodeCount, boolean complete) {
        val builder = TableBucket.builder();
        for (int i = 0; i < nodeCount; i++) {
            boolean indexNode = (i < nodeCount - 1) || !complete;
            builder.node(new TableBucket.Node(indexNode, UUID.randomUUID(), i + 1));
        }
        return builder.build();
    }
}
