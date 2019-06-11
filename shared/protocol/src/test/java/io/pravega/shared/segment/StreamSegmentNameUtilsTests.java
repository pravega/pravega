/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.segment;

import io.pravega.test.common.AssertExtensions;
import java.util.List;
import java.util.Stack;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for StreamSegmentNameUtils class.
 */
public class StreamSegmentNameUtilsTests {
    /**
     * Tests the basic batch name generation, with only one level of batches.
     */
    @Test
    public void testSimpleBatchNameGeneration() {
        int transactionCount = 100;
        String segmentName = "foo";
        String parentName = StreamSegmentNameUtils.getParentStreamSegmentName(segmentName);
        Assert.assertNull("getParentStreamSegmentName() extracted a parent name when none was expected.", parentName);

        for (int i = 0; i < transactionCount; i++) {
            String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
            AssertExtensions.assertNotNullOrEmpty("getTransactionNameFromId() did not return any Segment Name.", transactionName);
            AssertExtensions.assertGreaterThan("getTransactionNameFromId() returned a Segment Name that is shorter than the base.", segmentName.length(), transactionName.length());

            parentName = StreamSegmentNameUtils.getParentStreamSegmentName(transactionName);
            assertEquals("getParentStreamSegmentName() generated an unexpected value for parent.", segmentName, parentName);
        }
    }

    /**
     * Tests recursive batch generation. This is not a direct requirement or in any way represents how the service works,
     * but it is good to test the principles of batch generation (i.e., only look at the last part of a segment name and
     * ignore the first part).
     */
    @Test
    public void testRecursiveBatchNameGeneration() {
        int recursionCount = 10;
        Stack<String> names = new Stack<>();
        names.push("foo"); // Base segment.
        for (int i = 0; i < recursionCount; i++) {
            // Generate a batch name for the last generated name.
            names.push(StreamSegmentNameUtils.getTransactionNameFromId(names.peek(), UUID.randomUUID()));
        }

        // Make sure we can retrace our roots.
        String lastName = names.pop();
        while (names.size() > 0) {
            String expectedName = names.pop();
            String actualName = StreamSegmentNameUtils.getParentStreamSegmentName(lastName);
            assertEquals("Unexpected parent name.", expectedName, actualName);
            lastName = expectedName;
        }

        Assert.assertNull("Unexpected parent name when none was expected.", StreamSegmentNameUtils.getParentStreamSegmentName(lastName));
    }

    @Test
    public void testSegmentId() {
        // compute segment id and then extract primary and secondary ids
        long segmentId = StreamSegmentNameUtils.computeSegmentId(10, 14);
        assertEquals(10, StreamSegmentNameUtils.getSegmentNumber(segmentId));
        assertEquals(14, StreamSegmentNameUtils.getEpoch(segmentId));

        AssertExtensions.assertThrows("Negative integers not allowed", () -> StreamSegmentNameUtils.computeSegmentId(-1, 10),
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void testQualifiedStreamSegmentName() {
        long segmentId = StreamSegmentNameUtils.computeSegmentId(10, 100);
        String qualifiedName = StreamSegmentNameUtils.getQualifiedStreamSegmentName("scope", "stream", segmentId);

        UUID transactionId = UUID.randomUUID();
        String txnSegment = StreamSegmentNameUtils.getTransactionNameFromId(qualifiedName, transactionId);
        assertTrue(StreamSegmentNameUtils.isTransactionSegment(txnSegment));
        assertEquals(qualifiedName, StreamSegmentNameUtils.getParentStreamSegmentName(txnSegment));

        String primary = StreamSegmentNameUtils.extractPrimaryStreamSegmentName(qualifiedName);
        assertEquals("scope/stream/10", primary);

        String primaryFromTxn = StreamSegmentNameUtils.extractPrimaryStreamSegmentName(txnSegment);
        assertEquals("scope/stream/10", primaryFromTxn);
    }

    @Test
    public void testSegmentTokens() {
        long segmentId = StreamSegmentNameUtils.computeSegmentId(10, 100);
        String qualifiedName = StreamSegmentNameUtils.getQualifiedStreamSegmentName("scope", "stream", segmentId);
        List<String> tokens = StreamSegmentNameUtils.extractSegmentTokens(qualifiedName);
        assertEquals(3, tokens.size());
        assertEquals("scope", tokens.get(0));
        assertEquals("stream", tokens.get(1));
        assertEquals(Long.toString(segmentId), tokens.get(2));
    }

    @Test
    public void testMetadataSegmentName() {
        Assert.assertEquals("_system/containers/metadata_123", StreamSegmentNameUtils.getMetadataSegmentName(123));
        AssertExtensions.assertThrows(
                "getMetadataSegmentName allowed negative container ids.",
                () -> StreamSegmentNameUtils.getMetadataSegmentName(-1),
                ex -> ex instanceof IllegalArgumentException);
    }
    
    @Test
    public void testTableSegmentName() {
        String name = StreamSegmentNameUtils.getQualifiedTableName("scope", "tok1", "tok2", "tok3");
        assertEquals("scope/_tables/tok1/tok2/tok3", name);
        assertTrue(StreamSegmentNameUtils.isTableSegment(name));
        List<String> tokens = StreamSegmentNameUtils.extractTableSegmentTokens(name);
        
        assertEquals(4, tokens.size());
        assertEquals("scope", tokens.get(0));
        assertEquals("tok1", tokens.get(1));
        assertEquals("tok2", tokens.get(2));
        assertEquals("tok3", tokens.get(3));

        AssertExtensions.assertThrows("No tokens supplied", () -> StreamSegmentNameUtils.getQualifiedTableName("scope"), 
                e -> e instanceof IllegalArgumentException);
    }
}
