/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared;

import io.pravega.test.common.AssertExtensions;
import java.util.List;
import java.util.Stack;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.shared.NameUtils.TAG_EPOCH;
import static io.pravega.shared.NameUtils.TAG_SCOPE;
import static io.pravega.shared.NameUtils.TAG_SEGMENT;
import static io.pravega.shared.NameUtils.TAG_STREAM;
import static io.pravega.shared.NameUtils.TAG_WRITER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for NameUtils class.
 */
public class StreamSegmentNameUtilsTests {
    /**
     * Tests the basic batch name generation, with only one level of batches.
     */
    @Test
    public void testSimpleBatchNameGeneration() {
        int transactionCount = 100;
        String segmentName = "foo";
        String parentName = NameUtils.getParentStreamSegmentName(segmentName);
        Assert.assertNull("getParentStreamSegmentName() extracted a parent name when none was expected.", parentName);

        for (int i = 0; i < transactionCount; i++) {
            String transactionName = NameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
            AssertExtensions.assertNotNullOrEmpty("getTransactionNameFromId() did not return any Segment Name.", transactionName);
            AssertExtensions.assertGreaterThan("getTransactionNameFromId() returned a Segment Name that is shorter than the base.", segmentName.length(), transactionName.length());

            parentName = NameUtils.getParentStreamSegmentName(transactionName);
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
            names.push(NameUtils.getTransactionNameFromId(names.peek(), UUID.randomUUID()));
        }

        // Make sure we can retrace our roots.
        String lastName = names.pop();
        while (names.size() > 0) {
            String expectedName = names.pop();
            String actualName = NameUtils.getParentStreamSegmentName(lastName);
            assertEquals("Unexpected parent name.", expectedName, actualName);
            lastName = expectedName;
        }

        Assert.assertNull("Unexpected parent name when none was expected.", NameUtils.getParentStreamSegmentName(lastName));
    }

    @Test
    public void testSegmentId() {
        // compute segment id and then extract primary and secondary ids
        long segmentId = NameUtils.computeSegmentId(10, 14);
        assertEquals(10, NameUtils.getSegmentNumber(segmentId));
        assertEquals(14, NameUtils.getEpoch(segmentId));

        AssertExtensions.assertThrows("Negative integers not allowed", () -> NameUtils.computeSegmentId(-1, 10),
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void testTransientStreamSegmentName() {
        long segmentId = NameUtils.computeSegmentId(10, 100);
        String qualifiedName = NameUtils.getQualifiedStreamSegmentName("scope", "stream", segmentId);

        UUID writerId = UUID.randomUUID();
        String transientSegment = NameUtils.getTransientNameFromId(qualifiedName, writerId);
        assertTrue(NameUtils.isTransientSegment(transientSegment));
        assertEquals(qualifiedName, NameUtils.getParentStreamSegmentName(transientSegment));

        String primary = NameUtils.extractPrimaryStreamSegmentName(qualifiedName);
        assertEquals("scope/stream/10", primary);

        String primaryFromTransient = NameUtils.extractPrimaryStreamSegmentName(transientSegment);
        assertEquals("scope/stream/10", primaryFromTransient);
    }

    @Test
    public void testQualifiedStreamSegmentName() {
        long segmentId = NameUtils.computeSegmentId(10, 100);
        String qualifiedName = NameUtils.getQualifiedStreamSegmentName("scope", "stream", segmentId);

        UUID transactionId = UUID.randomUUID();
        String txnSegment = NameUtils.getTransactionNameFromId(qualifiedName, transactionId);
        assertTrue(NameUtils.isTransactionSegment(txnSegment));
        assertEquals(qualifiedName, NameUtils.getParentStreamSegmentName(txnSegment));

        String primary = NameUtils.extractPrimaryStreamSegmentName(qualifiedName);
        assertEquals("scope/stream/10", primary);

        String primaryFromTxn = NameUtils.extractPrimaryStreamSegmentName(txnSegment);
        assertEquals("scope/stream/10", primaryFromTxn);
    }

    @Test
    public void testQualifiedTableSegmentName() {
        long segmentId = NameUtils.computeSegmentId(10, 100);
        String qualifiedName = NameUtils.getQualifiedTableSegmentName("scope1", "keyvaluetable1", segmentId);
        String expectedName = "scope1/keyvaluetable1_kvtable/10.#epoch.100";
        assertEquals(expectedName, qualifiedName);
    }

    @Test
    public void testSegmentTokens() {
        long segmentId = NameUtils.computeSegmentId(10, 100);
        String qualifiedName = NameUtils.getQualifiedStreamSegmentName("scope", "stream", segmentId);
        List<String> tokens = NameUtils.extractSegmentTokens(qualifiedName);
        assertEquals(3, tokens.size());
        assertEquals("scope", tokens.get(0));
        assertEquals("stream", tokens.get(1));
        assertEquals(Long.toString(segmentId), tokens.get(2));
    }

    @Test
    public void testMetadataSegmentName() {
        Assert.assertEquals("_system/containers/metadata_123", NameUtils.getMetadataSegmentName(123));
        AssertExtensions.assertThrows(
                "getMetadataSegmentName allowed negative container ids.",
                () -> NameUtils.getMetadataSegmentName(-1),
                ex -> ex instanceof IllegalArgumentException);
    }
    
    @Test
    public void testTableSegmentName() {
        String name = NameUtils.getQualifiedTableName("scope", "tok1", "tok2", "tok3");
        assertEquals("scope/_tables/tok1/tok2/tok3", name);
        assertTrue(NameUtils.isTableSegment(name));
        List<String> tokens = NameUtils.extractTableSegmentTokens(name);
        
        assertEquals(4, tokens.size());
        assertEquals("scope", tokens.get(0));
        assertEquals("tok1", tokens.get(1));
        assertEquals("tok2", tokens.get(2));
        assertEquals("tok3", tokens.get(3));

        AssertExtensions.assertThrows("No tokens supplied", () -> NameUtils.getQualifiedTableName("scope"), 
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void testGetAttributeSegmentName() {
        String name = NameUtils.getAttributeSegmentName("foo");
        assertTrue(NameUtils.isAttributeSegment(name));
        AssertExtensions.assertThrows(
                "getAttributeSegmentName did not fail to add the attribute suffix.",
                () -> NameUtils.getAttributeSegmentName(name),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testGetHeaderSegmentName() {
        String name = NameUtils.getHeaderSegmentName("foo");
        AssertExtensions.assertThrows(
                "getHeaderSegmentName did not fail to add the header suffix.",
                () -> NameUtils.getHeaderSegmentName(name),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testGetSegmentNameFromHeader() {
        String name = NameUtils.getSegmentNameFromHeader(NameUtils.getHeaderSegmentName("foo"));
        AssertExtensions.assertThrows(
                "getSegmentNameFromHeader did not fail to remove the header suffix.",
                () -> NameUtils.getSegmentNameFromHeader("foo"),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testGetSegmentNameChunkName() {
        String name = NameUtils.getSegmentChunkName("foo", 0);
        AssertExtensions.assertThrows(
                "getSegmentChunkName did not fail to concatenate the offset.",
                () -> NameUtils.getSegmentChunkName(name, 0),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testGetScopedStreamName() {
        String name = NameUtils.getScopedStreamName("scope", "stream");
        Assert.assertTrue(name.equals("scope/stream"));
        name = NameUtils.getScopedStreamName("", "stream");
        Assert.assertTrue(name.equals("stream"));
        name = NameUtils.getScopedStreamName(null, "stream");
        Assert.assertTrue(name.equals("stream"));
    }

    @Test
    public void testComputeSegmentId() {
        long sid = NameUtils.computeSegmentId(1, 1);
        Assert.assertEquals(sid, (0x1L << 32) + 1);

        AssertExtensions.assertThrows(
                "Accepted a negative epoch",
                () -> NameUtils.computeSegmentId(1, -1),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testExtractSegmentTokens() {
        String name  = NameUtils.getQualifiedStreamSegmentName("scope", "stream", 0);
        List<String> tokens = NameUtils.extractSegmentTokens(name);
        Assert.assertEquals(tokens.size(), 3);
        Assert.assertTrue(tokens.get(0).equals("scope"));
        Assert.assertTrue(tokens.get(1).equals("stream"));
        Assert.assertTrue(tokens.get(2).equals("0"));

        name  = NameUtils.getQualifiedStreamSegmentName("", "stream", 0);
        tokens = NameUtils.extractSegmentTokens(name);
        Assert.assertEquals(tokens.size(), 2);
        Assert.assertTrue(tokens.get(0).equals("stream"));
        Assert.assertTrue(tokens.get(1).equals("0"));

        name  = NameUtils.getQualifiedStreamSegmentName(null, "stream", 0);
        tokens = NameUtils.extractSegmentTokens(name);
        Assert.assertEquals(tokens.size(), 2);
        Assert.assertTrue(tokens.get(0).equals("stream"));
        Assert.assertTrue(tokens.get(1).equals("0"));
    }

    @Test
    public void testSegmentTags() {
        String[] tags = NameUtils.segmentTags("scope/stream/segment.#epoch.1552095534");
        assertEquals(8, tags.length);
        assertEquals(TAG_SCOPE, tags[0]);
        assertEquals("scope", tags[1]);
        assertEquals(TAG_STREAM, tags[2]);
        assertEquals("stream", tags[3]);
        assertEquals(TAG_SEGMENT, tags[4]);
        assertEquals("segment", tags[5]);
        assertEquals(TAG_EPOCH, tags[6]);
        assertEquals("1552095534", tags[7]);

        //test missing scope and epoch
        tags = NameUtils.segmentTags("stream/segment");
        assertEquals(8, tags.length);
        assertEquals(TAG_SCOPE, tags[0]);
        assertEquals("default", tags[1]);
        assertEquals(TAG_STREAM, tags[2]);
        assertEquals("stream", tags[3]);
        assertEquals(TAG_SEGMENT, tags[4]);
        assertEquals("segment", tags[5]);
        assertEquals(TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);

        // test missing scope, stream and epoch
        tags = NameUtils.segmentTags("segment");
        assertEquals(8, tags.length);
        assertEquals(TAG_SCOPE, tags[0]);
        assertEquals("default", tags[1]);
        assertEquals(TAG_STREAM, tags[2]);
        assertEquals("default", tags[3]);
        assertEquals(TAG_SEGMENT, tags[4]);
        assertEquals("segment", tags[5]);
        assertEquals(TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);
    }

    @Test
    public void testSegmentTagsForTransactions() {
        long segmentId = NameUtils.computeSegmentId(10, 100);
        String qualifiedName = NameUtils.getQualifiedStreamSegmentName("scope", "stream", segmentId);
        UUID transactionId = UUID.randomUUID();
        String txnSegment = NameUtils.getTransactionNameFromId(qualifiedName, transactionId);

        String[] tags = NameUtils.segmentTags(txnSegment);
        assertEquals(8, tags.length);
        assertEquals(TAG_SCOPE, tags[0]);
        assertEquals("scope", tags[1]);
        assertEquals(TAG_STREAM, tags[2]);
        assertEquals("stream", tags[3]);
        assertEquals(TAG_SEGMENT, tags[4]);
        assertEquals("10", tags[5]);
        assertEquals(TAG_EPOCH, tags[6]);
        assertEquals("100", tags[7]);
    }

    @Test
    public void testSegmentTagsWithWriterId() {

        String[] tags = NameUtils.segmentTags("scope/stream/segment.#epoch.1552095534", "writer01");
        assertEquals(10, tags.length);
        assertEquals(TAG_SCOPE, tags[0]);
        assertEquals("scope", tags[1]);
        assertEquals(TAG_STREAM, tags[2]);
        assertEquals("stream", tags[3]);
        assertEquals(TAG_SEGMENT, tags[4]);
        assertEquals("segment", tags[5]);
        assertEquals(TAG_EPOCH, tags[6]);
        assertEquals("1552095534", tags[7]);
        assertEquals(TAG_WRITER, tags[8]);
        assertEquals("writer01", tags[9]);

        //test missing scope and epoch
        tags = NameUtils.segmentTags("stream/segment", "writer01");
        assertEquals(10, tags.length);
        assertEquals(TAG_SCOPE, tags[0]);
        assertEquals("default", tags[1]);
        assertEquals(TAG_STREAM, tags[2]);
        assertEquals("stream", tags[3]);
        assertEquals(TAG_SEGMENT, tags[4]);
        assertEquals("segment", tags[5]);
        assertEquals(TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);
        assertEquals(TAG_WRITER, tags[8]);
        assertEquals("writer01", tags[9]);

        // test missing scope, stream and epoch
        tags = NameUtils.segmentTags("segment", "writer01");
        assertEquals(10, tags.length);
        assertEquals(TAG_SCOPE, tags[0]);
        assertEquals("default", tags[1]);
        assertEquals(TAG_STREAM, tags[2]);
        assertEquals("default", tags[3]);
        assertEquals(TAG_SEGMENT, tags[4]);
        assertEquals("segment", tags[5]);
        assertEquals(TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);
        assertEquals(TAG_WRITER, tags[8]);
        assertEquals("writer01", tags[9]);

    }

    @Test
    public void testWriterTags() {
        String[] tags = NameUtils.writerTags("writer-01");
        assertEquals(2, tags.length);
        assertEquals(TAG_WRITER, tags[0]);
        assertEquals("writer-01", tags[1]);
        // validate error conditions.
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.writerTags(""));
    }
}
