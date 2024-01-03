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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NameUtilsTest {

    //Ensure each test completes within 10 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    @Test
    public void testUserStreamNameVerifier() {
        testUserStreamNameVerifier(NameUtils::validateUserStreamName);
    }

    private void testUserStreamNameVerifier(Function<String, String> toTest) {
        Assert.assertEquals("stream123", toTest.apply("stream123"));
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> toTest.apply("_stream"));
        AssertExtensions.assertThrows(NullPointerException.class, () -> toTest.apply(null));
        Assert.assertEquals("a-b-c", toTest.apply("a-b-c"));
        Assert.assertEquals("1.2.3", toTest.apply("1.2.3"));
    }

    @Test
    public void testUserKeyValueTableNameVerifier() {
        // Currently, the same set of rules apply as for User Stream Names.
        testUserStreamNameVerifier(NameUtils::validateUserKeyValueTableName);
    }

    @Test
    public void testGetScopedKeyValueTableName() {
        String scope = "scope";
        String kvt = "kvt";
        String scopedName = NameUtils.getScopedKeyValueTableName(scope, kvt);
        Assert.assertTrue(scopedName.startsWith(scope));
        Assert.assertTrue(scopedName.endsWith(kvt));
        val tokens = NameUtils.extractScopedNameTokens(scopedName);
        Assert.assertEquals(2, tokens.size());
        Assert.assertEquals(scope, tokens.get(0));
        Assert.assertEquals(kvt, tokens.get(1));
        AssertExtensions.assertThrows("", () -> NameUtils.extractScopedNameTokens(scope), ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("", () -> NameUtils.extractScopedNameTokens("a/b/c"), ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testGetScopedReaderGroupName() {
        String scope = "scope";
        String readergroup = "readerGroup";
        String scopedName = NameUtils.getScopedReaderGroupName(scope, readergroup);
        Assert.assertTrue(scopedName.startsWith(scope));
        Assert.assertTrue(scopedName.endsWith(readergroup));
        val tokens = NameUtils.extractScopedNameTokens(scopedName);
        Assert.assertEquals(2, tokens.size());
        Assert.assertEquals(scope, tokens.get(0));
        Assert.assertEquals(readergroup, tokens.get(1));
        AssertExtensions.assertThrows("", () -> NameUtils.extractScopedNameTokens(scope), ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("", () -> NameUtils.extractScopedNameTokens("a/b/c"), ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testStreamNameVerifier() {
        NameUtils.validateStreamName("_systemstream123");
        NameUtils.validateStreamName("stream123");
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.validateStreamName("system_stream123"));
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.validateStreamName("stream/123"));
        AssertExtensions.assertThrows(NullPointerException.class, () -> NameUtils.validateStreamName(null));
        NameUtils.validateStreamName("a-b-c");
        NameUtils.validateStreamName("1.2.3");
    }

    @Test
    public void testStreamNameLimit() {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = NameUtils.MAX_NAME_SIZE + 1;
        final String internalName = randomAlphanumeric(targetStringLength);
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> NameUtils.validateStreamName(internalName));
        targetStringLength = NameUtils.MAX_GIVEN_NAME_SIZE + 1;
        final String externalName = randomAlphanumeric(targetStringLength);
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> NameUtils.validateUserStreamName(externalName));
    }

    @Test
    public void testUserScopeNameVerifier() {
        NameUtils.validateUserScopeName("stream123");
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.validateUserScopeName("_stream"));
        AssertExtensions.assertThrows(NullPointerException.class, () -> NameUtils.validateUserScopeName(null));
        int targetStringLength = NameUtils.MAX_NAME_SIZE + 1;
        final String externalName = randomAlphanumeric(targetStringLength);
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> NameUtils.validateUserScopeName(externalName));
    }

    @Test
    public void testScopeNameVerifier() {

        NameUtils.validateScopeName("_systemscope123");
        NameUtils.validateScopeName("userscope123");
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.validateScopeName("system_scope"));
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.validateScopeName("system/scope"));
        AssertExtensions.assertThrows(NullPointerException.class, () -> NameUtils.validateScopeName(null));
        int targetStringLength = NameUtils.MAX_NAME_SIZE + 1;
        final String internalName = randomAlphanumeric(targetStringLength);
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> NameUtils.validateScopeName(internalName));

    }

    @Test
    public void testReaderGroupNameVerifier() {
        NameUtils.validateReaderGroupName("stream123");
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.validateReaderGroupName("_stream"));
        AssertExtensions.assertThrows(NullPointerException.class, () -> NameUtils.validateReaderGroupName(null));
    }

    @Test
    public void testInternalStreamName() {
        Assert.assertTrue(NameUtils.getInternalNameForStream("stream").startsWith(
                NameUtils.INTERNAL_NAME_PREFIX));
    }

    @Test
    public void testInternalReaderGroupName() {
        Assert.assertTrue(NameUtils.getStreamForReaderGroup("readergroup1").startsWith(
                NameUtils.READER_GROUP_STREAM_PREFIX));
    }

    @Test
    public void testMarkSegmentName() {
        String myStream = "myStream";
        String name = NameUtils.getMarkStreamForStream(myStream);
        assertTrue(name.endsWith(myStream));
        assertTrue(name.startsWith(NameUtils.getMARK_PREFIX()));
    }

    @Test
    public void testGetEpoch() {
        UUID txnId = UUID.fromString("00000065-0000-000a-0000-000000000064");
        assertEquals(101, NameUtils.getEpoch(txnId));
    }

    @Test
    public void testStorageNames() {
        Assert.assertEquals(NameUtils.getStorageMetadataSegmentName(1), "_system/containers/storage_metadata_1");
        Assert.assertEquals(NameUtils.getSystemJournalFileName(2, 3, 4),
                "_system/containers/_sysjournal.epoch3.container2.file4");
        Assert.assertEquals(NameUtils.getSystemJournalSnapshotFileName(5, 6, 7),
                "_system/containers/_sysjournal.epoch6.container5.snapshot7");
        Assert.assertEquals(NameUtils.getSystemJournalSnapshotInfoFileName(42),
                "_system/containers/_sysjournal.container42.snapshot_info");
        Assert.assertEquals(NameUtils.getContainerEpochFileName(0),
                "_system/containers/container_0_epoch");
        Assert.assertTrue(NameUtils.getSegmentChunkName("segment", 8, 9).startsWith("segment.E-8-O-9"));
        Assert.assertEquals(NameUtils.getSegmentReadIndexBlockName("segment", 10), "segment.B-10");
    }

    @Test
    public void testIsContainerMetadataSegmentName() {
        Assert.assertTrue(NameUtils.isMetadataSegmentName("_system/containers/metadata_1"));
        Assert.assertTrue(NameUtils.isMetadataSegmentName("_system/containers/metadata_99"));
        Assert.assertFalse(NameUtils.isMetadataSegmentName("_system/containers/_metadata_1"));
        Assert.assertFalse(NameUtils.isMetadataSegmentName("system/containers/metadata_1"));
    }

    @Test
    public void testIsStorageMetadataSegmentName() {
        Assert.assertTrue(NameUtils.isStorageMetadataSegmentName("_system/containers/storage_metadata_1"));
        Assert.assertTrue(NameUtils.isStorageMetadataSegmentName("_system/containers/storage_metadata_99"));
        Assert.assertFalse(NameUtils.isStorageMetadataSegmentName("_system/containers/_storage_metadata_1"));
        Assert.assertFalse(NameUtils.isStorageMetadataSegmentName("system/containers/storage_metadata_1"));
    }

    @Test
    public void testGetEventProcessorSegmentName() {
        Assert.assertEquals(NameUtils.getEventProcessorSegmentName(0, "test"), "_system/containers/event_processor_test_0");
    }

    @Test
    public void testIsTransientSegment() {
        Assert.assertFalse(NameUtils.isTransientSegment(""));
        Assert.assertTrue(NameUtils.isTransientSegment(getSegmentName("#transient.")));
        Assert.assertFalse(NameUtils.isTransientSegment("scope/stream/transientSegment"));
        Assert.assertTrue(NameUtils.isTransientSegment("#transient.01234567890123456789012345678901"));
        Assert.assertFalse(NameUtils.isTransientSegment("#transient"));
    }

    @Test
    public void testTransientAndTransactionConflicts() {
        String transientSegmentName = getSegmentName("#transient.");
        String transactionSegmentName = getSegmentName("#transaction.");
        // First validate they are valid formats themselves, then ensure each naming convention can't be confused for the other.
        Assert.assertTrue(NameUtils.isTransactionSegment(transactionSegmentName) && !NameUtils.isTransientSegment(transactionSegmentName));
        Assert.assertTrue(NameUtils.isTransientSegment(transientSegmentName) && !NameUtils.isTransactionSegment(transientSegmentName));
    }

    private String getSegmentName(String delimiter) {
        return String.format("scope/stream/transactionSegment%s01234567890123456789012345678901", delimiter);
    }

    @Test
    public void testGetConnectionDetails() {
        Assert.assertEquals("localhost", NameUtils.getConnectionDetails("localhost :12345")[0].trim());
        Assert.assertEquals("12345", NameUtils.getConnectionDetails("localhost :12345")[1].trim());
    }

    @Test
    public void testIndexSegmentName() {
        String scope = "scope";
        String stream = "stream";
        String qualifiedStreamSegmentName = NameUtils.getQualifiedStreamSegmentName(scope, stream, 0L);
        String indexSegmentName = NameUtils.getIndexSegmentName(qualifiedStreamSegmentName);
        Assert.assertTrue("Passed segment is an index segment", NameUtils.isIndexSegment(indexSegmentName));
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.validateStreamName(indexSegmentName));
        AssertExtensions.assertThrows("", () -> NameUtils.getIndexSegmentName(indexSegmentName), ex -> ex instanceof IllegalArgumentException);
    }

    @Test(timeout = 5000)
    public void isUserStreamSegment() {
        testUserStreamVerifier(NameUtils::isUserStreamSegment);
    }

    private void testUserStreamVerifier(Function<String, Boolean> toTest) {
        Assert.assertEquals(Boolean.TRUE, toTest.apply("testScope/testStream/0"));
        Assert.assertEquals(Boolean.FALSE, toTest.apply("_stream/_requestStream/0"));
        Assert.assertEquals(Boolean.FALSE, toTest.apply(null));
        Assert.assertEquals(Boolean.TRUE, toTest.apply("test/a-b-c/1"));
        Assert.assertEquals(Boolean.TRUE, toTest.apply("test/1.2.3/0"));
        Assert.assertEquals(Boolean.FALSE, toTest.apply("test/1.2.3/0#index"));
    }
}
