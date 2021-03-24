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
}
