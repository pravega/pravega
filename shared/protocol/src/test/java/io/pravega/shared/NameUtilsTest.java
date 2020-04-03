/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared;

import io.pravega.test.common.AssertExtensions;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

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
    public void testUserScopeNameVerifier() {
        NameUtils.validateUserScopeName("stream123");
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.validateUserScopeName("_stream"));
        AssertExtensions.assertThrows(NullPointerException.class, () -> NameUtils.validateUserScopeName(null));
    }

    @Test
    public void testScopeNameVerifier() {

        NameUtils.validateScopeName("_systemscope123");
        NameUtils.validateScopeName("userscope123");
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.validateScopeName("system_scope"));
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> NameUtils.validateScopeName("system/scope"));
        AssertExtensions.assertThrows(NullPointerException.class, () -> NameUtils.validateScopeName(null));

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
}
