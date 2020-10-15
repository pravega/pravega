/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.security.auth;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AuthorizationResourceImplTest {
    private final AuthorizationResourceImpl objectUnderTest = new AuthorizationResourceImpl();

    @Test
    public void testOfScopesReturnsValidResourceStr() {
        assertEquals("prn::/", objectUnderTest.ofScopes());
    }

    @Test
    public void testOfAScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("prn::/scope:testScopeName", objectUnderTest.ofScope("testScopeName"));
    }

    @Test
    public void testOfStreamsInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("prn::/scope:testScopeName", objectUnderTest.ofStreamsInScope("testScopeName"));
    }

    @Test (expected = NullPointerException.class)
    public void testOfStreamsInScopeThrowsExceptionWhenInputIsNull() {
        objectUnderTest.ofStreamsInScope(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testOfStreamsInScopeThrowsExceptionWhenInputIsEmpty() {
        objectUnderTest.ofStreamsInScope("");
    }

    @Test
    public void testOfAStreamInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("prn::/scope:testScopeName/stream:testStreamName",
                objectUnderTest.ofStreamInScope("testScopeName", "testStreamName"));
    }

    @Test (expected = NullPointerException.class)
    public void testOfAStreamInScopeThrowsExceptionWhenStreamNameIsNull() {
        objectUnderTest.ofStreamInScope("testScopeName", null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testOfAStreamInScopeThrowsExceptionWhenStreamNameIsEmpty() {
        objectUnderTest.ofStreamInScope("testScopeName", "");
    }

    @Test
    public void testOfReaderGroupsInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("prn::/scope:testScopeName", objectUnderTest.ofReaderGroupsInScope("testScopeName"));
    }

    @Test
    public void testOfAReaderGroupInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("prn::/scope:testScopeName/reader-group:readerGroupName",
                objectUnderTest.ofReaderGroupInScope("testScopeName", "readerGroupName"));
    }

    @Test
    public void testOfAKvtableInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("prn::/scope:testScopeName/key-value-table:kvtName",
                objectUnderTest.ofKeyValueTableInScope("testScopeName", "kvtName"));
    }
}
