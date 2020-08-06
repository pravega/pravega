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

/**
 * Unit tests for the AuthResourceRepresentation class.
 */
public class LegacyAuthorizationResourceImplTest {

    private final LegacyAuthorizationResourceImpl objectUnderTest = new LegacyAuthorizationResourceImpl();

    @Test
    public void testOfScopesReturnsValidResourceStr() {
        assertEquals("/", objectUnderTest.ofScopes());
    }

    @Test
    public void testOfAScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("testScopeName", objectUnderTest.ofScope("testScopeName"));
    }

    @Test
    public void testOfStreamsInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("testScopeName", objectUnderTest.ofStreamsInScope("testScopeName"));
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
        assertEquals("testScopeName/testStreamName",
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
        assertEquals("scopeName", objectUnderTest.ofReaderGroupsInScope("scopeName"));
    }

    @Test
    public void testOfAReaderGroupInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("scopeName/readerGroupName",
                objectUnderTest.ofReaderGroupInScope("scopeName", "readerGroupName"));
    }
}