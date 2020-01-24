/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit tests for the AuthResourceRepresentation class.
 */
public class AuthResourceRepresentationTest {

    @Test
    public void testOfScopesReturnsValidResourceStr() {
        assertEquals("/", AuthResourceRepresentation.ofScopes());
    }

    @Test
    public void testOfAScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("testScopeName", AuthResourceRepresentation.ofScope("testScopeName"));
    }

    @Test
    public void testOfStreamsInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("testScopeName", AuthResourceRepresentation.ofStreamsInScope("testScopeName"));
    }

    @Test (expected = NullPointerException.class)
    public void testOfStreamsInScopeThrowsExceptionWhenInputIsNull() {
        AuthResourceRepresentation.ofStreamsInScope(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testOfStreamsInScopeThrowsExceptionWhenInputIsEmpty() {
        AuthResourceRepresentation.ofStreamsInScope("");
    }

    @Test
    public void testOfAStreamInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("testScopeName/testStreamName",
                AuthResourceRepresentation.ofStreamInScope("testScopeName", "testStreamName"));
    }

    @Test (expected = NullPointerException.class)
    public void testOfAStreamInScopeThrowsExceptionWhenStreamNameIsNull() {
        AuthResourceRepresentation.ofStreamInScope("testScopeName", null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testOfAStreamInScopeThrowsExceptionWhenStreamNameIsEmpty() {
        AuthResourceRepresentation.ofStreamInScope("testScopeName", "");
    }

    @Test
    public void testOfReaderGroupsInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("scopeName", AuthResourceRepresentation.ofReaderGroupsInScope("scopeName"));
    }

    @Test
    public void testOfAReaderGroupInScopeReturnsValidResourceStrWhenInputIsLegal() {
        assertEquals("scopeName/readerGroupName",
                AuthResourceRepresentation.ofReaderGroupInScope("scopeName", "readerGroupName"));
    }
}