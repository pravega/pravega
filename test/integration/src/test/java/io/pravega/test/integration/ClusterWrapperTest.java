/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.test.integration.demo.ClusterWrapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.*;

@Slf4j
public class ClusterWrapperTest {

    @Test
    public void testSetsDefaultValuesWhenBuilderSpecifiesNoValues() {
        ClusterWrapper objectUnderTest = ClusterWrapper.builder().build();

        assertFalse(objectUnderTest.isAuthEnabled());
        assertTrue(objectUnderTest.isRgWritesWithReadPermEnabled());
        assertEquals(600, objectUnderTest.getTokenTtlInSeconds());
        assertEquals(4, objectUnderTest.getContainerCount());
        assertTrue(objectUnderTest.getTokenSigningKeyBasis().length() > 0);
    }
}
