/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.security;

import org.junit.Test;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class TLSConfigChangeEventConsumerTests {

    @Test (expected = NullPointerException.class)
    public void testNullCtorArgumentsAreRejected() {
        new TLSConfigChangeEventConsumer(new AtomicReference<>(null), null, null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testEmptyPathToCertificateFileIsRejected() {
        TLSConfigChangeEventConsumer subjectUnderTest = new TLSConfigChangeEventConsumer(new AtomicReference<>(null),
                "", "non-existent");
        subjectUnderTest.accept(null);

        assertEquals(1, subjectUnderTest.getNumOfConfigChangesSinceStart());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testEmptyPathToKeyFileIsRejected() {
        TLSConfigChangeEventConsumer subjectUnderTest = new TLSConfigChangeEventConsumer(new AtomicReference<>(null),
                "non-existent", "");
        subjectUnderTest.accept(null);
        assertEquals(1, subjectUnderTest.getNumOfConfigChangesSinceStart());
    }
}
