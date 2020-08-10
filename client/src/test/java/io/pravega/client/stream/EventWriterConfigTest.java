/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;

public class EventWriterConfigTest {


    @Test
    public void testValidValues() {
        EventWriterConfig config = EventWriterConfig.builder()
                .automaticallyNoteTime(true)
                .backoffMultiple(2)
                .enableConnectionPooling(false)
                .initialBackoffMillis(100)
                .maxBackoffMillis(1000)
                .retryAttempts(3)
                .transactionTimeoutTime(100000)
                .build();
        assertEquals(true, config.isAutomaticallyNoteTime());
        assertEquals(2, config.getBackoffMultiple());
        assertEquals(false, config.isEnableConnectionPooling());
        assertEquals(100, config.getInitialBackoffMillis());
        assertEquals(1000, config.getMaxBackoffMillis());
        assertEquals(3, config.getRetryAttempts());
        assertEquals(100000, config.getTransactionTimeoutTime());
    }

    @Test
    public void testInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> EventWriterConfig.builder().backoffMultiple(-2).build());
        assertThrows(IllegalArgumentException.class, () -> EventWriterConfig.builder().initialBackoffMillis(-2).build());
        assertThrows(IllegalArgumentException.class, () -> EventWriterConfig.builder().maxBackoffMillis(-2).build());
        assertThrows(IllegalArgumentException.class, () -> EventWriterConfig.builder().retryAttempts(-2).build());
        assertThrows(IllegalArgumentException.class, () -> EventWriterConfig.builder().transactionTimeoutTime(-2).build());
    }

}
