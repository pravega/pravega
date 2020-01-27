/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ControllerEventSerializer} class.
 */
public class ControllerEventSerializerTests {
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    @Test
    public void testAbortEvent() {
        testClass(() -> new AbortEvent(SCOPE, STREAM, 123, UUID.randomUUID()));
    }

    @Test
    public void testAutoScaleEvent() {
        testClass(() -> new AutoScaleEvent(SCOPE, STREAM, 12345L, AutoScaleEvent.DOWN, 434L, 2, true, 684L));
    }

    @Test
    public void testCommitEvent() {
        testClass(() -> new CommitEvent(SCOPE, STREAM, 123));
    }

    @Test
    public void testDeleteStreamEvent() {
        testClass(() -> new DeleteStreamEvent(SCOPE, STREAM, 123L, 345L));
    }

    @Test
    public void testScaleOpEvent() {
        testClass(() -> new ScaleOpEvent(SCOPE, STREAM, Arrays.asList(1L, 2L, 3L),
                new ArrayList<>(Collections.singletonMap(10.0, 20.0).entrySet()),
                true, 45L, 654L));
    }

    @Test
    public void testSealStreamEvent() {
        testClass(() -> new SealStreamEvent(SCOPE, STREAM, 123L));
    }

    @Test
    public void testTruncateStreamEvent() {
        testClass(() -> new TruncateStreamEvent(SCOPE, STREAM, 123L));
    }

    @Test
    public void testUpdateStreamEvent() {
        testClass(() -> new UpdateStreamEvent(SCOPE, STREAM, 123L));
    }

    private <T extends ControllerEvent> void testClass(Supplier<T> generateInstance) {
        val s = new ControllerEventSerializer();
        T baseInstance = generateInstance.get();
        val serialization = s.toByteBuffer(baseInstance);
        @SuppressWarnings("unchecked")
        T newInstance = (T) s.fromByteBuffer(serialization);
        Assert.assertEquals(baseInstance, newInstance);
    }
}
