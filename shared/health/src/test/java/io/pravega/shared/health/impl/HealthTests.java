/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health.impl;

import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

public class HealthTests {

    /**
     * Test that a {@link Health} object cannot be used when it is in a logically invalid state, i.e. it reports 'ready'
     * but has an {@link Status#UNKNOWN} or {@link Status#DOWN}. See {@link Health#isAlive} or {@link Health#isReady()}
     * for further explanation on these incompatible states.
     */
    @Test
    public void testInvalidReadyState() {
        Health readyAndDown = Health.builder().ready(true).status(Status.DOWN).build();
        AssertExtensions.assertThrows("isReady() should throw if the Health result is ready but DOWN..",
                () -> readyAndDown.isReady(),
                ex -> ex instanceof RuntimeException);
        Health readyAndNotAlive = Health.builder().ready(true).alive(false).status(Status.UP).build();
        AssertExtensions.assertThrows("isReady() should throw if the Health result is ready but not alive.",
                () -> readyAndNotAlive.isReady(),
                ex -> ex instanceof RuntimeException);
    }

    /**
     * Tests that the default/empty {@link Health} reports the expected readiness result.
     */
    @Test
    public void testDefaultReadyLogic() {
        Health health = Health.builder().build();
        Assert.assertEquals("isReady() should be false by default if no Status is set.", false, health.isReady());
        health = Health.builder().status(Status.UP).build();
        Assert.assertEquals("isReady() should be true by default if an UP Status is supplied.", true, health.isReady());
    }

    /**
     * Test that a {@link Health} object cannot be used when it is in a logically invalid state, i.e. it reports 'alive'
     * but has an {@link Status#UNKNOWN} or {@link Status#DOWN}. See {@link Health#isAlive} or {@link Health#isReady()}
     * for further explanation on these incompatible states.
     */
    @Test
    public void testInvalidAliveState() {
        Health aliveAndDown = Health.builder().alive(true).status(Status.DOWN).build();
        AssertExtensions.assertThrows("isAlive() should throw if the Health result is alive but DOWN..",
                () -> aliveAndDown.isAlive(),
                ex -> ex instanceof RuntimeException);
        Health notAliveAndUp = Health.builder().alive(false).alive(false).status(Status.UP).build();
        AssertExtensions.assertThrows("isAlive() should throw if the Health result is not alive but UP.",
                () -> notAliveAndUp.isAlive(),
                ex -> ex instanceof RuntimeException);
        Health notAliveAndReady = Health.builder().alive(false).alive(false).status(Status.UP).build();
        AssertExtensions.assertThrows("isAlive() should throw if the Health result is ready but not alive.",
                () -> notAliveAndReady.isAlive(),
                ex -> ex instanceof RuntimeException);
    }

    /**
     * Tests that the default/empty {@link Health} reports the expected liveness result.
     */
    @Test
    public void testDefaultAliveLogic() {
        Health health = Health.builder().build();
        Assert.assertEquals("isAlive() should be false by default if no Status is set.", false, health.isAlive());
        health = Health.builder().status(Status.UP).build();
        Assert.assertEquals("isAlive() should be true by default if an UP Status is supplied.", true, health.isAlive());
    }
}
