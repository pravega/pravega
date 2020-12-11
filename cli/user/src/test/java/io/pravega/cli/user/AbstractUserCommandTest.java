/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user;

import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.test.integration.demo.ClusterWrapper;
import lombok.val;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.cli.user.TestUtils.createPravegaCluster;
import static io.pravega.cli.user.TestUtils.setInteractiveConfig;

public abstract class AbstractUserCommandTest {

    protected static final AtomicReference<ClusterWrapper> CLUSTER = new AtomicReference<>();
    protected static final AtomicReference<InteractiveConfig> CONFIG = new AtomicReference<>();
    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    public static void setUpCluster(boolean authEnabled, boolean tlsEnabled) {
        CLUSTER.set(createPravegaCluster(authEnabled, tlsEnabled));
        CLUSTER.get().start();
        setInteractiveConfig(CLUSTER.get().controllerUri().replace("tcp://", "").replace("tls://", ""),
                authEnabled, tlsEnabled, CONFIG);
    }

    @BeforeClass
    public static void start() {
        setUpCluster(false, false);
    }

    @AfterClass
    public static void stop() {
        val cluster = CLUSTER.getAndSet(null);
        if (cluster != null) {
            cluster.close();
        }
    }
}