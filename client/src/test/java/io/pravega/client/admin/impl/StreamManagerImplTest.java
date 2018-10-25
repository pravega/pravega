/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.admin.impl;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class StreamManagerImplTest {

    private static final int SERVICE_PORT = 12345;
    private String defaultScope = "foo";
    private StreamManager streamManager;
    private Controller controller = null;

    @Before
    public void setUp() {
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        this.controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        this.streamManager = new StreamManagerImpl(controller, null);
    }

    @Test
    public void testCreateAndDeleteScope() {
        // Create and delete immediately
        Assert.assertTrue(streamManager.createScope(defaultScope));
        Assert.assertTrue(streamManager.deleteScope(defaultScope));

        // Create twice
        Assert.assertTrue(streamManager.createScope(defaultScope));
        Assert.assertFalse(streamManager.createScope(defaultScope));
        Assert.assertTrue(streamManager.deleteScope(defaultScope));

        // Try to create invalid scope name.
        AssertExtensions.assertThrows(Exception.class, () -> streamManager.createScope("_system"));

        // This call should actually fail
        Assert.assertFalse(streamManager.deleteScope(defaultScope));
    }
}
