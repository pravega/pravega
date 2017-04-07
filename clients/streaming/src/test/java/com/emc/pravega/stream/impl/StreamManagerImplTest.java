/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.stream.impl;

import com.emc.pravega.StreamManager;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.stream.mock.MockConnectionFactoryImpl;
import com.emc.pravega.stream.mock.MockController;

import com.emc.pravega.testcommon.AssertExtensions;
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
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        this.controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        this.streamManager = new StreamManagerImpl(controller);
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
