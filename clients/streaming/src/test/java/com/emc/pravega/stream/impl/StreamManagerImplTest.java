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

import org.junit.Before;
import org.junit.Test;


public class StreamManagerImplTest {

    private String defaultScope = "foo";
    private StreamManager streamManager;
    private Controller controller = null;

    @Before
    public void setUp() {
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", 1234);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        this.controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        this.streamManager = new StreamManagerImpl(defaultScope, controller);
    }

    @Test
    public void testCreateAndDeleteScope() {
        // Create and delete immediately
        streamManager.createScope("scope1");
        streamManager.deleteScope("scope1");

        // According to the service mock implementation,
        // these calls should fail
        streamManager.createScope("scope1");
        // This call should actually fail
        // TODO: https://github.com/pravega/pravega/issues/743
        streamManager.createScope("scope1");
        streamManager.deleteScope("scope1");

        // This call should actually fail
        // TODO: https://github.com/pravega/pravega/issues/743
        streamManager.deleteScope("scope1");
    }
}
