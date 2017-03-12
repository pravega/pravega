/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.stream.impl;

import com.emc.pravega.ScopeManager;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.ServerImpl;
import com.emc.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceImplBase;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ScopeManagerImplTest {

    private ServerImpl fakeServer = null;
    private Controller controllerClient = null;

    @Before
    public void setUp() {
        ControllerServiceImplBase fakeServerImpl = new ControllerImplTest.MockServiceImpl();
        try {
            fakeServer = InProcessServerBuilder.forName("mockServiceImpl")
                    .addService(fakeServerImpl)
                    .directExecutor()
                    .build()
                    .start();
            controllerClient = new ControllerImpl(InProcessChannelBuilder.forName("mockServiceImpl").directExecutor());
        } catch (IOException e) {
            Assert.fail("Exception during set up" +  e.getMessage());
        }
    }

    @Test
    public void testCreateAndDeleteScope() {
        ScopeManager manager = new ScopeManagerImpl(controllerClient);

        // Create and delete immediately
        manager.createScope("scope1");
        manager.deleteScope("scope1");

        // According to the service mock implementation,
        // these calls should fail
        manager.createScope("scope2");
        manager.deleteScope("scope2");
    }
}
